//! ReadIndex (§6.4) and client session deduplication (§8).
//!
//! Two promises to clients that Raft's internal invariants say nothing about.
//! A leader can satisfy every safety property in the paper and still hand a
//! client a stale value, or apply its retried command twice — and only the
//! linearizability checker notices either.

use kvstore::{KvCommand, KvResult};
use raft::{BugSwitches, RaftConfig, Role};
use sim::{ClientDriver, Cluster, FaultConfig, NetworkConfig, SimConfig, WorkloadConfig};

fn cluster(seed: u64, nodes: usize) -> Cluster {
    Cluster::new(SimConfig {
        seed,
        nodes,
        ..SimConfig::default()
    })
}

fn put(sim: &mut Cluster, seq: u64, key: &str, value: &str) {
    let leader = sim.leader().unwrap_or(0);
    sim.submit(
        leader,
        1,
        seq,
        KvCommand::Put {
            key: key.into(),
            value: value.into(),
        },
    );
}

fn read(sim: &mut Cluster, node: u32, client: u32, seq: u64, key: &str) {
    sim.submit(node, client, seq, KvCommand::Get { key: key.into() });
}

fn answer(sim: &Cluster, client: u32, seq: u64) -> Option<KvResult> {
    sim.outcomes()
        .iter()
        .find(|o| o.client == client && o.seq == seq)
        .and_then(|o| match &o.result {
            sim::OutcomeKind::Applied(r) => Some(r.clone()),
            _ => None,
        })
}

// ---------------------------------------------------------------------------
// ReadIndex
// ---------------------------------------------------------------------------

#[test]
fn a_read_returns_the_latest_committed_value() {
    let mut sim = cluster(1, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    put(&mut sim, 0, "a", "1");
    sim.run_for(1_000);

    read(&mut sim, leader, 2, 0, "a");
    sim.run_for(1_000);
    assert_eq!(answer(&sim, 2, 0), Some(KvResult::Value(Some("1".into()))));
    sim.assert_no_violations();
}

/// A read must not enter the log. That is the entire point of ReadIndex: one
/// round of heartbeats instead of a replication round trip.
#[test]
fn reads_do_not_grow_the_log() {
    let mut sim = cluster(2, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    put(&mut sim, 0, "a", "1");
    sim.run_for(1_000);
    let before = sim.node(leader).log().last_index();

    for i in 0..20u64 {
        read(&mut sim, leader, 2, i, "a");
        sim.run_for(100);
    }
    sim.run_for(1_000);

    assert_eq!(
        sim.node(leader).log().last_index(),
        before,
        "reads should never be replicated"
    );
    assert_eq!(sim.stats().reads_served, 20);
    sim.assert_no_violations();
}

/// A leader that cannot reach a quorum cannot confirm it is still the leader,
/// so it must not answer. The client hears nothing — which is correct, because
/// the alternative is answering with state that may be arbitrarily stale.
#[test]
fn a_partitioned_leader_cannot_serve_a_read() {
    let mut sim = cluster(3, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    put(&mut sim, 0, "a", "1");
    sim.run_for(1_000);

    let others: Vec<u32> = sim
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    sim.partition(&[leader], &others);

    read(&mut sim, leader, 2, 0, "a");
    sim.run_for(10_000);
    assert_eq!(
        answer(&sim, 2, 0),
        None,
        "an unconfirmed leader must not answer a read"
    );
    assert!(
        sim.node(leader).pending_reads() > 0,
        "the read should still be waiting"
    );

    // Healing lets it confirm and answer.
    sim.heal();
    sim.run_for(10_000);
    // Whoever leads now can serve it; the old leader may have stepped down, in
    // which case the client simply never hears and would retry.
    sim.assert_no_violations();
}

#[test]
fn a_follower_redirects_a_read_rather_than_answering_it() {
    let mut sim = cluster(4, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    sim.run_for(500);
    let follower = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();

    read(&mut sim, follower, 2, 0, "a");
    sim.run_for(500);
    let outcome = sim
        .outcomes()
        .iter()
        .find(|o| o.client == 2)
        .expect("the follower said nothing");
    assert!(
        matches!(outcome.result, sim::OutcomeKind::NotLeader { .. }),
        "a follower must not answer reads from its own state machine"
    );
}

/// A freshly elected leader does not yet know what is committed (§5.4.2), so a
/// read arriving immediately must wait for its no-op — not be answered against
/// a commit index of zero.
#[test]
fn a_read_waits_for_the_new_leader_to_learn_what_is_committed() {
    for seed in 0..16u64 {
        let mut sim = cluster(seed, 5);
        let old = sim.run_until_leader(5_000).expect("leader");
        for i in 0..4u64 {
            put(&mut sim, i, "a", &format!("v{i}"));
            sim.run_for(300);
        }
        sim.run_for(1_000);

        // Kill the leader and fire a read at whoever takes over, as early as
        // possible.
        sim.crash(old);
        let others: Vec<u32> = sim.node_ids().into_iter().filter(|id| *id != old).collect();
        let deadline = sim.now() + 20_000;
        let mut asked = false;
        while sim.now() < deadline {
            sim.step_once();
            if !asked {
                if let Some(new) = others
                    .iter()
                    .find(|id| sim.node(**id).role() == Role::Leader)
                {
                    read(&mut sim, *new, 9, 0, "a");
                    asked = true;
                }
            }
        }
        assert!(asked, "seed {seed}: nobody took over");
        assert_eq!(
            answer(&sim, 9, 0),
            Some(KvResult::Value(Some("v3".into()))),
            "seed {seed}: a read served during the handover must still see the last write"
        );
        sim.assert_no_violations();
    }
}

/// The control for the bug switch: with ReadIndex skipped, the same runs
/// produce impossible histories. This is what step 9 built the switch for, and
/// now there is a real implementation on the other side of it.
#[test]
fn skipping_the_read_index_round_produces_stale_reads() {
    let mut broken = 0;
    for seed in 0..200u64 {
        let mut sim = Cluster::new(SimConfig {
            seed,
            nodes: 5,
            network: NetworkConfig::long_tail(),
            faults: FaultConfig::aggressive(),
            stale_reads: true,
            ..SimConfig::default()
        });
        let mut driver = ClientDriver::new(seed, WorkloadConfig::contended(), 600, 4);
        sim.run_until_leader(10_000);
        while sim.now() < 30_000 {
            let gap = driver.step(&mut sim);
            sim.run_for(gap);
        }
        if sim.check_linearizability().is_violation() {
            broken += 1;
        }
    }
    assert!(
        broken > 0,
        "skipping the ReadIndex round should eventually produce a stale read"
    );
}

#[test]
fn with_read_index_the_same_runs_are_clean() {
    for seed in 0..200u64 {
        let mut sim = Cluster::new(SimConfig {
            seed,
            nodes: 5,
            network: NetworkConfig::long_tail(),
            faults: FaultConfig::aggressive(),
            ..SimConfig::default()
        });
        let mut driver = ClientDriver::new(seed, WorkloadConfig::contended(), 600, 4);
        sim.run_until_leader(10_000);
        while sim.now() < 30_000 {
            let gap = driver.step(&mut sim);
            sim.run_for(gap);
        }
        let verdict = sim.check_linearizability();
        assert!(!verdict.is_violation(), "seed {seed}: {verdict:?}");
        sim.assert_no_violations();
    }
}

// ---------------------------------------------------------------------------
// Session deduplication
// ---------------------------------------------------------------------------

/// A retried compare-and-set must not be applied twice. Applied twice it fails
/// the second time, and the client is told something that never happened.
#[test]
fn a_retried_command_is_applied_once() {
    let mut sim = cluster(5, 3);
    let leader = sim.run_until_leader(5_000).expect("leader");

    let cas = KvCommand::Cas {
        key: "a".into(),
        expect: None,
        value: "1".into(),
    };
    // Same (client, seq), submitted repeatedly — exactly what a client that
    // never hears back does.
    for _ in 0..4 {
        sim.submit(leader, 7, 0, cas.clone());
        sim.run_for(150);
    }
    sim.run_for(2_000);

    assert_eq!(
        answer(&sim, 7, 0),
        Some(KvResult::Ok),
        "the client should be told it succeeded"
    );
    for id in sim.node_ids() {
        assert_eq!(
            sim.machine(id).get("a"),
            Some(&"1".to_string()),
            "node {id} has the wrong value"
        );
        assert_eq!(sim.machine(id).last_seq(7), Some(0));
    }
    sim.assert_no_violations();
}

#[test]
fn deduplication_survives_a_leader_change() {
    // The session table lives in the replicated state machine, so a retry that
    // lands on a *different* leader is still recognised. A leader-local table
    // would forget everything exactly when clients retry.
    let mut sim = cluster(6, 5);
    let old = sim.run_until_leader(5_000).expect("leader");
    let cas = KvCommand::Cas {
        key: "a".into(),
        expect: None,
        value: "1".into(),
    };
    sim.submit(old, 7, 0, cas.clone());
    sim.run_for(2_000);
    assert_eq!(answer(&sim, 7, 0), Some(KvResult::Ok));

    sim.crash(old);
    let others: Vec<u32> = sim.node_ids().into_iter().filter(|id| *id != old).collect();
    let deadline = sim.now() + 20_000;
    while sim.now() < deadline && !others.iter().any(|id| sim.node(*id).role() == Role::Leader) {
        sim.step_once();
    }
    let new = sim.leader().expect("a new leader");

    // The client never heard the first answer and retries at the new leader.
    sim.submit(new, 7, 0, cas);
    sim.run_for(3_000);

    assert_eq!(
        sim.machine(new).get("a"),
        Some(&"1".to_string()),
        "the retry must not have re-run the compare-and-set"
    );
    sim.assert_no_violations();
}

#[test]
fn distinct_requests_from_one_client_all_apply() {
    let mut sim = cluster(7, 3);
    let leader = sim.run_until_leader(5_000).expect("leader");
    for i in 0..6u64 {
        sim.submit(
            leader,
            7,
            i,
            KvCommand::Put {
                key: format!("k{i}"),
                value: "v".into(),
            },
        );
        sim.run_for(200);
    }
    sim.run_for(2_000);
    assert_eq!(
        sim.machine(leader).len(),
        6,
        "dedup must not swallow new requests"
    );
    assert_eq!(sim.machine(leader).last_seq(7), Some(5));
    sim.assert_no_violations();
}

// ---------------------------------------------------------------------------
// Retries under faults
// ---------------------------------------------------------------------------

fn retrying_run(seed: u64, bugs: BugSwitches) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 5,
        network: NetworkConfig::long_tail(),
        faults: FaultConfig::aggressive(),
        snapshot_every: Some(15),
        raft: RaftConfig {
            bugs,
            ..RaftConfig::default()
        },
        ..SimConfig::default()
    });
    let mut driver = ClientDriver::new(seed, WorkloadConfig::contended(), 500, 5);
    sim.run_until_leader(10_000);
    while sim.now() < 25_000 {
        let gap = driver.step(&mut sim);
        sim.run_for(gap);
    }
    sim.run_for(5_000);
    sim
}

#[test]
fn retrying_clients_still_see_a_linearizable_history() {
    for seed in 0..24u64 {
        let sim = retrying_run(seed, BugSwitches::default());
        let verdict = sim.check_linearizability();
        assert!(!verdict.is_violation(), "seed {seed}: {verdict:?}");
        sim.assert_no_violations();
    }
}

#[test]
fn retries_actually_happen_and_get_deduplicated() {
    let mut retried = 0u64;
    let mut duplicated_arrivals = 0u64;
    for seed in 0..24u64 {
        let mut sim = Cluster::new(SimConfig {
            seed,
            nodes: 5,
            network: NetworkConfig::long_tail(),
            faults: FaultConfig::aggressive(),
            ..SimConfig::default()
        });
        let mut driver = ClientDriver::new(seed, WorkloadConfig::contended(), 500, 5);
        sim.run_until_leader(10_000);
        while sim.now() < 25_000 {
            let gap = driver.step(&mut sim);
            sim.run_for(gap);
        }
        retried += driver.retries();
        // Every operation whose attempts exceed one was resent at least once.
        duplicated_arrivals += sim
            .history()
            .operations()
            .iter()
            .filter(|o| o.attempts > 1)
            .count() as u64;
    }
    assert!(retried > 100, "clients barely retried: {retried}");
    assert!(
        duplicated_arrivals > 50,
        "few operations were actually resent: {duplicated_arrivals}"
    );
}

/// Retries must be recorded as one logical operation, not several. Recording
/// each attempt separately would invent concurrency that never existed.
#[test]
fn a_retried_operation_is_one_entry_in_the_history() {
    let sim = retrying_run(3, BugSwitches::default());
    let history = sim.history();
    let retried: Vec<_> = history
        .operations()
        .iter()
        .filter(|o| o.attempts > 1)
        .collect();
    assert!(!retried.is_empty(), "no retries happened in this run");

    // No two operations may share a (client, seq): that is what makes a retry a
    // retry rather than a new operation.
    let mut seen = std::collections::BTreeSet::new();
    for op in history.operations() {
        assert!(
            seen.insert((op.process, op.seq)),
            "the same request appears twice in the history"
        );
    }
}
