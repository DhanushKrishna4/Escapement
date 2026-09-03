//! Snapshots and log compaction (§7).
//!
//! The trap this file exists to guard is the one the paper is easy to misread
//! on: compaction discards a log prefix, but the leader still has to answer
//! `prevLogIndex` / `prevLogTerm` for the entry immediately before whatever it
//! sends next. Drop that and everything still looks healthy while replication
//! silently wedges.

use kvstore::KvCommand;
use raft::{Role, Snapshot};
use sim::{Cluster, FaultConfig, NetworkConfig, NodeStatus, SimConfig, Workload, WorkloadConfig};

fn cluster(seed: u64, nodes: usize, snapshot_every: Option<u64>) -> Cluster {
    Cluster::new(SimConfig {
        seed,
        nodes,
        snapshot_every,
        ..SimConfig::default()
    })
}

fn write(sim: &mut Cluster, to: u32, seq: u64, key: &str, value: &str) {
    sim.submit(
        to,
        1,
        seq,
        KvCommand::Put {
            key: key.into(),
            value: value.into(),
        },
    );
}

/// Drive `count` writes at whoever is leading.
fn writes(sim: &mut Cluster, count: u64, from: u64) {
    for i in 0..count {
        let leader = sim.leader().unwrap_or(0);
        write(
            sim,
            leader,
            from + i,
            &format!("k{}", (from + i) % 3),
            &format!("v{}", from + i),
        );
        sim.run_for(120);
    }
    sim.run_for(1_000);
}

// ---------------------------------------------------------------------------
// Compaction on a healthy cluster
// ---------------------------------------------------------------------------

#[test]
fn compaction_discards_the_prefix_and_keeps_the_boundary() {
    let mut sim = cluster(1, 3, Some(10));
    let leader = sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 40, 0);

    let log = sim.node(leader).log();
    assert!(
        log.first_index() > 1,
        "nothing was compacted: {:?}",
        sim.stats()
    );
    let boundary = log.first_index() - 1;
    assert!(
        log.term_at(boundary).is_some(),
        "the entry before the live range must still be describable"
    );
    assert!(
        log.get(boundary).is_none(),
        "but it should not still be stored"
    );
    assert!(
        sim.node(leader).snapshot().is_some(),
        "a node that compacted must have kept the snapshot"
    );
    sim.assert_no_violations();
}

#[test]
fn replication_keeps_working_across_a_compaction_boundary() {
    let mut sim = cluster(2, 3, Some(8));
    sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 60, 0);

    // Everyone still agrees, and everyone compacted.
    let leader = sim.leader().expect("leader");
    for id in sim.node_ids() {
        assert_eq!(
            sim.machine(id).snapshot(),
            sim.machine(leader).snapshot(),
            "node {id} diverged across compaction"
        );
        assert!(
            sim.node(id).log().first_index() > 1,
            "node {id} never compacted"
        );
    }
    sim.assert_no_violations();
}

#[test]
fn a_compacted_leader_still_commits_new_entries() {
    let mut sim = cluster(3, 5, Some(10));
    sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 40, 0);
    let before = sim.leader().map(|l| sim.node(l).commit_index()).unwrap();

    writes(&mut sim, 20, 100);
    let leader = sim.leader().expect("leader");
    assert!(
        sim.node(leader).commit_index() > before,
        "commitment stalled after compaction"
    );
    assert!(
        sim.machine(leader).get("k0").is_some(),
        "writes after compaction should still be visible"
    );
    sim.assert_no_violations();
}

/// The control: without compaction the same run keeps its whole log.
#[test]
fn without_compaction_the_log_grows_forever() {
    let mut sim = cluster(1, 3, None);
    let leader = sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 40, 0);
    assert_eq!(sim.node(leader).log().first_index(), 1);
    assert!(sim.node(leader).snapshot().is_none());
    assert_eq!(sim.stats().snapshots_taken, 0);
}

// ---------------------------------------------------------------------------
// InstallSnapshot
// ---------------------------------------------------------------------------

/// The reason InstallSnapshot exists: a follower that fell behind the leader's
/// compaction boundary cannot be caught up with AppendEntries, because the
/// entries it needs no longer exist.
#[test]
fn a_follower_left_behind_the_boundary_is_caught_up_by_a_snapshot() {
    let mut sim = cluster(4, 5, Some(8));
    let leader = sim.run_until_leader(5_000).expect("leader");
    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();

    // Cut it off and race ahead well past what it can ever be sent entry by
    // entry.
    sim.isolate(victim);
    writes(&mut sim, 60, 0);

    let leader = sim.leader().expect("leader");
    let boundary = sim.node(leader).log().first_index();
    assert!(
        boundary > 2,
        "the leader should have compacted past the victim"
    );
    assert!(
        sim.node(victim).log().last_index() < boundary,
        "the victim should be behind the boundary"
    );
    let installs_before = sim.stats().snapshots_installed;

    sim.heal();
    sim.run_for(20_000);

    assert!(
        sim.stats().snapshots_installed > installs_before,
        "the follower should have been caught up by a snapshot, not by entries"
    );
    assert_eq!(
        sim.machine(victim).snapshot(),
        sim.machine(leader).snapshot(),
        "the revived follower never caught up"
    );
    sim.assert_no_violations();
}

#[test]
fn a_restarted_node_recovers_from_its_snapshot_and_replays_the_rest() {
    let mut sim = cluster(5, 5, Some(8));
    let leader = sim.run_until_leader(5_000).expect("leader");
    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
    writes(&mut sim, 40, 0);

    let snapshot = sim.node(victim).snapshot().expect("a snapshot").clone();
    let covered = snapshot.index();
    let from_snapshot: kvstore::KvStore =
        serde_json::from_slice(&snapshot.data).expect("a serialized store");
    let live_before = sim.machine(victim).snapshot().clone();
    assert!(covered > 0);
    assert!(
        sim.node(victim).last_applied() >= covered,
        "the snapshot cannot be ahead of what was applied"
    );

    sim.crash(victim);
    sim.restart(victim);

    // With a snapshot, commitIndex and lastApplied come back at its index
    // rather than at zero: the entries below it are gone and cannot be
    // replayed.
    assert_eq!(sim.node(victim).commit_index(), covered);
    assert_eq!(sim.node(victim).last_applied(), covered);
    assert_eq!(sim.node(victim).role(), Role::Follower);

    // The state machine comes back as of the snapshot — NOT as of the moment it
    // crashed. Anything applied after the snapshot was volatile and is gone; it
    // gets replayed from the surviving log suffix.
    assert_eq!(
        sim.machine(victim).snapshot(),
        from_snapshot.snapshot(),
        "recovery should land exactly on the snapshot"
    );

    sim.run_for(15_000);
    let leader = sim.leader().expect("a leader");
    assert_eq!(
        sim.machine(victim).snapshot(),
        sim.machine(leader).snapshot(),
        "the log suffix should have replayed on top of the snapshot"
    );
    // And what it lost in the crash is back.
    for (key, value) in &live_before {
        assert_eq!(
            sim.machine(victim).get(key),
            Some(value),
            "{key} was not restored after replay"
        );
    }
    sim.assert_no_violations();
}

#[test]
fn a_node_that_never_snapshotted_still_replays_its_whole_log() {
    // The other half of the recovery rule.
    let mut sim = cluster(5, 5, None);
    let leader = sim.run_until_leader(5_000).expect("leader");
    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
    writes(&mut sim, 20, 0);

    sim.crash(victim);
    sim.restart(victim);
    assert_eq!(
        sim.node(victim).commit_index(),
        0,
        "no snapshot, so nothing is known"
    );
    assert!(sim.machine(victim).is_empty());

    sim.run_for(10_000);
    assert_eq!(
        sim.machine(victim).snapshot(),
        sim.machine(leader).snapshot(),
        "it should have replayed its way back"
    );
    sim.assert_no_violations();
}

#[test]
fn installing_a_snapshot_does_not_lose_a_matching_suffix() {
    // A follower slightly behind the boundary but otherwise consistent should
    // keep the entries it has after the snapshot point rather than throwing
    // them away.
    let mut sim = cluster(7, 5, Some(10));
    sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 50, 0);
    sim.run_for(5_000);

    let leader = sim.leader().expect("leader");
    for id in sim.node_ids() {
        // Everyone converged, and everyone holds the same last entry.
        assert_eq!(
            sim.node(id).log().last_log_id(),
            sim.node(leader).log().last_log_id(),
            "node {id} disagrees about the end of the log"
        );
    }
    sim.assert_no_violations();
}

// ---------------------------------------------------------------------------
// Compaction plus everything else
// ---------------------------------------------------------------------------

fn randomized(seed: u64, snapshot_every: Option<u64>) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 5,
        network: NetworkConfig::long_tail(),
        faults: FaultConfig::aggressive(),
        snapshot_every,
        ..SimConfig::default()
    });
    let mut work = Workload::new(seed, WorkloadConfig::contended());
    sim.run_until_leader(10_000);
    while sim.now() < 30_000 {
        let leader = sim.leader();
        let target = work.target(leader, 5);
        let req = work.next_request();
        sim.submit(target, req.client, req.seq, req.command);
        let gap = work.gap();
        sim.run_for(gap);
    }
    sim.run_for(5_000);
    sim
}

#[test]
fn safety_holds_with_compaction_under_faults() {
    for seed in 0..24u64 {
        let sim = randomized(seed, Some(12));
        if !sim.violations().is_empty() {
            let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
            panic!("seed {seed}:\n{}", report.join("\n"));
        }
    }
}

#[test]
fn histories_stay_linearizable_with_compaction() {
    for seed in 0..16u64 {
        let sim = randomized(seed, Some(12));
        let verdict = sim.check_linearizability();
        assert!(
            !verdict.is_violation(),
            "seed {seed}: compaction broke the client-visible history: {verdict:?}"
        );
    }
}

#[test]
fn determinism_holds_with_compaction() {
    for seed in 0..8u64 {
        let a = randomized(seed, Some(12));
        let b = randomized(seed, Some(12));
        assert_eq!(
            a.trace().digest(),
            b.trace().digest(),
            "seed {seed} not reproducible; first difference at {:?}",
            a.trace().first_difference(b.trace())
        );
        assert_eq!(a.stats(), b.stats());
    }
}

#[test]
fn compaction_and_crashes_together_still_converge() {
    for seed in 0..16u64 {
        let mut sim = Cluster::new(SimConfig {
            seed,
            nodes: 5,
            faults: FaultConfig::crash_only(),
            snapshot_every: Some(10),
            ..SimConfig::default()
        });
        sim.run_until_leader(10_000);
        let mut n = 0u64;
        while sim.now() < 25_000 {
            let leader = sim.leader().unwrap_or(0);
            write(
                &mut sim,
                leader,
                n,
                &format!("k{}", n % 3),
                &format!("v{n}"),
            );
            n += 1;
            sim.run_for(150);
        }
        // Stop the schedule as well as healing. `crash_only` keeps firing
        // otherwise, and a node it restarted moments before the check has not
        // had time to catch up -- which looks exactly like a node that never
        // will.
        sim.settle(25_000);

        let ids = sim.node_ids();
        let running: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|id| sim.status(*id) == NodeStatus::Running)
            .collect();
        let reference = sim.machine(running[0]).snapshot().clone();
        for id in &running {
            assert_eq!(
                sim.machine(*id).snapshot(),
                &reference,
                "seed {seed}: node {id} never reconverged"
            );
        }
        sim.assert_no_violations();
    }
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

#[test]
fn compaction_and_snapshot_shipping_actually_happen() {
    let mut taken = 0;
    let mut installed = 0;
    let mut max_first_index = 0;
    for seed in 0..16u64 {
        let sim = randomized(seed, Some(12));
        taken += sim.stats().snapshots_taken;
        installed += sim.stats().snapshots_installed;
        for id in sim.node_ids() {
            max_first_index = max_first_index.max(sim.node(id).log().first_index());
        }
    }
    assert!(taken > 100, "compaction barely fired: {taken}");
    assert!(
        installed > 0,
        "no snapshot was ever shipped to a follower, so InstallSnapshot is untested"
    );
    assert!(
        max_first_index > 20,
        "logs were never trimmed much: {max_first_index}"
    );
}

#[test]
fn compaction_actually_bounds_the_log() {
    // The whole point: a log that would otherwise grow without limit stays
    // small.
    let uncompacted = randomized(3, None);
    let compacted = randomized(3, Some(10));

    let biggest = |sim: &Cluster| {
        sim.node_ids()
            .into_iter()
            .map(|id| sim.node(id).log().len())
            .max()
            .unwrap_or(0)
    };
    assert!(
        biggest(&compacted) < biggest(&uncompacted),
        "compaction did not shrink anything: {} vs {}",
        biggest(&compacted),
        biggest(&uncompacted)
    );
}

#[test]
fn a_snapshot_carries_the_state_machine_faithfully() {
    let mut sim = cluster(9, 3, Some(10));
    let leader = sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 40, 0);

    let snapshot: &Snapshot = sim.node(leader).snapshot().expect("a snapshot");
    let decoded: kvstore::KvStore =
        serde_json::from_slice(&snapshot.data).expect("a snapshot is a serialized store");
    // The snapshot was taken at `last_applied`, which may be behind the current
    // state, so it must be a prefix-consistent view rather than identical.
    assert!(!decoded.is_empty(), "the snapshot captured nothing");
    for key in decoded.snapshot().keys() {
        assert!(
            sim.machine(leader).get(key).is_some(),
            "the snapshot holds {key}, which the live store does not"
        );
    }
}
