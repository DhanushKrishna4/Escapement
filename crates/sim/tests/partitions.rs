//! Partitions, including asymmetric ones.
//!
//! The handcrafted scenarios here are precise: partition exactly these nodes at
//! exactly this moment, and assert exactly what should follow. The randomized
//! ones at the bottom sweep seeds and schedules for safety.
//!
//! Safety must hold under every partition, however cruel. Liveness must not:
//! a cluster with no majority side is *supposed* to stop committing, and two of
//! the tests below pin down cases where Raft-as-written loses liveness on
//! purpose.

use kvstore::KvCommand;
use raft::Role;
use sim::{Cluster, FaultConfig, NetworkConfig, SimConfig};

fn cluster(seed: u64, nodes: usize) -> Cluster {
    Cluster::new(SimConfig {
        seed,
        nodes,
        ..SimConfig::default()
    })
}

/// Run until some node in `among` is leading, or give up.
fn leader_within(sim: &mut Cluster, among: &[u32], deadline_ticks: u64) -> Option<u32> {
    let deadline = sim.now() + deadline_ticks;
    while sim.now() < deadline {
        if !sim.step_once() {
            break;
        }
        if let Some(id) = among
            .iter()
            .find(|id| sim.node(**id).role() == Role::Leader)
        {
            return Some(*id);
        }
    }
    None
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

// ---------------------------------------------------------------------------
// Symmetric partitions
// ---------------------------------------------------------------------------

#[test]
fn a_minority_cannot_elect_a_leader() {
    let mut sim = cluster(1, 5);
    sim.run_until_leader(5_000).expect("leader");

    // Two nodes on their own: not a majority of five, so no matter how long
    // they campaign they can never win.
    sim.partition(&[0, 1], &[2, 3, 4]);
    sim.run_for(20_000);

    for id in [0, 1] {
        assert_ne!(
            sim.node(id).role(),
            Role::Leader,
            "node {id} led from a minority partition"
        );
    }
    // And their terms climb, because they keep trying.
    assert!(
        sim.node(0).current_term() > 5,
        "the minority should be campaigning repeatedly"
    );
    sim.assert_no_violations();
}

#[test]
fn the_majority_side_keeps_working() {
    let mut sim = cluster(2, 5);
    sim.run_until_leader(5_000).expect("leader");
    sim.partition(&[0, 1], &[2, 3, 4]);

    let leader = leader_within(&mut sim, &[2, 3, 4], 20_000).expect("majority should elect");
    for i in 0..5u64 {
        write(&mut sim, leader, i, "k", &format!("v{i}"));
        sim.run_for(300);
    }
    sim.run_for(2_000);

    for id in [2, 3, 4] {
        assert_eq!(
            sim.machine(id).get("k"),
            Some(&"v4".to_string()),
            "node {id} on the majority side should have the writes"
        );
    }
    for id in [0, 1] {
        assert_eq!(
            sim.machine(id).get("k"),
            None,
            "node {id} is cut off and must not have them"
        );
    }
    sim.assert_no_violations();
}

/// The single most illuminating scenario in the project.
///
/// A leader is partitioned away from its cluster but does not know it. Clients
/// keep writing to it, and it keeps appending — those entries can never commit,
/// because it has no majority. Meanwhile the other side elects a new leader and
/// commits different entries at the same indices. When the partition heals, the
/// old leader's uncommitted entries are truncated and it adopts the winner's
/// log.
///
/// Nothing committed is ever lost. Everything uncommitted may be.
#[test]
fn a_partitioned_leader_diverges_and_then_reconverges() {
    let mut sim = cluster(3, 5);
    let old_leader = sim.run_until_leader(5_000).expect("leader");
    let others: Vec<u32> = sim
        .node_ids()
        .into_iter()
        .filter(|id| *id != old_leader)
        .collect();

    // Everyone agrees on one committed write.
    write(&mut sim, old_leader, 0, "k", "committed");
    sim.run_for(1_000);
    let committed_index = sim.node(old_leader).commit_index();
    assert!(committed_index >= 2);

    // Cut the leader off. It has no idea.
    sim.partition(&[old_leader], &others);

    // Clients keep writing to it. It appends; it cannot commit.
    for i in 0..5u64 {
        write(&mut sim, old_leader, 10 + i, "k", &format!("stranded{i}"));
        sim.run_for(200);
    }
    let stranded = sim.node(old_leader).log().last_index();
    assert!(
        stranded > committed_index,
        "the old leader should have grown its log"
    );
    assert_eq!(
        sim.node(old_leader).commit_index(),
        committed_index,
        "a leader with no majority must not commit anything"
    );
    assert_eq!(
        sim.node(old_leader).role(),
        Role::Leader,
        "and it still thinks it leads"
    );

    // The other side elects someone and commits different entries at the same
    // indices.
    let new_leader = leader_within(&mut sim, &others, 20_000).expect("majority elects");
    for i in 0..3u64 {
        write(&mut sim, new_leader, 20 + i, "k", &format!("real{i}"));
        sim.run_for(300);
    }
    sim.run_for(2_000);

    let truncations_before = sim.stats().log_truncations;
    assert_ne!(
        sim.node(old_leader).log().last_log_id().term,
        sim.node(new_leader).log().last_log_id().term,
        "the two logs should genuinely have diverged"
    );

    // Heal. The old leader learns of the higher term, steps down, and has its
    // stranded entries truncated.
    sim.heal();
    sim.run_for(10_000);

    assert!(
        sim.stats().log_truncations > truncations_before,
        "healing should have forced a truncation"
    );
    assert_eq!(sim.node(old_leader).role(), Role::Follower);
    assert_eq!(
        sim.machine(old_leader).get("k"),
        Some(&"real2".to_string()),
        "the old leader should have adopted the winner's history"
    );
    for id in sim.node_ids() {
        assert_eq!(
            sim.machine(id).snapshot(),
            sim.machine(new_leader).snapshot(),
            "node {id} did not reconverge"
        );
    }
    sim.assert_no_violations();
}

#[test]
fn an_isolated_node_catches_up_after_healing() {
    let mut sim = cluster(4, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();

    sim.isolate(victim);
    for i in 0..8u64 {
        write(&mut sim, leader, i, &format!("k{i}"), "v");
        sim.run_for(200);
    }
    sim.run_for(1_000);
    assert!(
        sim.machine(victim).is_empty(),
        "the isolated node should have missed everything"
    );

    sim.heal();
    sim.run_for(10_000);
    assert_eq!(
        sim.machine(victim).snapshot(),
        sim.machine(leader).snapshot(),
        "the isolated node should have caught up"
    );
    sim.assert_no_violations();
}

// ---------------------------------------------------------------------------
// Asymmetric partitions
// ---------------------------------------------------------------------------

#[test]
fn an_asymmetric_cut_really_is_one_way() {
    let mut sim = cluster(5, 3);
    sim.cut(0, 1);
    assert!(!sim.is_reachable(0, 1));
    assert!(sim.is_reachable(1, 0));
    assert!(sim.is_reachable(0, 2) && sim.is_reachable(2, 0));
}

/// A leader that can send but cannot receive.
///
/// Its heartbeats keep arriving, so no follower ever times out and nobody
/// campaigns — but no acknowledgement ever gets back, so `matchIndex` never
/// advances and nothing can commit. The cluster sits there, healthy-looking and
/// completely stuck, until the link is repaired.
///
/// This is exactly the case a leader lease / CheckQuorum exists to solve: a
/// leader that cannot hear a majority should step down and let someone else
/// take over. Raft as written in the paper does not do that, and neither does
/// this implementation yet (it arrives with ReadIndex in step 12). Safety is
/// never in question; liveness is entirely lost.
#[test]
fn a_leader_that_can_send_but_not_receive_stalls_the_cluster() {
    let mut sim = cluster(6, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    sim.run_for(1_000);
    let committed_before = sim.node(leader).commit_index();

    // Every follower's replies are lost; the leader's own messages get through.
    for id in sim.node_ids() {
        if id != leader {
            sim.cut(id, leader);
        }
    }

    for i in 0..5u64 {
        write(&mut sim, leader, i, "k", &format!("v{i}"));
        sim.run_for(400);
    }
    sim.run_for(10_000);

    assert_eq!(
        sim.node(leader).commit_index(),
        committed_before,
        "a leader hearing nobody must not commit"
    );
    assert_eq!(
        sim.node(leader).role(),
        Role::Leader,
        "and without CheckQuorum it does not step down either"
    );
    assert_eq!(
        sim.leaders(),
        vec![leader],
        "its heartbeats keep the followers quiet, so nobody replaces it"
    );
    sim.assert_no_violations();

    // Repairing the link unsticks it immediately.
    sim.heal();
    sim.run_for(5_000);
    assert!(
        sim.node(leader).commit_index() > committed_before,
        "progress should resume once replies get through"
    );
    sim.assert_no_violations();
}

/// A follower that cannot hear the leader, but can still be heard.
///
/// It times out and campaigns forever, and every RequestVote it sends carries a
/// higher term. Under §5.1 alone that would depose a perfectly healthy leader
/// each time, and the whole cluster's term would climb with it.
///
/// §6's disruption guard is what stops that: a server that has heard from a
/// leader within the minimum election timeout ignores vote requests entirely —
/// it does not update its term and does not reply. So the deaf node's *own*
/// term still runs away, because nothing tells it otherwise, while the healthy
/// cluster is untouched.
///
/// That containment is not a nicety. It became load-bearing the moment
/// membership changes arrived: a server removed by a configuration change is
/// exactly this node, and without the guard a single stranded one drove the
/// term into the hundreds and the cluster never settled.
#[test]
fn a_deaf_follower_runs_away_alone_without_disrupting_the_cluster() {
    let mut sim = cluster(7, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    let deaf = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();

    sim.run_for(1_000);
    let term_before = sim.node(leader).current_term();

    // The deaf node hears nothing from anyone, but everyone hears it.
    for id in sim.node_ids() {
        if id != deaf {
            sim.cut(id, deaf);
        }
    }
    sim.run_for(20_000);

    // It campaigns endlessly, so its own term runs away.
    assert!(
        sim.node(deaf).current_term() > term_before + 10,
        "the deaf node should be campaigning constantly: {} -> {}",
        term_before,
        sim.node(deaf).current_term()
    );

    // But the healthy cluster ignores it completely: same leader, same term.
    assert_eq!(
        sim.leaders(),
        vec![leader],
        "the sitting leader should never have been deposed"
    );
    assert_eq!(
        sim.node(leader).current_term(),
        term_before,
        "§6's guard should have kept the disruption entirely local"
    );
    for id in sim.node_ids() {
        if id != deaf {
            assert_eq!(
                sim.node(id).current_term(),
                term_before,
                "node {id} adopted the disrupting node's term"
            );
        }
    }
    sim.assert_no_violations();

    // And the runaway rejoins cleanly once it can hear again.
    sim.heal();
    sim.run_for(20_000);
    let l = sim.leader().expect("a leader");
    write(&mut sim, l, 99, "k", "after");
    sim.run_for(3_000);
    assert_eq!(sim.machine(l).get("k"), Some(&"after".to_string()));
    sim.assert_no_violations();
}

// ---------------------------------------------------------------------------
// Randomized schedules
// ---------------------------------------------------------------------------

/// A named fault schedule. Aliased because the bare tuple-of-fn-pointer type is
/// unreadable at every use site.
type Schedule = (&'static str, fn() -> FaultConfig);

const SCHEDULES: [Schedule; 3] = [
    ("occasional", FaultConfig::occasional),
    ("aggressive", FaultConfig::aggressive),
    ("asymmetric_only", FaultConfig::asymmetric_only),
];

fn randomized(seed: u64, net: NetworkConfig, faults: FaultConfig, ticks: u64) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 5,
        network: net,
        faults,
        ..SimConfig::default()
    });
    sim.run_until_leader(10_000);
    let mut issued = 0u64;
    while sim.now() < ticks {
        // Aim at whoever leads; fall back to spraying when nobody does, which
        // is what a real client with a stale hint would do.
        let target = sim.leader().unwrap_or((issued % 5) as u32);
        write(
            &mut sim,
            target,
            issued,
            &format!("k{}", issued % 4),
            &format!("v{issued}"),
        );
        issued += 1;
        sim.run_for(200);
    }
    sim
}

#[test]
fn safety_holds_under_every_partition_schedule() {
    for (name, schedule) in SCHEDULES {
        for seed in 0..24u64 {
            let sim = randomized(seed, NetworkConfig::perfect(), schedule(), 30_000);
            if !sim.violations().is_empty() {
                let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
                panic!(
                    "schedule {name}, seed {seed}: {} violation(s)\n{}",
                    report.len(),
                    report.join("\n")
                );
            }
        }
    }
}

#[test]
fn safety_holds_with_partitions_on_top_of_a_bad_network() {
    for seed in 0..24u64 {
        let sim = randomized(
            seed,
            NetworkConfig::long_tail(),
            FaultConfig::aggressive(),
            30_000,
        );
        sim.assert_no_violations();
    }
}

#[test]
fn determinism_holds_under_partition_schedules() {
    for (name, schedule) in SCHEDULES {
        for seed in 0..8u64 {
            let a = randomized(seed, NetworkConfig::long_tail(), schedule(), 15_000);
            let b = randomized(seed, NetworkConfig::long_tail(), schedule(), 15_000);
            assert_eq!(
                a.trace().digest(),
                b.trace().digest(),
                "schedule {name}, seed {seed} was not reproducible; first difference at {:?}",
                a.trace().first_difference(b.trace())
            );
            assert_eq!(a.faults_injected().len(), b.faults_injected().len());
        }
    }
}

#[test]
fn the_cluster_recovers_between_disturbances() {
    // `occasional` leaves long healthy stretches, so a run should end with
    // everyone agreeing.
    let mut converged = 0;
    for seed in 0..16u64 {
        let mut sim = randomized(
            seed,
            NetworkConfig::perfect(),
            FaultConfig::occasional(),
            30_000,
        );
        sim.settle(30_000);
        let ids = sim.node_ids();
        let reference = sim.machine(ids[0]).snapshot().clone();
        if ids
            .iter()
            .all(|id| sim.machine(*id).snapshot() == &reference)
            && !reference.is_empty()
        {
            converged += 1;
        }
    }
    assert_eq!(converged, 16, "every seed should reconverge once healed");
}

/// The accounting from step 5, redone now that partitions exist. This is the
/// number that shows the fault testing actually reaches log divergence.
#[test]
fn partitions_are_what_finally_reach_log_divergence() {
    let mut with_partitions = 0;
    let mut without = 0;
    for seed in 0..16u64 {
        with_partitions += randomized(
            seed,
            NetworkConfig::perfect(),
            FaultConfig::aggressive(),
            30_000,
        )
        .stats()
        .log_truncations;
        without += randomized(seed, NetworkConfig::perfect(), FaultConfig::none(), 30_000)
            .stats()
            .log_truncations;
    }
    assert_eq!(without, 0, "a healthy network still should not truncate");
    assert!(
        with_partitions > 40,
        "partitions should drive real divergence, got {with_partitions} truncations"
    );
}

/// The two kinds of partition attack different properties, and a fuzzer needs
/// both.
///
/// A symmetric split gives an isolated leader a private log to grow, which is
/// what produces divergence and truncation. A one-way cut never does that — it
/// makes a node deaf, so it campaigns forever and inflates the term, attacking
/// *liveness* while leaving every log consistent. Running only one kind would
/// leave half of Raft untested while looking busy.
#[test]
fn asymmetric_cuts_attack_liveness_while_symmetric_ones_attack_consistency() {
    let (mut asym_trunc, mut asym_term) = (0u64, 0u64);
    let (mut sym_trunc, mut sym_term) = (0u64, 0u64);
    for seed in 0..16u64 {
        let a = randomized(
            seed,
            NetworkConfig::perfect(),
            FaultConfig::asymmetric_only(),
            30_000,
        );
        asym_trunc += a.stats().log_truncations;
        asym_term = asym_term.max(a.stats().max_term);

        let s = randomized(
            seed,
            NetworkConfig::perfect(),
            FaultConfig::occasional(),
            30_000,
        );
        sym_trunc += s.stats().log_truncations;
        sym_term = sym_term.max(s.stats().max_term);
    }

    assert_eq!(
        asym_trunc, 0,
        "one-way cuts do not give any node a private log to diverge with"
    );
    assert!(
        asym_term > 8,
        "but they should inflate the term badly: reached {asym_term}"
    );
    assert!(
        sym_trunc > 20,
        "symmetric splits are what cause truncation: got {sym_trunc}"
    );
    assert!(sym_term > 8);
}
