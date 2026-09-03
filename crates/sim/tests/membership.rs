//! Membership changes end to end (§6).
//!
//! The node-level rules live in `crates/raft/tests/membership.rs`; this drives
//! them through the real simulator, with the network, faults and snapshots that
//! make a change actually difficult.

use std::collections::BTreeSet;

use kvstore::KvCommand;
use raft::{NodeId, Role};
use sim::{Cluster, FaultConfig, NetworkConfig, SimConfig, Workload, WorkloadConfig};

fn cluster(seed: u64, nodes: usize, voters: &[NodeId]) -> Cluster {
    Cluster::new(SimConfig {
        seed,
        nodes,
        initial_voters: Some(voters.to_vec()),
        ..SimConfig::default()
    })
}

fn write(sim: &mut Cluster, seq: u64, key: &str, value: &str) {
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

fn writes(sim: &mut Cluster, count: u64, from: u64) {
    for i in 0..count {
        write(
            sim,
            from + i,
            &format!("k{}", (from + i) % 3),
            &format!("v{}", from + i),
        );
        sim.run_for(150);
    }
    sim.run_for(1_000);
}

fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
    ids.iter().copied().collect()
}

/// Wait until every voter agrees on this membership.
fn settle(sim: &mut Cluster, expected: &[NodeId], ticks: u64) -> bool {
    let deadline = sim.now() + ticks;
    while sim.now() < deadline {
        sim.run_for(200);
        let all_agree = expected.iter().all(|id| {
            let cfg = sim.configuration(*id);
            !cfg.is_joint() && cfg.voters() == set(expected)
        });
        if all_agree {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Growing, shrinking, replacing
// ---------------------------------------------------------------------------

#[test]
fn a_cluster_can_grow() {
    let mut sim = cluster(1, 5, &[0, 1, 2]);
    sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 5, 0);

    assert!(sim.change_membership([0, 1, 2, 3, 4]));
    assert!(
        settle(&mut sim, &[0, 1, 2, 3, 4], 20_000),
        "the cluster never settled on the new membership"
    );

    // The new members are caught up and part of the cluster.
    let leader = sim.leader().expect("leader");
    writes(&mut sim, 5, 100);
    sim.run_for(5_000);
    for id in [3, 4] {
        assert_eq!(
            sim.machine(id).snapshot(),
            sim.machine(leader).snapshot(),
            "node {id} joined but never caught up"
        );
    }
    sim.assert_no_violations();
}

#[test]
fn a_cluster_can_shrink() {
    let mut sim = cluster(2, 5, &[0, 1, 2, 3, 4]);
    sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 5, 0);

    assert!(sim.change_membership([0, 1, 2]));
    assert!(settle(&mut sim, &[0, 1, 2], 20_000), "never settled");

    for id in [0, 1, 2] {
        assert!(!sim.configuration(id).contains(3));
        assert!(!sim.configuration(id).contains(4));
    }
    // And the remaining three keep working on their own.
    writes(&mut sim, 5, 100);
    sim.run_for(3_000);
    let leader = sim.leader().expect("leader");
    assert!([0, 1, 2].contains(&leader));
    assert!(sim.node(leader).commit_index() > 10);
    sim.assert_no_violations();
}

#[test]
fn a_cluster_can_replace_most_of_itself() {
    // {0,1,2} -> {2,3,4}: the halves overlap at exactly one node, which is the
    // case joint consensus exists for.
    let mut sim = cluster(3, 5, &[0, 1, 2]);
    sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 8, 0);

    assert!(sim.change_membership([2, 3, 4]));
    assert!(settle(&mut sim, &[2, 3, 4], 30_000), "never settled");

    writes(&mut sim, 5, 100);
    sim.run_for(5_000);
    let leader = sim.leader().expect("leader");
    assert!(
        [2, 3, 4].contains(&leader),
        "the leader should be in the new set"
    );
    for id in [3, 4] {
        assert_eq!(
            sim.machine(id).snapshot(),
            sim.machine(leader).snapshot(),
            "node {id} never caught up"
        );
    }
    sim.assert_no_violations();
}

/// §6: a leader that removes itself keeps leading until C_new commits, then
/// steps down.
#[test]
fn a_leader_that_removes_itself_hands_over() {
    for seed in 0..8u64 {
        let mut sim = cluster(seed, 5, &[0, 1, 2, 3, 4]);
        let old = sim.run_until_leader(5_000).expect("leader");
        writes(&mut sim, 5, 0);

        let remaining: Vec<NodeId> = sim
            .node_ids()
            .into_iter()
            .filter(|id| *id != old)
            .take(3)
            .collect();
        assert!(sim.change_membership(remaining.clone()));
        assert!(
            settle(&mut sim, &remaining, 30_000),
            "seed {seed}: never settled"
        );
        sim.run_for(10_000);

        assert_eq!(
            sim.node(old).role(),
            Role::Follower,
            "seed {seed}: the removed leader should have stepped down"
        );
        let leader = sim.leader().expect("a new leader");
        assert!(
            remaining.contains(&leader),
            "seed {seed}: leadership should be inside C_new"
        );
        sim.assert_no_violations();
    }
}

#[test]
fn membership_changes_survive_a_flaky_network() {
    for seed in 0..12u64 {
        let mut sim = Cluster::new(SimConfig {
            seed,
            nodes: 5,
            initial_voters: Some(vec![0, 1, 2]),
            network: NetworkConfig::flaky(),
            ..SimConfig::default()
        });
        sim.run_until_leader(10_000).expect("leader");
        writes(&mut sim, 5, 0);
        assert!(sim.change_membership([0, 1, 2, 3, 4]));
        assert!(
            settle(&mut sim, &[0, 1, 2, 3, 4], 40_000),
            "seed {seed}: never settled on a flaky network"
        );
        sim.assert_no_violations();
    }
}

// ---------------------------------------------------------------------------
// Changes plus everything else
// ---------------------------------------------------------------------------

fn churn(seed: u64, snapshot_every: Option<u64>) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 7,
        initial_voters: Some(vec![0, 1, 2]),
        network: NetworkConfig::long_tail(),
        faults: FaultConfig::aggressive(),
        snapshot_every,
        ..SimConfig::default()
    });
    let mut work = Workload::new(seed, WorkloadConfig::contended());
    sim.run_until_leader(10_000);

    // Cycle the membership around while everything else is going wrong.
    let rotations: [&[NodeId]; 4] = [&[0, 1, 2, 3, 4], &[2, 3, 4], &[2, 3, 4, 5, 6], &[0, 1, 2]];
    let mut next_change = 6_000u64;
    let mut rotation = 0usize;

    while sim.now() < 45_000 {
        if sim.now() >= next_change {
            sim.change_membership(rotations[rotation % rotations.len()].iter().copied());
            rotation += 1;
            next_change = sim.now() + 9_000;
        }
        let leader = sim.leader();
        let target = work.target(leader, 7);
        let req = work.next_request();
        sim.submit(target, req.client, req.seq, req.command);
        let gap = work.gap();
        sim.run_for(gap);
    }
    sim.heal();
    sim.run_for(10_000);
    sim
}

#[test]
fn safety_holds_while_membership_churns_under_faults() {
    for seed in 0..16u64 {
        let sim = churn(seed, None);
        if !sim.violations().is_empty() {
            let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
            panic!("seed {seed}:\n{}", report.join("\n"));
        }
    }
}

#[test]
fn safety_holds_with_membership_churn_and_compaction() {
    for seed in 0..16u64 {
        let sim = churn(seed, Some(12));
        if !sim.violations().is_empty() {
            let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
            panic!("seed {seed}:\n{}", report.join("\n"));
        }
    }
}

#[test]
fn histories_stay_linearizable_across_membership_changes() {
    for seed in 0..12u64 {
        let sim = churn(seed, Some(12));
        let verdict = sim.check_linearizability();
        assert!(
            !verdict.is_violation(),
            "seed {seed}: membership churn broke the client-visible history: {verdict:?}"
        );
    }
}

#[test]
fn determinism_holds_across_membership_changes() {
    for seed in 0..8u64 {
        let a = churn(seed, Some(12));
        let b = churn(seed, Some(12));
        assert_eq!(
            a.trace().digest(),
            b.trace().digest(),
            "seed {seed} not reproducible; first difference at {:?}",
            a.trace().first_difference(b.trace())
        );
        assert_eq!(a.stats(), b.stats());
    }
}

/// A snapshot has to carry the membership, or a node recovering from one falls
/// back to whatever it was started with and resurrects a configuration the
/// cluster left behind.
#[test]
fn a_snapshot_carries_the_membership_across_a_restart() {
    let mut sim = Cluster::new(SimConfig {
        seed: 5,
        nodes: 5,
        initial_voters: Some(vec![0, 1, 2]),
        snapshot_every: Some(6),
        ..SimConfig::default()
    });
    sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 6, 0);
    assert!(sim.change_membership([0, 1, 2, 3, 4]));
    assert!(settle(&mut sim, &[0, 1, 2, 3, 4], 20_000), "never settled");

    // Push far enough past the change that the config entry is compacted away
    // and only the snapshot records it.
    writes(&mut sim, 30, 100);
    let victim = 3;
    let snapshot = sim.node(victim).snapshot().expect("a snapshot").clone();
    assert_eq!(
        snapshot.config.voters(),
        set(&[0, 1, 2, 3, 4]),
        "the snapshot must record the membership in force at its boundary"
    );
    assert!(
        sim.node(victim).log().first_index() > 1,
        "the configuration entry should be behind the compaction boundary"
    );

    sim.crash(victim);
    sim.restart(victim);
    assert_eq!(
        sim.configuration(victim).voters(),
        set(&[0, 1, 2, 3, 4]),
        "the recovered node forgot the membership"
    );
    sim.run_for(10_000);
    sim.assert_no_violations();
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

#[test]
fn the_churn_actually_changes_membership() {
    let mut changes = 0;
    let mut distinct = BTreeSet::new();
    for seed in 0..8u64 {
        let sim = churn(seed, Some(12));
        changes += sim.stats().membership_changes;
        for id in sim.node_ids() {
            distinct.insert(sim.configuration(id).describe());
        }
    }
    assert!(changes > 20, "membership barely changed: {changes}");
    assert!(
        distinct.len() > 2,
        "the cluster never really moved between configurations: {distinct:?}"
    );
}

#[test]
fn a_joint_configuration_is_actually_reached() {
    // The transitional state is where all the difficulty is; a run that skips
    // straight from C_old to C_new would not be testing §6 at all.
    let mut sim = cluster(11, 5, &[0, 1, 2]);
    sim.run_until_leader(5_000).expect("leader");
    writes(&mut sim, 3, 0);

    // Cut the incoming members off so the joint entry cannot commit, freezing
    // the cluster in C_old,new.
    //
    // C_new has to actually NEED the unreachable nodes for this to block. A
    // change from {0,1,2} to a superset would still commit, because {0,1,2} is
    // already a majority of the larger set -- which is the first version of
    // this test, and it did not test anything.
    sim.partition(&[0, 1, 2], &[3, 4]);
    assert!(sim.change_membership([2, 3, 4]));
    sim.run_for(3_000);

    let leader = sim.leader().expect("leader");
    assert!(
        sim.configuration(leader).is_joint(),
        "the leader should be stuck in the joint configuration"
    );
    assert_eq!(
        sim.configuration(leader).new_voters(),
        Some(&set(&[2, 3, 4])),
        "and it should know which configuration it is moving to"
    );
    assert!(
        sim.node(leader).commit_index() < sim.node(leader).config_index(),
        "the joint entry must still be uncommitted"
    );

    // Healing lets it finish.
    sim.heal();
    assert!(settle(&mut sim, &[2, 3, 4], 30_000), "never completed");
    sim.assert_no_violations();
}
