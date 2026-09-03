//! Step 2 of the build order: a 3-node cluster on a perfect network elects a
//! leader and replicates entries.
//!
//! These are behavioural tests over the simulator rather than unit tests of the
//! node; the unit tests for individual Raft rules live in `crates/raft`.

use std::collections::{BTreeMap, BTreeSet};

use kvstore::{KvCommand, KvResult};
use raft::{Role, Term};
use sim::{Cluster, OutcomeKind, SimConfig};

fn cluster(seed: u64, nodes: usize) -> Cluster {
    Cluster::new(SimConfig {
        seed,
        nodes,
        ..SimConfig::default()
    })
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    for seed in 0..32u64 {
        let mut sim = cluster(seed, 3);
        let leader = sim.run_until_leader(5_000);
        assert!(leader.is_some(), "seed {seed}: no leader within 5000 ticks");

        // Let the term settle, then there must be exactly one leader.
        sim.run_for(500);
        let leaders = sim.leaders();
        assert_eq!(
            leaders.len(),
            1,
            "seed {seed}: expected one leader, got {leaders:?}"
        );

        // And everyone else must agree on who it is and on the term.
        let leader = leaders[0];
        let term = sim.node(leader).current_term();
        for (id, node) in sim.nodes() {
            assert_eq!(
                node.current_term(),
                term,
                "seed {seed}: node {id} term differs"
            );
            if *id != leader {
                assert_eq!(node.role(), Role::Follower);
                assert_eq!(
                    node.leader_id(),
                    Some(leader),
                    "seed {seed}: node {id} disagrees on the leader"
                );
            }
        }
    }
}

#[test]
fn a_single_node_cluster_elects_itself() {
    let mut sim = cluster(1, 1);
    let leader = sim.run_until_leader(1_000);
    assert_eq!(leader, Some(0));

    // With no peers, its own append is already a majority, so entries commit
    // immediately.
    sim.submit(
        0,
        1,
        0,
        KvCommand::Put {
            key: "a".into(),
            value: "1".into(),
        },
    );
    sim.run_for(100);
    assert_eq!(sim.machine(0).get("a"), Some(&"1".to_string()));
}

#[test]
fn five_nodes_elect_exactly_one_leader() {
    for seed in 0..16u64 {
        let mut sim = cluster(seed, 5);
        assert!(
            sim.run_until_leader(5_000).is_some(),
            "seed {seed}: no leader"
        );
        sim.run_for(500);
        assert_eq!(sim.leaders().len(), 1, "seed {seed}");
    }
}

#[test]
fn a_leader_appends_a_noop_of_its_own_term_on_election() {
    // §5.4.2: without an entry of its own term a leader could never advance
    // commitIndex, so it appends one immediately.
    let mut sim = cluster(3, 3);
    let leader = sim.run_until_leader(5_000).expect("leader");
    let node = sim.node(leader);
    let last = node.last_log_id();
    assert_eq!(last.index, 1, "the no-op should be the first entry");
    assert_eq!(last.term, node.current_term());
}

#[test]
fn entries_replicate_to_every_node() {
    let mut sim = cluster(11, 3);
    let leader = sim.run_until_leader(5_000).expect("leader");

    for i in 0..5u64 {
        sim.submit(
            leader,
            1,
            i,
            KvCommand::Put {
                key: format!("key{i}"),
                value: format!("value{i}"),
            },
        );
        sim.run_for(120);
    }
    sim.run_for(500);

    // Every node applied every write, and their state machines agree.
    let expected: BTreeMap<String, String> = (0..5)
        .map(|i| (format!("key{i}"), format!("value{i}")))
        .collect();
    for id in sim.node_ids() {
        assert_eq!(
            sim.machine(id).snapshot(),
            &expected,
            "node {id} has the wrong state machine contents"
        );
    }

    // And the commit index moved on every node, not just the leader.
    let leader_commit = sim.node(leader).commit_index();
    assert!(leader_commit >= 6, "leader committed only {leader_commit}");
    for id in sim.node_ids() {
        assert_eq!(
            sim.node(id).commit_index(),
            leader_commit,
            "node {id} lags the leader's commit index"
        );
    }
}

#[test]
fn the_client_gets_the_state_machine_result_back() {
    let mut sim = cluster(5, 3);
    let leader = sim.run_until_leader(5_000).expect("leader");

    sim.submit(
        leader,
        1,
        0,
        KvCommand::Put {
            key: "a".into(),
            value: "1".into(),
        },
    );
    sim.run_for(200);
    sim.submit(leader, 1, 1, KvCommand::Get { key: "a".into() });
    sim.run_for(200);

    let applied: Vec<&OutcomeKind> = sim
        .outcomes()
        .iter()
        .filter(|o| o.client == 1)
        .map(|o| &o.result)
        .collect();
    assert_eq!(
        applied,
        vec![
            &OutcomeKind::Applied(KvResult::Ok),
            &OutcomeKind::Applied(KvResult::Value(Some("1".into()))),
        ]
    );
}

#[test]
fn a_follower_redirects_the_client_to_the_leader() {
    let mut sim = cluster(5, 3);
    let leader = sim.run_until_leader(5_000).expect("leader");
    // Let the first heartbeat land. Until it does the follower has genuinely
    // never heard of this leader and correctly answers `NotLeader { None }`;
    // the hint only exists once an AppendEntries has arrived.
    sim.run_for(200);
    let follower = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
    assert_eq!(sim.node(follower).leader_id(), Some(leader));

    sim.submit(
        follower,
        9,
        0,
        KvCommand::Put {
            key: "a".into(),
            value: "1".into(),
        },
    );
    sim.run_for(100);

    let outcome = sim
        .outcomes()
        .iter()
        .find(|o| o.client == 9)
        .expect("the follower answered nothing");
    assert_eq!(
        outcome.result,
        OutcomeKind::NotLeader {
            leader: Some(leader)
        }
    );

    // And the write must not have happened anywhere.
    for id in sim.node_ids() {
        assert_eq!(sim.machine(id).get("a"), None);
    }
}

#[test]
fn a_healthy_cluster_does_not_churn_leaders() {
    // On a perfect network the first leader should hold. If terms keep climbing
    // the timing constants are wrong (heartbeats slower than election timeouts)
    // or heartbeats are not resetting follower timers.
    let mut sim = cluster(2, 3);
    let leader = sim.run_until_leader(5_000).expect("leader");
    let term_after_election = sim.node(leader).current_term();

    sim.run_for(20_000);

    assert_eq!(
        sim.leaders(),
        vec![leader],
        "leadership moved on a perfect network"
    );
    assert_eq!(
        sim.node(leader).current_term(),
        term_after_election,
        "the term advanced on a perfect network: the cluster is churning"
    );
}

/// Election Safety (§5.2): at most one leader per term. Checked at every event
/// rather than at the end, because a violation could easily be transient.
#[test]
fn election_safety_holds_throughout_the_run() {
    for seed in 0..16u64 {
        let mut sim = cluster(seed, 5);
        let mut leader_of_term: BTreeMap<Term, u32> = BTreeMap::new();
        for _ in 0..8_000 {
            if !sim.step_once() {
                break;
            }
            for (id, node) in sim.nodes() {
                if node.role() == Role::Leader {
                    if let Some(prev) = leader_of_term.insert(node.current_term(), *id) {
                        assert_eq!(
                            prev,
                            *id,
                            "seed {seed}: nodes {prev} and {id} were both leader of term {}",
                            node.current_term()
                        );
                    }
                }
            }
        }
    }
}

/// Log Matching (§5.3): if two logs contain an entry with the same index and
/// term, the logs are identical in all preceding entries.
#[test]
fn log_matching_holds_across_nodes() {
    let mut sim = cluster(13, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    for i in 0..10u64 {
        sim.submit(
            leader,
            1,
            i,
            KvCommand::Put {
                key: "k".into(),
                value: format!("{i}"),
            },
        );
        sim.run_for(80);
    }
    sim.run_for(500);

    let ids = sim.node_ids();
    for a in &ids {
        for b in &ids {
            let (la, lb) = (sim.node(*a).log(), sim.node(*b).log());
            let upto = la.last_index().min(lb.last_index());
            let mut matched_from = None;
            for i in (1..=upto).rev() {
                let (ea, eb) = (la.get(i).unwrap(), lb.get(i).unwrap());
                if ea.term == eb.term {
                    matched_from = Some(i);
                    assert_eq!(
                        ea, eb,
                        "nodes {a} and {b} have different entries at index {i} with the same term"
                    );
                } else if matched_from.is_some() {
                    panic!("nodes {a} and {b} match above index {i} but differ at it");
                }
            }
        }
    }
}

/// State Machine Safety: no two nodes apply a different command at the same
/// index.
#[test]
fn no_two_nodes_apply_different_commands_at_the_same_index() {
    let mut sim = cluster(17, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    for i in 0..10u64 {
        sim.submit(
            leader,
            1,
            i,
            KvCommand::Put {
                key: format!("k{i}"),
                value: "v".into(),
            },
        );
        sim.run_for(80);
    }
    sim.run_for(500);

    let mut at_index: BTreeMap<u64, (u32, Option<KvCommand>)> = BTreeMap::new();
    for id in sim.node_ids() {
        // Applied entries must also arrive in index order with no gaps.
        for (position, e) in sim.applied(id).iter().enumerate() {
            let expect = position as u64 + 1;
            assert_eq!(e.index, expect, "node {id} applied out of order");
            match at_index.get(&e.index) {
                Some((other, cmd)) => assert_eq!(
                    cmd, &e.command,
                    "nodes {other} and {id} applied different commands at index {}",
                    e.index
                ),
                None => {
                    at_index.insert(e.index, (id, e.command.clone()));
                }
            }
        }
    }
    assert!(
        at_index.len() >= 10,
        "not enough entries applied to be a real check"
    );
}

/// What each node has on disk must match what it has in memory. If it does not,
/// crash recovery (step 7) will silently restore the wrong thing.
#[test]
fn durable_state_tracks_in_memory_state() {
    let mut sim = cluster(23, 3);
    let leader = sim.run_until_leader(5_000).expect("leader");
    for i in 0..6u64 {
        sim.submit(
            leader,
            1,
            i,
            KvCommand::Put {
                key: "k".into(),
                value: format!("{i}"),
            },
        );
        sim.run_for(90);
    }
    sim.run_for(400);

    for id in sim.node_ids() {
        let node = sim.node(id);
        let disk = sim.storage(id);
        assert_eq!(disk.current_term, node.current_term(), "node {id} term");
        assert_eq!(disk.voted_for, node.voted_for(), "node {id} vote");
        assert_eq!(disk.log, *node.log(), "node {id} log");
        assert!(disk.writes > 0);
    }
}

#[test]
fn every_node_votes_at_most_once_per_term() {
    // The persisted vote is what enforces this across crashes; here we check
    // the in-memory rule holds while running.
    let mut sim = cluster(31, 5);
    let mut votes: BTreeMap<Term, BTreeMap<u32, u32>> = BTreeMap::new();
    for _ in 0..8_000 {
        if !sim.step_once() {
            break;
        }
        for (id, node) in sim.nodes() {
            if let Some(candidate) = node.voted_for() {
                let per_term = votes.entry(node.current_term()).or_default();
                if let Some(prev) = per_term.insert(*id, candidate) {
                    assert_eq!(
                        prev,
                        candidate,
                        "node {id} voted for {prev} and then {candidate} in term {}",
                        node.current_term()
                    );
                }
            }
        }
    }
    let distinct_terms: BTreeSet<Term> = votes.keys().copied().collect();
    assert!(!distinct_terms.is_empty(), "no votes were cast at all");
}
