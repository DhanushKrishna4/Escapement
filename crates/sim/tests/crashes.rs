//! Crash, restart, pause and clock skew.
//!
//! The central claim being tested: a node that dies and comes back with nothing
//! but its disk must rejoin correctly. That is only meaningful because the
//! simulator throws away *everything* else on a crash — the role, `commitIndex`,
//! `lastApplied` and the entire state machine — so the log has to replay into a
//! fresh store. Persist too little and recovery loses something; keep anything
//! extra and the test would pass for the wrong reason.

use kvstore::KvCommand;
use raft::{BugSwitches, RaftConfig, Role};
use sim::invariants::Invariant;
use sim::{Cluster, DiskConfig, FaultConfig, NetworkConfig, NodeStatus, SimConfig};

fn cluster(seed: u64, nodes: usize) -> Cluster {
    Cluster::new(SimConfig {
        seed,
        nodes,
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

fn leader_within(sim: &mut Cluster, among: &[u32], ticks: u64) -> Option<u32> {
    let deadline = sim.now() + ticks;
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

// ---------------------------------------------------------------------------
// Recovery correctness
// ---------------------------------------------------------------------------

#[test]
fn a_restarted_node_keeps_its_durable_state_and_loses_everything_else() {
    let mut sim = cluster(1, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    for i in 0..4u64 {
        write(&mut sim, leader, i, &format!("k{i}"), "v");
        sim.run_for(300);
    }
    sim.run_for(1_000);

    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
    let term_before = sim.node(victim).current_term();
    let vote_before = sim.node(victim).voted_for();
    let log_before = sim.node(victim).log().clone();
    let commit_before = sim.node(victim).commit_index();
    assert!(commit_before > 0 && !log_before.is_empty());

    sim.crash(victim);
    assert_eq!(sim.status(victim), NodeStatus::Crashed);
    sim.restart(victim);

    // Persistent state (§5.1, Figure 2) survives exactly.
    assert_eq!(
        sim.node(victim).current_term(),
        term_before,
        "currentTerm must survive"
    );
    assert_eq!(
        sim.node(victim).voted_for(),
        vote_before,
        "votedFor must survive"
    );
    assert_eq!(sim.node(victim).log(), &log_before, "the log must survive");

    // Volatile state is gone.
    assert_eq!(
        sim.node(victim).role(),
        Role::Follower,
        "a node wakes up a follower"
    );
    assert_eq!(
        sim.node(victim).commit_index(),
        0,
        "commitIndex is volatile and legitimately restarts at 0"
    );
    assert_eq!(sim.node(victim).last_applied(), 0);
    assert!(
        sim.machine(victim).is_empty(),
        "the state machine is volatile and must be rebuilt from the log"
    );
}

#[test]
fn a_restarted_node_replays_its_log_into_a_fresh_state_machine() {
    let mut sim = cluster(2, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    for i in 0..5u64 {
        write(&mut sim, leader, i, &format!("k{i}"), &format!("v{i}"));
        sim.run_for(300);
    }
    sim.run_for(1_000);

    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
    let expected = sim.machine(victim).snapshot().clone();
    assert_eq!(expected.len(), 5);

    sim.crash(victim);
    sim.restart(victim);
    assert!(sim.machine(victim).is_empty());

    // Once the leader tells it what is committed, it replays.
    sim.run_for(5_000);
    assert_eq!(
        sim.machine(victim).snapshot(),
        &expected,
        "the log alone should rebuild the state machine"
    );
    sim.assert_no_violations();
}

#[test]
fn a_crashed_leader_is_replaced_and_rejoins_as_a_follower() {
    let mut sim = cluster(3, 5);
    let old = sim.run_until_leader(5_000).expect("leader");
    write(&mut sim, old, 0, "k", "before");
    sim.run_for(1_000);

    sim.crash(old);
    let others: Vec<u32> = sim.node_ids().into_iter().filter(|id| *id != old).collect();
    let new = leader_within(&mut sim, &others, 20_000).expect("a new leader");
    assert_ne!(new, old);

    for i in 0..3u64 {
        write(&mut sim, new, 10 + i, "k", &format!("after{i}"));
        sim.run_for(300);
    }
    sim.run_for(1_000);

    sim.restart(old);
    sim.run_for(10_000);

    assert_eq!(sim.node(old).role(), Role::Follower);
    assert_eq!(
        sim.machine(old).snapshot(),
        sim.machine(new).snapshot(),
        "the revived leader should have caught up on what it missed"
    );
    sim.assert_no_violations();
}

#[test]
fn losing_a_majority_stops_progress_and_regaining_it_resumes() {
    let mut sim = cluster(4, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    write(&mut sim, leader, 0, "k", "committed");
    sim.run_for(1_000);
    let commit_before = sim.node(leader).commit_index();

    // Kill three of five. Two survivors cannot form a quorum.
    let victims: Vec<u32> = sim
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .take(3)
        .collect();
    for v in &victims {
        sim.crash(*v);
    }

    for i in 0..4u64 {
        write(&mut sim, leader, 10 + i, "k", &format!("stranded{i}"));
        sim.run_for(400);
    }
    sim.run_for(10_000);
    assert_eq!(
        sim.node(leader).commit_index(),
        commit_before,
        "no quorum means nothing new can commit"
    );

    for v in &victims {
        sim.restart(*v);
    }
    sim.run_for(20_000);
    let l = sim.leader().expect("a leader");
    assert!(
        sim.node(l).commit_index() > commit_before,
        "progress should resume once a quorum is back"
    );
    sim.assert_no_violations();
}

#[test]
fn a_node_crashed_and_restarted_repeatedly_still_converges() {
    let mut sim = cluster(5, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();

    for i in 0..6u64 {
        let target = sim.leader().unwrap_or(leader);
        write(&mut sim, target, i, &format!("k{i}"), "v");
        sim.run_for(400);
        sim.crash(victim);
        sim.run_for(300);
        sim.restart(victim);
        sim.run_for(400);
    }
    sim.run_for(20_000);

    let l = sim.leader().expect("a leader");
    assert_eq!(
        sim.machine(victim).snapshot(),
        sim.machine(l).snapshot(),
        "a repeatedly-crashing node should still end up in agreement"
    );
    sim.assert_no_violations();
}

// ---------------------------------------------------------------------------
// Pauses
// ---------------------------------------------------------------------------

#[test]
fn a_paused_node_stops_processing_and_wakes_to_a_backlog() {
    let mut sim = cluster(6, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();

    let applied_before = sim.applied(victim).len();
    sim.pause(victim, 3_000);
    assert!(matches!(sim.status(victim), NodeStatus::Paused { .. }));

    for i in 0..5u64 {
        write(&mut sim, leader, i, &format!("k{i}"), "v");
        sim.run_for(300);
    }
    assert_eq!(
        sim.applied(victim).len(),
        applied_before,
        "a frozen node must not process anything"
    );
    assert!(
        sim.stats().messages_deferred > 0,
        "its messages should have been held, not dropped"
    );

    // It wakes up, drains the backlog, and catches up.
    sim.run_for(10_000);
    assert_eq!(sim.status(victim), NodeStatus::Running);
    assert_eq!(
        sim.machine(victim).snapshot(),
        sim.machine(leader).snapshot(),
        "it should catch up once it resumes"
    );
    sim.assert_no_violations();
}

/// A leader frozen for longer than an election timeout comes back to find the
/// world has moved on. It must notice and step down rather than carrying on as
/// if nothing happened.
#[test]
fn a_paused_leader_steps_down_when_it_wakes_up() {
    let mut sim = cluster(7, 5);
    let old = sim.run_until_leader(5_000).expect("leader");
    sim.run_for(1_000);
    let term_before = sim.node(old).current_term();

    sim.pause(old, 4_000);
    let others: Vec<u32> = sim.node_ids().into_iter().filter(|id| *id != old).collect();
    let new = leader_within(&mut sim, &others, 20_000).expect("someone else takes over");
    assert_ne!(new, old);

    sim.run_for(20_000);
    assert_eq!(
        sim.node(old).role(),
        Role::Follower,
        "the woken leader must concede to the higher term"
    );
    assert!(sim.node(old).current_term() > term_before);
    assert_eq!(sim.leaders().len(), 1);
    sim.assert_no_violations();
}

// ---------------------------------------------------------------------------
// Clock skew
// ---------------------------------------------------------------------------

fn skewed(seed: u64, skew: u32, faults: FaultConfig) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 5,
        clock_skew_permille: skew,
        faults,
        ..SimConfig::default()
    });
    sim.run_until_leader(20_000);
    let mut issued = 0u64;
    while sim.now() < 25_000 {
        let target = sim.leader().unwrap_or((issued % 5) as u32);
        write(
            &mut sim,
            target,
            issued,
            &format!("k{}", issued % 4),
            &format!("v{issued}"),
        );
        issued += 1;
        sim.run_for(250);
    }
    sim
}

#[test]
fn nodes_really_do_run_on_different_clocks() {
    let sim = skewed(1, 200, FaultConfig::none());
    let rates: Vec<u64> = sim
        .node_ids()
        .into_iter()
        .map(|id| sim.clock_rate(id))
        .collect();
    let distinct: std::collections::BTreeSet<u64> = rates.iter().copied().collect();
    assert!(
        distinct.len() > 1,
        "every clock ran at the same rate: {rates:?}"
    );
    assert!(
        rates.iter().all(|r| (800..=1200).contains(r)),
        "rates outside the configured skew: {rates:?}"
    );
}

#[test]
fn a_cluster_with_skewed_clocks_still_works() {
    for seed in 0..16u64 {
        let sim = skewed(seed, 200, FaultConfig::none());
        sim.assert_no_violations();
        let l = sim.leader().expect("a leader despite skew");
        assert!(
            sim.node(l).commit_index() > 10,
            "seed {seed}: the cluster made no progress"
        );
    }
}

#[test]
fn skew_does_not_break_determinism() {
    for seed in 0..8u64 {
        let a = skewed(seed, 300, FaultConfig::aggressive());
        let b = skewed(seed, 300, FaultConfig::aggressive());
        assert_eq!(
            a.trace().digest(),
            b.trace().digest(),
            "seed {seed} not reproducible; first difference at {:?}",
            a.trace().first_difference(b.trace())
        );
    }
}

// ---------------------------------------------------------------------------
// Checker validation, now end to end
// ---------------------------------------------------------------------------

/// Steps 4 and 6 could only validate `TermMonotonic` by handing the checker
/// fabricated states, because a correct node cannot un-learn a term while it is
/// running. With crashes it finally becomes reachable for real: a node that
/// does not persist `currentTerm` and `votedFor` comes back believing it is in
/// term 0 having voted for nobody.
#[test]
fn a_node_that_does_not_persist_its_term_regresses_on_restart() {
    let mut sim = Cluster::new(SimConfig {
        seed: 11,
        nodes: 5,
        raft: RaftConfig {
            bugs: BugSwitches {
                skip_hard_state_persistence: true,
                ..BugSwitches::default()
            },
            ..RaftConfig::default()
        },
        ..SimConfig::default()
    });
    let leader = sim.run_until_leader(5_000).expect("leader");
    sim.run_for(2_000);

    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
    assert!(sim.node(victim).current_term() > 0);
    assert_eq!(
        sim.storage(victim).current_term,
        0,
        "the bug is supposed to be on: nothing reached the disk"
    );

    sim.crash(victim);
    sim.restart(victim);
    sim.run_for(2_000);

    assert!(
        sim.broken_invariants().contains(&Invariant::TermMonotonic),
        "the checker missed a term going backwards across a restart: {:?}",
        sim.violations()
    );
    let text = sim
        .violations()
        .iter()
        .find(|v| v.invariant == Invariant::TermMonotonic)
        .unwrap()
        .to_string();
    assert!(text.contains("§5.1"), "{text}");
    assert!(text.contains("check persistence"), "{text}");
}

/// The control: with persistence working, the identical crash and restart is
/// completely clean.
#[test]
fn a_node_that_does_persist_its_term_restarts_cleanly() {
    let mut sim = cluster(11, 5);
    let leader = sim.run_until_leader(5_000).expect("leader");
    sim.run_for(2_000);
    let victim = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
    let term = sim.node(victim).current_term();
    assert_eq!(
        sim.storage(victim).current_term,
        term,
        "it reached the disk"
    );

    sim.crash(victim);
    sim.restart(victim);
    sim.run_for(2_000);

    assert_eq!(sim.node(victim).current_term(), term, "and came back");
    sim.assert_no_violations();
}

// ---------------------------------------------------------------------------
// Randomized
// ---------------------------------------------------------------------------

fn randomized(seed: u64, net: NetworkConfig, faults: FaultConfig, skew: u32) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 5,
        network: net,
        faults,
        clock_skew_permille: skew,
        ..SimConfig::default()
    });
    sim.run_until_leader(20_000);
    let mut issued = 0u64;
    while sim.now() < 30_000 {
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
fn safety_holds_under_crashes_alone() {
    for seed in 0..24u64 {
        let sim = randomized(seed, NetworkConfig::perfect(), FaultConfig::crash_only(), 0);
        if !sim.violations().is_empty() {
            let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
            panic!("seed {seed}:\n{}", report.join("\n"));
        }
    }
}

#[test]
fn safety_holds_with_everything_at_once() {
    // Bad network, partitions, crashes, pauses and skewed clocks together.
    for seed in 0..24u64 {
        let sim = randomized(
            seed,
            NetworkConfig::long_tail(),
            FaultConfig::aggressive(),
            150,
        );
        if !sim.violations().is_empty() {
            let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
            panic!("seed {seed}:\n{}", report.join("\n"));
        }
    }
}

#[test]
fn determinism_holds_with_crashes_and_skew() {
    for seed in 0..8u64 {
        let a = randomized(
            seed,
            NetworkConfig::long_tail(),
            FaultConfig::aggressive(),
            150,
        );
        let b = randomized(
            seed,
            NetworkConfig::long_tail(),
            FaultConfig::aggressive(),
            150,
        );
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
fn the_node_faults_actually_fire() {
    let mut crashes = 0;
    let mut restarts = 0;
    let mut pauses = 0;
    let mut deferred = 0;
    for seed in 0..16u64 {
        let sim = randomized(seed, NetworkConfig::perfect(), FaultConfig::aggressive(), 0);
        crashes += sim.stats().crashes;
        restarts += sim.stats().restarts;
        pauses += sim.stats().pauses;
        deferred += sim.stats().messages_deferred;
    }
    assert!(crashes > 20, "crashes barely fired: {crashes}");
    assert!(restarts > 20, "restarts barely fired: {restarts}");
    assert!(pauses > 5, "pauses barely fired: {pauses}");
    assert!(
        deferred > 20,
        "no messages were ever held for a paused node"
    );
}

/// Crashes attack availability and recovery, not log consistency — and that is
/// correct, not a gap in the model.
///
/// A leader appends an entry and broadcasts it in the *same* step, so by the
/// time it can crash, the messages are already in the network and will be
/// delivered regardless. The followers therefore have the entry too, and there
/// is nothing to diverge from. Divergence needs the append to survive while the
/// send does *not*, which is what a partition provides and a crash does not.
///
/// (The one case a crash could produce it is a torn step: persist the entry,
/// die before the send. Steps are atomic here, so that is not modelled — see
/// the journal for the disk-fault discussion.)
///
/// What crashes *do* exercise, heavily, is recovery: 260 restarts across these
/// seeds, each rebuilding its state machine by replaying its log.
#[test]
fn crashes_attack_availability_while_partitions_attack_consistency() {
    let (mut crash_trunc, mut crash_restarts, mut crash_elections) = (0u64, 0u64, 0u64);
    let (mut part_trunc, mut part_restarts) = (0u64, 0u64);
    for seed in 0..16u64 {
        let c = randomized(seed, NetworkConfig::perfect(), FaultConfig::crash_only(), 0);
        crash_trunc += c.stats().log_truncations;
        crash_restarts += c.stats().restarts;
        crash_elections += c.stats().elections_started;

        let p = randomized(seed, NetworkConfig::perfect(), FaultConfig::occasional(), 0);
        part_trunc += p.stats().log_truncations;
        part_restarts += p.stats().restarts;
    }

    assert!(
        crash_restarts > 100,
        "crash_only should exercise recovery hard: {crash_restarts} restarts"
    );
    assert!(
        crash_elections > 100,
        "and force plenty of elections: {crash_elections}"
    );
    assert!(
        crash_trunc < 10,
        "but crashes alone should barely diverge any logs: {crash_trunc} truncations"
    );
    assert!(
        part_trunc > crash_trunc * 3,
        "partitions should cause far more divergence than crashes: \
         {part_trunc} vs {crash_trunc}"
    );
    assert!(part_restarts > 0, "the mixed schedule still restarts nodes");
}

// ---------------------------------------------------------------------------
// Torn steps
// ---------------------------------------------------------------------------

fn torn(seed: u64, permille: u32, faults: FaultConfig) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 5,
        disk: DiskConfig {
            torn_step_permille: permille,
            ..DiskConfig::reliable()
        },
        faults,
        ..SimConfig::default()
    });
    sim.run_until_leader(20_000);
    let mut issued = 0u64;
    while sim.now() < 30_000 {
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
    // Let any tear near the end of the run get its restart.
    sim.run_for(5_000);
    sim
}

#[test]
fn torn_steps_fire_and_kill_the_node() {
    let sim = torn(1, 20, FaultConfig::none());
    assert!(
        sim.stats().torn_steps > 5,
        "torn steps barely fired: {:?}",
        sim.stats()
    );
    assert_eq!(
        sim.stats().restarts,
        sim.stats().crashes,
        "every torn node should have been brought back"
    );
}

/// Safety must survive a torn step, and the reason it does is the ordering
/// contract: persists are emitted before sends, so a write that did not reach
/// the disk cannot have reached the wire either.
///
/// The sharp case is a vote. If a node could reply "I voted for you" and then
/// die having lost the record, it would come back free to vote again in the
/// same term and elect a second leader. It cannot, because the reply is ordered
/// after the write that is being torn away.
#[test]
fn safety_survives_torn_steps() {
    for seed in 0..24u64 {
        let sim = torn(seed, 20, FaultConfig::none());
        if !sim.violations().is_empty() {
            let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
            panic!("seed {seed}:\n{}", report.join("\n"));
        }
    }
}

#[test]
fn safety_survives_torn_steps_combined_with_everything_else() {
    for seed in 0..24u64 {
        let mut sim = Cluster::new(SimConfig {
            seed,
            nodes: 5,
            network: NetworkConfig::long_tail(),
            faults: FaultConfig::aggressive(),
            disk: DiskConfig::flaky(),
            clock_skew_permille: 150,
            ..SimConfig::default()
        });
        sim.run_until_leader(20_000);
        let mut issued = 0u64;
        while sim.now() < 30_000 {
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
        if !sim.violations().is_empty() {
            let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
            panic!("seed {seed}:\n{}", report.join("\n"));
        }
    }
}

/// The prediction from entry 5, checked.
///
/// A plain crash cannot diverge a log, because the leader appends and
/// broadcasts in the same step — by the time it can die the messages are
/// already gone. A torn step is exactly what separates the two: the entry
/// reaches the disk and nothing reaches the network. This is the only fault
/// that lets a crash produce divergence, and it does.
#[test]
fn torn_steps_are_the_crash_that_can_diverge_a_log() {
    let mut plain = 0u64;
    let mut torn_trunc = 0u64;
    for seed in 0..24u64 {
        plain += torn(seed, 0, FaultConfig::crash_only())
            .stats()
            .log_truncations;
        torn_trunc += torn(seed, 30, FaultConfig::none()).stats().log_truncations;
    }
    assert!(
        torn_trunc > plain,
        "torn steps should diverge more than plain crashes: {torn_trunc} vs {plain}"
    );
    assert!(
        torn_trunc > 5,
        "torn steps should cause real divergence, got {torn_trunc} truncations"
    );
}

#[test]
fn determinism_holds_with_torn_steps() {
    for seed in 0..8u64 {
        let a = torn(seed, 30, FaultConfig::aggressive());
        let b = torn(seed, 30, FaultConfig::aggressive());
        assert_eq!(
            a.trace().digest(),
            b.trace().digest(),
            "seed {seed} not reproducible; first difference at {:?}",
            a.trace().first_difference(b.trace())
        );
    }
}
