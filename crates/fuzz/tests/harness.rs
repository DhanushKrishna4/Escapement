//! Tests for the fuzzer itself.
//!
//! A harness that reports "no violations" is only worth something if it would
//! have said otherwise given a real bug. So most of what follows turns a
//! deliberate bug on and checks the harness finds it, minimizes it, and hands
//! back a repro that actually reproduces.

use fuzz::{config_for_seed, minimize, run_seed, sweep, Options};
use raft::BugSwitches;

fn with_bug(bugs: BugSwitches) -> Options {
    Options {
        ticks: 15_000,
        bugs,
        record_trace: false,
        minimize_budget: 300,
        ..Options::default()
    }
}

fn clean() -> Options {
    Options {
        ticks: 15_000,
        ..Options::default()
    }
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

#[test]
fn a_seed_produces_the_same_run_every_time() {
    let opts = clean();
    for seed in [0u64, 7, 999] {
        let a = run_seed(seed, &opts);
        let b = run_seed(seed, &opts);
        assert_eq!(a.stats, b.stats, "seed {seed} stats differ");
        assert_eq!(a.network, b.network, "seed {seed} network stats differ");
        assert_eq!(a.faults, b.faults, "seed {seed} fault schedules differ");
        assert_eq!(a.violations.len(), b.violations.len());
    }
}

#[test]
fn parallelism_does_not_change_results() {
    // Each seed's run is a pure function of its config, so thread count must
    // not be observable in the output. If it ever is, something is shared that
    // should not be.
    let opts = clean();
    let one = sweep(0, 40, 1, &opts);
    let many = sweep(0, 40, 8, &opts);
    assert_eq!(one.coverage.seeds, many.coverage.seeds);
    assert_eq!(one.coverage.stats, many.coverage.stats);
    assert_eq!(one.coverage.network, many.coverage.network);
    assert_eq!(
        one.failures.iter().map(|f| f.seed).collect::<Vec<_>>(),
        many.failures.iter().map(|f| f.seed).collect::<Vec<_>>()
    );
}

#[test]
fn seeds_actually_produce_different_configurations() {
    let opts = clean();
    let mut sizes = std::collections::BTreeSet::new();
    let mut batches = std::collections::BTreeSet::new();
    let mut fault_kinds = std::collections::BTreeSet::new();
    for seed in 0..200u64 {
        let (cfg, _) = config_for_seed(seed, &opts);
        sizes.insert(cfg.nodes);
        batches.insert(cfg.raft.max_entries_per_append);
        fault_kinds.insert(format!("{:?}", cfg.faults));
    }
    assert!(sizes.len() >= 3, "cluster size barely varies: {sizes:?}");
    assert!(batches.len() >= 3, "batch size barely varies: {batches:?}");
    assert!(fault_kinds.len() >= 4, "fault schedules barely vary");
}

// ---------------------------------------------------------------------------
// Can it find a bug?
// ---------------------------------------------------------------------------

#[test]
fn the_harness_finds_a_node_that_votes_twice() {
    let opts = with_bug(BugSwitches {
        vote_twice_per_term: true,
        ..BugSwitches::default()
    });
    let result = sweep(0, 60, 4, &opts);
    assert!(
        !result.failures.is_empty(),
        "60 seeds should be plenty to catch a node voting twice per term"
    );
}

#[test]
fn the_harness_finds_a_node_that_does_not_persist_its_term() {
    let opts = with_bug(BugSwitches {
        skip_hard_state_persistence: true,
        ..BugSwitches::default()
    });
    let result = sweep(0, 120, 4, &opts);
    assert!(
        !result.failures.is_empty(),
        "a node that forgets its term across a crash should be caught"
    );
}

#[test]
fn the_harness_finds_an_uncapped_leader_commit() {
    let opts = with_bug(BugSwitches {
        trust_leader_commit_blindly: true,
        ..BugSwitches::default()
    });
    let result = sweep(0, 120, 4, &opts);
    assert!(
        !result.failures.is_empty(),
        "a follower committing past the end of its log should be caught"
    );
}

/// The control that makes the three tests above mean something: the identical
/// sweep with no bug enabled must come back clean.
#[test]
fn a_clean_build_produces_no_violations() {
    let result = sweep(0, 120, 4, &clean());
    if !result.failures.is_empty() {
        let report: Vec<String> = result.failures.iter().map(fuzz::report::failure).collect();
        panic!("{}", report.join("\n"));
    }
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

#[test]
fn a_sweep_reaches_the_interesting_states() {
    // A green sweep proves nothing if the runs never diverged a log, never
    // crashed a node, and never dropped a packet.
    let result = sweep(0, 120, 4, &clean());
    let c = &result.coverage;
    assert!(
        c.stats.elections_started > 100,
        "too few elections: {:?}",
        c.stats
    );
    assert!(
        c.stats.log_truncations > 0,
        "no divergence was ever reached"
    );
    assert!(c.stats.crashes > 0, "no node ever crashed");
    assert!(c.stats.restarts > 0, "no node ever recovered");
    assert!(c.network.dropped > 0, "no message was ever dropped");
    assert!(
        c.network.partitioned > 0,
        "no partition ever blocked a message"
    );
    assert!(c.network.duplicated > 0, "no message was ever duplicated");
    assert!(
        c.stats.entries_applied > 1_000,
        "barely anything was committed"
    );
    assert!(
        c.schedules_with_faults > 90,
        "most seeds should schedule faults: {}",
        c.schedules_with_faults
    );
}

// ---------------------------------------------------------------------------
// Minimization
// ---------------------------------------------------------------------------

#[test]
fn minimizing_produces_a_repro_that_still_reproduces() {
    let opts = with_bug(BugSwitches {
        vote_twice_per_term: true,
        ..BugSwitches::default()
    });
    let failing = sweep(0, 60, 4, &opts)
        .failures
        .first()
        .expect("a failing seed")
        .seed;

    let m = minimize(failing, &opts).expect("a minimized repro");
    assert!(
        m.reproduced,
        "the failure should replay from a fixed script"
    );
    assert!(m.attempts > 0);

    // The minimized script must genuinely still fail, replayed from scratch.
    let (mut cfg, workload) = config_for_seed(failing, &opts);
    cfg.scripted_faults = Some(m.faults.clone());
    let (_, replayed) = fuzz::run_config(cfg, workload, m.ticks);
    let reproduces = match m.target {
        fuzz::Target::Invariant(i) => replayed.violations.iter().any(|v| v.invariant == i),
        fuzz::Target::Linearizability => replayed.linearizability.is_violation(),
    };
    assert!(
        reproduces,
        "the minimized repro no longer reproduces the failure it was shrunk from"
    );
}

#[test]
fn minimizing_actually_removes_faults() {
    let opts = with_bug(BugSwitches {
        skip_hard_state_persistence: true,
        ..BugSwitches::default()
    });
    let failures = sweep(0, 120, 4, &opts).failures;
    let mut shrank = 0;
    let mut checked = 0;
    for f in failures.iter().take(4) {
        if f.faults.len() < 4 {
            continue; // nothing to shrink
        }
        checked += 1;
        let m = minimize(f.seed, &opts).expect("a repro");
        if m.reproduced && m.faults.len() < m.original_faults {
            shrank += 1;
        }
    }
    assert!(checked > 0, "no failing seed had enough faults to shrink");
    assert!(
        shrank > 0,
        "minimization removed nothing from any of {checked} candidates"
    );
}

#[test]
fn minimizing_a_clean_seed_returns_nothing() {
    assert!(
        minimize(3, &clean()).is_none(),
        "there is nothing to minimize when nothing failed"
    );
}

#[test]
fn a_scripted_replay_reproduces_the_generated_schedule() {
    // Minimization rests on this: replaying a captured fault list must produce
    // the same run as generating it. If it did not, shrinking would be chasing
    // a different failure than the one reported.
    let opts = clean();
    let generated = run_seed(11, &opts);
    let (mut cfg, workload) = config_for_seed(11, &opts);
    cfg.scripted_faults = Some(generated.faults.clone());
    let (_, replayed) = fuzz::run_config(cfg, workload, opts.ticks);
    assert_eq!(
        generated.stats, replayed.stats,
        "a scripted replay diverged from the schedule it was captured from"
    );
}

// ---------------------------------------------------------------------------
// Linearizability, through the harness
// ---------------------------------------------------------------------------

#[test]
fn every_clean_seed_produces_a_linearizable_history() {
    let result = sweep(0, 120, 4, &clean());
    assert_eq!(
        result.coverage.histories_checked + result.coverage.histories_undecided,
        result.coverage.seeds,
        "every seed should get a verdict"
    );
    assert_eq!(
        result.coverage.histories_undecided, 0,
        "the default budget should be enough for these histories"
    );
}

/// The pending path is where a linearizability checker most easily goes wrong,
/// so a sweep that never strands an operation is not testing it.
#[test]
fn a_sweep_strands_operations_for_the_checker_to_reason_about() {
    let result = sweep(0, 120, 4, &clean());
    assert!(
        result.coverage.operations_pending > 100,
        "no operations were left unanswered: {}",
        result.coverage.operations_pending
    );
    assert!(
        result.coverage.operations_completed > 5_000,
        "barely any operations completed: {}",
        result.coverage.operations_completed
    );
}

/// A history that is impossible must be reported as a failure even when every
/// Raft invariant held, because the two check different things.
#[test]
fn a_linearizability_violation_counts_as_a_failure() {
    use sim::linearizability::Verdict;
    let mut result = run_seed(0, &clean());
    assert!(!result.failed());
    // Forge the verdict: the point is that `failed()` consults it at all.
    result.linearizability =
        Verdict::NotLinearizable(Box::new(sim::linearizability::Explanation {
            key: "x".into(),
            operations: vec![],
            linearized: vec![],
            state_after: None,
            blocked: vec![],
        }));
    assert!(
        result.failed(),
        "a broken history must count as a failing seed"
    );
}

/// The test that justifies the linearizability checker's existence.
///
/// With `stale_reads` a leader answers reads straight from its own state
/// machine, with no ReadIndex round to confirm it is still the leader. A leader
/// deposed behind a partition then serves values the rest of the cluster has
/// long since overwritten.
///
/// **Every Raft invariant still holds.** One leader per term, logs matching,
/// nothing committed and lost — because the read never enters the log at all.
/// The only thing wrong is the story told to clients, and the only thing that
/// can see it is the linearizability check.
#[test]
fn the_linearizability_checker_catches_what_no_invariant_can() {
    let opts = Options {
        ticks: 20_000,
        stale_reads: true,
        ..clean()
    };
    let result = sweep(0, 400, 4, &opts);
    assert!(
        !result.failures.is_empty(),
        "stale reads should produce an impossible history"
    );

    let only_linearizability = result
        .failures
        .iter()
        .filter(|f| f.violations.is_empty() && f.linearizability.is_violation())
        .count();
    assert!(
        only_linearizability > 0,
        "every stale-read failure also broke an invariant, so this proves nothing \
         about the linearizability checker"
    );

    // And the report must explain itself, not just say no.
    let f = result
        .failures
        .iter()
        .find(|f| f.linearizability.is_violation())
        .unwrap();
    let text = fuzz::report::failure(f);
    assert!(
        text.contains("the client-visible history is impossible"),
        "{text}"
    );
    assert!(text.contains("by invocation time"), "{text}");
}

/// The control: the identical sweep without the bug is clean.
#[test]
fn without_stale_reads_the_same_sweep_is_clean() {
    let opts = Options {
        ticks: 20_000,
        stale_reads: false,
        ..clean()
    };
    let result = sweep(0, 400, 4, &opts);
    assert!(
        result.failures.is_empty(),
        "{}",
        result
            .failures
            .iter()
            .map(fuzz::report::failure)
            .collect::<Vec<_>>()
            .join("\n")
    );
}
