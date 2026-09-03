//! The determinism test. This one is non-negotiable.
//!
//! If a run of a given seed ever stops being byte-identical to another run of
//! that seed, every recorded repro seed in the project becomes meaningless and
//! the fuzzer can no longer hand anyone a failure they can reproduce. So the
//! comparison is over the *full trace* -- every input delivered and every
//! output produced -- not over final state, which would let a divergence that
//! happens to reconverge slip through unnoticed.

use kvstore::KvCommand;
use sim::{Cluster, SimConfig};

/// A run with enough going on to exercise timers, elections, replication and
/// client traffic.
fn run(seed: u64) -> Cluster {
    let mut sim = Cluster::new(SimConfig::with_seed(seed));
    sim.run_for(600);

    // Client traffic aimed at whoever is leading, plus some at a fixed node so
    // the NotLeader redirect path is exercised too.
    for round in 0..8u64 {
        let target = sim.leader().unwrap_or(0);
        sim.submit(
            target,
            1,
            round,
            KvCommand::Put {
                key: format!("k{}", round % 3),
                value: format!("v{round}"),
            },
        );
        sim.submit(2, 2, round, KvCommand::Get { key: "k0".into() });
        sim.run_for(200);
    }
    sim.run_for(1000);
    sim
}

/// Everything observable about a finished run, as a string. Used to check that
/// state agrees as well as the trace.
fn state_fingerprint(sim: &Cluster) -> String {
    let mut s = String::new();
    for (id, node) in sim.nodes() {
        s.push_str(&format!(
            "node {id}: role={:?} term={} commit={} applied={} voted_for={:?} last={:?}\n",
            node.role(),
            node.current_term(),
            node.commit_index(),
            node.last_applied(),
            node.voted_for(),
            node.last_log_id(),
        ));
        s.push_str(&format!("  kv: {:?}\n", sim.machine(*id).snapshot()));
        s.push_str(&format!(
            "  disk: term={} voted_for={:?} last={:?} writes={}\n",
            sim.storage(*id).current_term,
            sim.storage(*id).voted_for,
            sim.storage(*id).log.last_log_id(),
            sim.storage(*id).writes,
        ));
    }
    s.push_str(&format!("outcomes: {:?}\n", sim.outcomes()));
    s
}

#[test]
fn same_seed_produces_byte_identical_traces() {
    let a = run(0xC0FFEE);
    let b = run(0xC0FFEE);

    if let Some(i) = a.trace().first_difference(b.trace()) {
        panic!(
            "traces diverged at record {i}\n  a: {:?}\n  b: {:?}",
            a.trace().records().get(i),
            b.trace().records().get(i)
        );
    }

    assert_eq!(
        a.trace().to_json(),
        b.trace().to_json(),
        "traces must be byte-identical"
    );
    assert_eq!(a.trace().digest(), b.trace().digest());
    assert_eq!(a.events_processed(), b.events_processed());
    assert_eq!(state_fingerprint(&a), state_fingerprint(&b));
}

#[test]
fn determinism_holds_across_many_seeds() {
    for seed in 0..24u64 {
        let a = run(seed);
        let b = run(seed);
        assert_eq!(
            a.trace().digest(),
            b.trace().digest(),
            "seed {seed} was not reproducible; first difference at {:?}",
            a.trace().first_difference(b.trace())
        );
    }
}

/// Guards against the test passing for the wrong reason. If every seed produced
/// the same trace -- because nothing interesting happens, or the seed is not
/// actually threaded through -- the test above would pass while proving nothing.
#[test]
fn different_seeds_produce_different_runs() {
    let digests: Vec<u64> = (0..16u64).map(|s| run(s).trace().digest()).collect();
    let unique: std::collections::BTreeSet<u64> = digests.iter().copied().collect();
    assert!(
        unique.len() > 12,
        "seeds barely affect the run: {} distinct traces out of {}",
        unique.len(),
        digests.len()
    );
}

/// Interleaving two runs, rather than running one after the other, catches
/// determinism bugs that come from process-global state (a lazily initialized
/// hasher, a static counter) which a sequential comparison would miss.
#[test]
fn interleaved_runs_stay_in_lockstep() {
    let cfg = SimConfig::with_seed(99);
    let mut a = Cluster::new(cfg.clone());
    let mut b = Cluster::new(cfg);

    for i in 0..4000 {
        let ka = a.step_once();
        let kb = b.step_once();
        assert_eq!(ka, kb, "queues emptied at different points (event {i})");
        assert_eq!(a.now(), b.now(), "clocks diverged at event {i}");
        assert_eq!(
            a.trace().len(),
            b.trace().len(),
            "trace lengths diverged at event {i}"
        );
    }
    assert_eq!(a.trace().digest(), b.trace().digest());
}

/// The trace has to actually contain the run. A determinism test over an empty
/// trace is worthless.
#[test]
fn the_trace_is_not_trivially_empty() {
    let sim = run(7);
    assert!(
        sim.trace().len() > 500,
        "trace only has {} records; it is not capturing the run",
        sim.trace().len()
    );
    assert!(sim.events_processed() > 200);
}
