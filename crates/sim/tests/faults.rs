//! Behaviour under a misbehaving network: drops, variable latency, duplication
//! and reordering.
//!
//! Two very different claims are being tested here, and they should not be
//! confused:
//!
//! * **Safety** must hold under *any* network, however bad. A network that
//!   loses a quarter of everything is allowed to stop the cluster making
//!   progress; it is never allowed to make it disagree.
//! * **Liveness** only has to hold when the network is good enough. Raft
//!   guarantees progress when message delay is reliably less than the election
//!   timeout, so it is tested against the mild presets, not the hostile one.

use kvstore::KvCommand;
use sim::{Cluster, LatencyModel, NetworkConfig, SimConfig};

fn run(seed: u64, nodes: usize, net: NetworkConfig, ticks: u64) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes,
        network: net,
        ..SimConfig::default()
    });
    sim.run_until_leader(10_000);

    // A client that behaves like a real one: aim at whoever is currently
    // leader, and keep going. Some of these will never be answered, which is
    // exactly the case the linearizability checker has to reason about later.
    let mut issued = 0u64;
    while sim.now() < ticks {
        if let Some(leader) = sim.leader() {
            sim.submit(
                leader,
                1,
                issued,
                KvCommand::Put {
                    key: format!("k{}", issued % 4),
                    value: format!("v{issued}"),
                },
            );
            issued += 1;
        }
        sim.run_for(150);
    }
    sim
}

/// A named network preset. Aliased because the bare tuple-of-fn-pointer type
/// is unreadable at every use site.
type Preset = (&'static str, fn() -> NetworkConfig);

const PRESETS: [Preset; 4] = [
    ("perfect", NetworkConfig::perfect),
    ("flaky", NetworkConfig::flaky),
    ("long_tail", NetworkConfig::long_tail),
    ("hostile", NetworkConfig::hostile),
];

// ---------------------------------------------------------------------------
// Safety
// ---------------------------------------------------------------------------

#[test]
fn safety_holds_under_every_network_preset() {
    for (name, preset) in PRESETS {
        for seed in 0..24u64 {
            let sim = run(seed, 5, preset(), 12_000);
            if !sim.violations().is_empty() {
                let report: Vec<String> = sim.violations().iter().map(|v| v.to_string()).collect();
                panic!(
                    "preset {name}, seed {seed}: {} violation(s)\n{}",
                    report.len(),
                    report.join("\n")
                );
            }
        }
    }
}

#[test]
fn safety_holds_on_a_three_node_cluster_too() {
    // Three nodes means a quorum of two, so a single lost message matters more.
    for seed in 0..24u64 {
        let sim = run(seed, 3, NetworkConfig::long_tail(), 12_000);
        sim.assert_no_violations();
    }
}

/// Duplication on its own, cranked to the maximum. Every message arrives twice.
/// A repeated AppendEntries must not truncate a log that has moved on, and a
/// repeated vote must not be counted twice.
#[test]
fn raft_is_idempotent_when_every_message_is_duplicated() {
    for seed in 0..16u64 {
        let net = NetworkConfig {
            latency: LatencyModel::Uniform { min: 5, max: 25 },
            duplicate_permille: 1000,
            drop_permille: 0,
            reorder_permille: 0,
        };
        let sim = run(seed, 5, net, 8_000);
        sim.assert_no_violations();
        assert!(
            sim.network_stats().duplicated > 100,
            "seed {seed}: duplication barely fired"
        );
    }
}

/// Reordering on its own. Messages overtake each other on every link.
#[test]
fn raft_survives_aggressive_reordering() {
    for seed in 0..16u64 {
        let net = NetworkConfig {
            latency: LatencyModel::Uniform { min: 2, max: 60 },
            drop_permille: 0,
            duplicate_permille: 0,
            reorder_permille: 1000,
        };
        let sim = run(seed, 5, net, 8_000);
        sim.assert_no_violations();
        assert!(
            sim.network_stats().reordered > 50,
            "seed {seed}: no reordering happened"
        );
    }
}

// ---------------------------------------------------------------------------
// Liveness
// ---------------------------------------------------------------------------

#[test]
fn the_cluster_still_makes_progress_on_a_flaky_network() {
    let presets: [Preset; 2] = [
        ("flaky", NetworkConfig::flaky),
        ("long_tail", NetworkConfig::long_tail),
    ];
    for (name, preset) in presets {
        let mut stuck = Vec::new();
        for seed in 0..16u64 {
            let sim = run(seed, 5, preset(), 12_000);
            let committed = sim.node(sim.leader().unwrap_or(0)).commit_index();
            if committed < 10 {
                stuck.push((seed, committed));
            }
        }
        assert!(
            stuck.is_empty(),
            "preset {name}: seeds made no real progress: {stuck:?}"
        );
    }
}

#[test]
fn every_node_converges_on_the_same_state_once_the_dust_settles() {
    for seed in 0..16u64 {
        let mut sim = run(seed, 5, NetworkConfig::flaky(), 10_000);
        // Stop issuing work and let replication catch up.
        sim.run_for(15_000);

        let ids = sim.node_ids();
        let reference = sim.machine(ids[0]).snapshot().clone();
        for id in &ids {
            assert_eq!(
                sim.machine(*id).snapshot(),
                &reference,
                "seed {seed}: node {id} never caught up"
            );
        }
        assert!(
            !reference.is_empty(),
            "seed {seed}: nothing was ever committed"
        );
    }
}

// ---------------------------------------------------------------------------
// Determinism, again — now with far more randomness in play
// ---------------------------------------------------------------------------

#[test]
fn determinism_holds_under_faults() {
    for (name, preset) in PRESETS {
        for seed in 0..12u64 {
            let a = run(seed, 5, preset(), 6_000);
            let b = run(seed, 5, preset(), 6_000);
            assert_eq!(
                a.trace().digest(),
                b.trace().digest(),
                "preset {name}, seed {seed} was not reproducible; first difference at {:?}",
                a.trace().first_difference(b.trace())
            );
            assert_eq!(a.network_stats(), b.network_stats());
        }
    }
}

/// Turning a fault knob to zero must leave the run byte-identical, so "same
/// seed, one knob changed" is a meaningful comparison rather than an unrelated
/// universe.
#[test]
fn a_disabled_fault_changes_nothing() {
    let base = NetworkConfig {
        latency: LatencyModel::Fixed(10),
        drop_permille: 0,
        duplicate_permille: 0,
        reorder_permille: 0,
    };
    let a = run(3, 5, base.clone(), 6_000);
    let b = run(3, 5, NetworkConfig::perfect(), 6_000);
    assert_eq!(a.trace().digest(), b.trace().digest());
}

// ---------------------------------------------------------------------------
// Coverage — a clean result means nothing if the faults never fired
// ---------------------------------------------------------------------------

#[test]
fn the_faults_actually_fire() {
    let sim = run(1, 5, NetworkConfig::hostile(), 12_000);
    let stats = sim.network_stats();
    assert!(stats.sent > 1_000, "barely any traffic: {stats:?}");
    assert!(stats.dropped > 100, "drops did not fire: {stats:?}");
    assert!(stats.duplicated > 20, "duplication did not fire: {stats:?}");
    assert!(stats.reordered > 20, "reordering did not fire: {stats:?}");
    assert!(
        stats.max_delay > 100,
        "the long tail never produced a slow message: {stats:?}"
    );
    assert!(
        sim.stats().elections_started > 5,
        "the cluster never had to re-elect: {:?}",
        sim.stats()
    );

    // And a perfect network must report none of it.
    let clean = run(1, 5, NetworkConfig::perfect(), 6_000);
    let stats = clean.network_stats();
    assert_eq!(
        (stats.dropped, stats.duplicated, stats.reordered),
        (0, 0, 0)
    );
}

/// How far these tests actually reach.
///
/// This is the honest accounting, and it is asserted so it cannot quietly
/// regress. Message loss alone does NOT produce log divergence: a follower
/// needs several consecutive heartbeats to go missing before it times out, so
/// at a few per cent loss the leader simply never changes, and with one leader
/// there is nothing to diverge from. Only the hostile preset churns leadership
/// hard enough to truncate anything.
///
/// So "24 seeds x 4 presets, no violations" is weaker evidence than it looks.
/// The truncation path is covered precisely by the handcrafted scenarios in
/// `checker_validation.rs`; covering it *randomly* needs partitions (step 6)
/// and crashes (step 7), where an isolated leader keeps appending entries that
/// a later leader has never seen.
#[test]
fn how_much_of_raft_these_faults_actually_reach() {
    let mild = run(1, 5, NetworkConfig::flaky(), 12_000);
    assert_eq!(
        mild.stats().log_truncations,
        0,
        "a few per cent loss should not be enough to destabilise leadership"
    );
    assert!(
        mild.stats().max_term <= 4,
        "a flaky network should not churn leaders: reached term {}",
        mild.stats().max_term
    );

    // The hostile preset does reach divergence, if only barely.
    let mut truncations = 0;
    let mut max_term = 0;
    for seed in 0..24u64 {
        let sim = run(seed, 5, NetworkConfig::hostile(), 12_000);
        truncations += sim.stats().log_truncations;
        max_term = max_term.max(sim.stats().max_term);
    }
    assert!(
        truncations > 0,
        "no preset reaches log truncation at all; the fault tests are not \
         exercising divergence and the suite should say so"
    );
    assert!(
        max_term > 10,
        "hostile should churn leadership: max term {max_term}"
    );
}

#[test]
fn a_bad_network_forces_more_elections_than_a_good_one() {
    // Sanity that the faults are actually stressing the algorithm rather than
    // being absorbed silently.
    let calm = run(5, 5, NetworkConfig::perfect(), 12_000);
    let rough = run(5, 5, NetworkConfig::hostile(), 12_000);
    let calm_term = calm.nodes().map(|(_, n)| n.current_term()).max().unwrap();
    let rough_term = rough.nodes().map(|(_, n)| n.current_term()).max().unwrap();
    assert!(
        rough_term > calm_term,
        "a hostile network should cause more elections: {rough_term} vs {calm_term}"
    );
}
