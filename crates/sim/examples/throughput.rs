//! How fast does the simulator run?
//!
//! The fuzzer's whole value proposition is tens of thousands of seeds in
//! minutes, which only works if a single run is cheap. This lives in
//! `examples/` rather than `src/` on purpose: it reads the wall clock, which is
//! banned in core and enforced by `tests/no_nondeterminism.rs`.
//!
//! Run with: cargo run --release --example throughput

use std::time::Instant;

use kvstore::KvCommand;
use sim::{Cluster, SimConfig};

fn main() {
    for (nodes, check_invariants) in [(3usize, true), (5, true), (3, false), (5, false)] {
        // Warm run to reach a steady state with a leader and live traffic.
        let mut sim = Cluster::new(SimConfig {
            seed: 42,
            nodes,
            check_invariants,
            ..SimConfig::default()
        });
        sim.run_until_leader(5_000);

        let target_events = 2_000_000u64;
        let start = Instant::now();
        let mut submitted = 0u64;
        while sim.events_processed() < target_events {
            if !sim.step_once() {
                break;
            }
            // Keep the log growing so this measures real work, not idle
            // heartbeats.
            if sim.events_processed().is_multiple_of(64) {
                if let Some(leader) = sim.leader() {
                    sim.submit(
                        leader,
                        1,
                        submitted,
                        KvCommand::Put {
                            key: format!("k{}", submitted % 16),
                            value: format!("v{submitted}"),
                        },
                    );
                    submitted += 1;
                }
            }
        }
        let elapsed = start.elapsed();
        let events = sim.events_processed();
        let per_sec = events as f64 / elapsed.as_secs_f64();

        println!(
            "{nodes} nodes, checks {}: {events} events in {:.2?} = {:.2} M events/sec \
             ({} invariant checks, {} commands, trace {} records)",
            if check_invariants { "on " } else { "off" },
            elapsed,
            per_sec / 1e6,
            sim.checker().checks(),
            submitted,
            sim.trace().len()
        );
    }
}
