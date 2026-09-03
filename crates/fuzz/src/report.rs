//! Turning results into something a person can act on.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use sim::linearizability::Verdict;
use sim::Fault;

use crate::{Minimized, SeedResult, Sweep};

/// The summary printed at the end of a sweep.
pub fn summary(sweep: &Sweep, seeds: u64) -> String {
    let c = &sweep.coverage;
    let mut out = String::new();
    let rate = if sweep.elapsed_secs > 0.0 {
        c.seeds as f64 / sweep.elapsed_secs
    } else {
        0.0
    };

    let _ = writeln!(
        out,
        "\n{} seeds in {:.2}s ({:.0} seeds/sec), {} violation(s)",
        c.seeds,
        sweep.elapsed_secs,
        rate,
        sweep.failures.len()
    );
    let _ = writeln!(out, "\ncoverage");
    let _ = writeln!(
        out,
        "  elections    {:>9}   leaders elected {:>8}   max term {:>6}",
        c.stats.elections_started, c.stats.leaders_elected, c.stats.max_term
    );
    let _ = writeln!(
        out,
        "  truncations  {:>9}   entries lost    {:>8}   applied  {:>6}",
        c.stats.log_truncations, c.stats.entries_truncated, c.stats.entries_applied
    );
    let _ = writeln!(
        out,
        "  crashes      {:>9}   restarts        {:>8}   pauses   {:>6}",
        c.stats.crashes, c.stats.restarts, c.stats.pauses
    );
    let _ = writeln!(
        out,
        "  torn steps   {:>9}   faults injected {:>8}   deferred {:>6}",
        c.stats.torn_steps, c.stats.faults_injected, c.stats.messages_deferred
    );
    let _ = writeln!(
        out,
        "  snapshots    {:>9}   installed       {:>8}   memberships {:>6}",
        c.stats.snapshots_taken, c.stats.snapshots_installed, c.stats.membership_changes
    );
    let _ = writeln!(
        out,
        "  reads served {:>9}   (via ReadIndex, never entering the log)",
        c.stats.reads_served
    );
    let _ = writeln!(
        out,
        "  msgs sent    {:>9}   dropped         {:>8}   partitioned {:>6}",
        c.network.sent, c.network.dropped, c.network.partitioned
    );
    let _ = writeln!(
        out,
        "  duplicated   {:>9}   reordered       {:>8}   max delay   {:>6}",
        c.network.duplicated, c.network.reordered, c.network.max_delay
    );
    let _ = writeln!(
        out,
        "  seeds with faults scheduled: {} of {}",
        c.schedules_with_faults, seeds
    );
    let _ = writeln!(
        out,
        "  client ops   {:>9}   never answered  {:>8}",
        c.operations_completed, c.operations_pending
    );
    let _ = writeln!(
        out,
        "  histories linearizable {:>7}   undecided {:>10}",
        c.histories_checked, c.histories_undecided
    );

    // A clean sweep means nothing if the runs never reached anything
    // interesting, so say so rather than letting a green result speak for
    // itself.
    let mut warnings = Vec::new();
    if c.stats.log_truncations == 0 {
        warnings.push("no log truncations: divergence was never reached");
    }
    if c.stats.elections_started < c.seeds {
        warnings.push("fewer elections than seeds: many runs may have done nothing");
    }
    if c.stats.entries_applied == 0 {
        warnings.push("nothing was ever applied: the workload is not committing");
    }
    if c.stats.snapshots_taken == 0 {
        warnings.push("no snapshots were taken: compaction is not being exercised");
    }
    if c.stats.snapshots_installed == 0 {
        warnings.push("no snapshot was ever shipped to a follower: InstallSnapshot is untested");
    }
    if c.stats.membership_changes == 0 {
        warnings.push("no membership changes: joint consensus is not being exercised");
    }
    if c.stats.reads_served == 0 {
        warnings.push("no reads were served: ReadIndex is not being exercised");
    }
    if c.operations_pending == 0 {
        warnings
            .push("no operation was ever left unanswered: the checker's pending path is untested");
    }
    if c.histories_undecided > c.histories_checked / 4 {
        warnings.push("many histories were undecided: raise the linearizability budget");
    }
    if !warnings.is_empty() {
        let _ = writeln!(out, "\nweak coverage:");
        for w in warnings {
            let _ = writeln!(out, "  - {w}");
        }
    }

    out
}

pub fn failure(result: &SeedResult) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "\nseed {} FAILED ({} nodes, {} violation(s))",
        result.seed,
        result.nodes,
        result.violations.len()
    );
    // Earliest first: the first violation is usually the cause and the rest are
    // downstream symptoms.
    let mut ordered = result.violations.clone();
    ordered.sort_by_key(|v| v.tick);
    for v in ordered.iter().take(5) {
        let _ = writeln!(out, "  {v}");
    }
    if ordered.len() > 5 {
        let _ = writeln!(out, "  ... and {} more", ordered.len() - 5);
    }
    if let Verdict::NotLinearizable(e) = &result.linearizability {
        let _ = writeln!(out, "\n  the client-visible history is impossible:");
        for line in e.to_string().lines() {
            let _ = writeln!(out, "  {line}");
        }
    }
    out
}

pub fn minimized(m: &Minimized) -> String {
    let mut out = String::new();
    if !m.reproduced {
        let _ = writeln!(
            out,
            "  could not reproduce seed {} from a fixed fault script; the seed alone is the repro",
            m.seed
        );
        return out;
    }
    let _ = writeln!(
        out,
        "  minimized: {} faults -> {} ({} removed), {} -> {} ticks, {} attempts",
        m.original_faults,
        m.faults.len(),
        m.shrank_by(),
        m.original_ticks,
        m.ticks,
        m.attempts
    );
    let _ = writeln!(out, "  preserving: {}", m.target.name());
    for (tick, fault) in &m.faults {
        let _ = writeln!(out, "    tick {tick:>7}  {}", describe_fault(fault));
    }
    out
}

pub fn describe_fault(fault: &Fault) -> String {
    match fault {
        Fault::Partition { a, b } => format!("partition {a:?} | {b:?}"),
        Fault::AsymmetricCut { from, to } => format!("cut {from} -> {to} (one way)"),
        Fault::Isolate { node } => format!("isolate {node}"),
        Fault::Heal => "heal".to_string(),
        Fault::Crash { node } => format!("crash {node}"),
        Fault::Restart { node } => format!("restart {node}"),
        Fault::Pause { node, ticks } => format!("pause {node} for {ticks}"),
    }
}

/// Write the full trace and a repro description for a failing seed.
pub fn write_artifacts(
    dir: &Path,
    result: &SeedResult,
    minimized: Option<&Minimized>,
    trace_json: &str,
) -> std::io::Result<Vec<PathBuf>> {
    std::fs::create_dir_all(dir)?;
    let mut written = Vec::new();

    let trace_path = dir.join(format!("seed-{}.trace.json", result.seed));
    std::fs::write(&trace_path, trace_json)?;
    written.push(trace_path);

    let mut repro = String::new();
    let _ = writeln!(repro, "# repro for seed {}", result.seed);
    let _ = write!(repro, "{}", failure(result));
    if let Some(m) = minimized {
        let _ = write!(repro, "{}", self::minimized(m));
    }
    let _ = writeln!(
        repro,
        "\nreplay with: cargo run --release -p fuzz -- --start {} --seeds 1 --trace-dir <dir>",
        result.seed
    );
    let repro_path = dir.join(format!("seed-{}.repro.txt", result.seed));
    std::fs::write(&repro_path, repro)?;
    written.push(repro_path);

    Ok(written)
}
