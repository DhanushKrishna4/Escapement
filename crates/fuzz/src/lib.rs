//! The fuzzing harness.
//!
//! Runs seeds, checks every safety invariant, and when one breaks, shrinks the
//! failure to the smallest fault schedule that still reproduces it. A four-event
//! repro is worth a hundred times more than a four-thousand-event one.
//!
//! Parallelism is one thread per core with seeds pulled off a shared counter.
//! That is safe precisely because a seed's run is a pure function of its
//! config: nothing is shared, so nothing can make two runs of a seed differ.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use kvstore::KvCommand;
use raft::{BugSwitches, RaftConfig};
use sim::history::History;
use sim::invariants::{Invariant, Violation};
use sim::linearizability::{self, Verdict};
use sim::{
    ClientDriver, Cluster, DiskConfig, Fault, FaultConfig, NetworkConfig, NetworkStats, RunStats,
    SimConfig, WorkloadConfig,
};

pub mod report;

/// How the sweep is run. Not part of a seed's identity — every field here that
/// affects behaviour is folded into the per-seed config.
#[derive(Clone, Debug)]
pub struct Options {
    /// Virtual ticks per run.
    pub ticks: u64,
    /// Deliberate bugs to enable, for validating that the harness can actually
    /// find something. Off means fuzzing the real implementation.
    pub bugs: BugSwitches,
    /// Record the full trace. Off by default: a long run produces hundreds of
    /// megabytes, and a failing seed can simply be re-run with it on.
    pub record_trace: bool,
    /// Give up shrinking after this many candidate re-runs.
    pub minimize_budget: usize,
    /// Enable the simulator-level stale-read bug, to check that the
    /// linearizability checker catches what the invariants cannot.
    pub stale_reads: bool,
    /// Search steps allowed for the linearizability check on one seed.
    ///
    /// Bounded so one pathological history cannot stall the whole sweep. When
    /// it runs out the answer is `Unknown`, never a cheerful "linearizable".
    pub linearizability_budget: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            ticks: 30_000,
            bugs: BugSwitches::default(),
            record_trace: false,
            minimize_budget: 2_000,
            stale_reads: false,
            linearizability_budget: 200_000,
        }
    }
}

/// Everything about a seed's run that matters afterwards.
#[derive(Clone, Debug)]
pub struct SeedResult {
    pub seed: u64,
    pub nodes: usize,
    pub violations: Vec<Violation>,
    pub stats: RunStats,
    pub network: NetworkStats,
    /// The faults that actually fired, in order. The starting point for
    /// shrinking.
    pub faults: Vec<(u64, Fault)>,
    /// Whether clients were told a coherent story.
    pub linearizability: Verdict,
    pub history_completed: usize,
    pub history_pending: usize,
}

impl SeedResult {
    /// A seed fails if any Raft invariant broke *or* the client-visible history
    /// turned out to be impossible. The two catch different things: the
    /// invariants check what the algorithm did internally, linearizability
    /// checks the promise made to clients.
    pub fn failed(&self) -> bool {
        !self.violations.is_empty() || self.linearizability.is_violation()
    }

    /// The earliest violation, which is usually the cause rather than a
    /// downstream symptom.
    pub fn first_violation(&self) -> Option<&Violation> {
        self.violations.iter().min_by_key(|v| v.tick)
    }
}

/// Build a seed's configuration.
///
/// Every dimension the spec asks to randomize — cluster size, network, fault
/// schedule, disk, clock skew, workload — is derived here from the seed alone,
/// so the seed really is a complete description of the run.
pub fn config_for_seed(seed: u64, opts: &Options) -> (SimConfig, WorkloadConfig) {
    let mut rng = raft::Rng::derive(seed, 0x0046_555a_5a00);

    let nodes = match rng.gen_range(0, 10) {
        0..=4 => 3,
        5..=8 => 5,
        _ => 7,
    };

    let network = match rng.gen_range(0, 10) {
        0..=1 => NetworkConfig::perfect(),
        2..=4 => NetworkConfig::flaky(),
        5..=7 => NetworkConfig::long_tail(),
        _ => NetworkConfig::hostile(),
    };

    // Narrow schedules as well as broad ones. A schedule that mixes five fault
    // kinds spends its weight budget thinly; `crash_only` and `asymmetric_only`
    // drive their own failure modes much harder.
    let faults = match rng.gen_range(0, 10) {
        0 => FaultConfig::none(),
        1..=3 => FaultConfig::occasional(),
        4..=6 => FaultConfig::aggressive(),
        7..=8 => FaultConfig::crash_only(),
        _ => FaultConfig::asymmetric_only(),
    };

    let disk = if rng.chance(3, 10) {
        DiskConfig::flaky()
    } else {
        DiskConfig::reliable()
    };

    // Some seeds start with only a subset of the nodes as voters, so a
    // membership change has somewhere to grow into.
    let initial_voters = if nodes >= 5 && rng.chance(3, 10) {
        Some((0..3u32).collect::<Vec<_>>())
    } else {
        None
    };

    // Compaction is part of the search space, and so is not compacting: an
    // uncompacted run is the control that makes any snapshot-specific failure
    // attributable.
    let snapshot_every = match rng.gen_range(0, 10) {
        0..=3 => None,
        4..=6 => Some(rng.gen_range(8, 25)),
        _ => Some(rng.gen_range(25, 80)),
    };

    let clock_skew_permille = if rng.chance(1, 2) {
        rng.gen_range(0, 250) as u32
    } else {
        0
    };

    // Batch size is load-bearing, and pinning it hides whole classes of bug.
    //
    // With a large batch a leader's backfill of an old entry and its own
    // election no-op always travel in the same message, so they commit
    // together and a §5.4.2 violation can never appear. With a batch of one
    // they are separable. The same parameter decides whether a follower can
    // fall far enough behind for an uncapped `leaderCommit` to run past the end
    // of its log. Both bugs were invisible to this harness until batch size
    // became part of the search space.
    let max_entries_per_append = match rng.gen_range(0, 10) {
        0..=2 => 1,
        3..=5 => rng.gen_range(2, 8) as usize,
        _ => 64,
    };

    let workload = match rng.gen_range(0, 10) {
        0..=1 => WorkloadConfig::writes_only(),
        2..=4 => WorkloadConfig::contended(),
        _ => WorkloadConfig::default(),
    };

    let cfg = SimConfig {
        seed,
        nodes,
        raft: RaftConfig {
            bugs: opts.bugs.clone(),
            max_entries_per_append,
            ..RaftConfig::default()
        },
        network,
        faults,
        disk,
        clock_skew_permille,
        record_trace: opts.record_trace,
        scripted_faults: None,
        initial_voters,
        snapshot_every,
        stale_reads: opts.stale_reads,
        check_invariants: true,
    };
    (cfg, workload)
}

/// A membership change schedule for a seed: when, and to what.
///
/// Derived from the seed like everything else, and empty for most seeds so that
/// runs without membership churn remain the control.
pub fn membership_plan(seed: u64, nodes: usize, ticks: u64) -> Vec<(u64, Vec<raft::NodeId>)> {
    let mut rng = raft::Rng::derive(seed, 0x004d_454d_4245_5200);
    if nodes < 5 || !rng.chance(35, 100) {
        return Vec::new();
    }
    let changes = rng.gen_range(1, 4);
    let mut plan = Vec::new();
    for i in 0..changes {
        // Spread the changes over the run, leaving room to settle.
        let at = ticks / (changes + 1) * (i + 1);

        // An odd-sized subset, so there is always a clear majority.
        let size = match rng.gen_range(0, 3) {
            0 => 3,
            1 => 5.min(nodes),
            _ => (nodes | 1).min(nodes),
        };
        let mut pool: Vec<raft::NodeId> = (0..nodes as raft::NodeId).collect();
        // Fisher-Yates with the seeded PRNG: deterministic, unlike anything
        // that would reach for a hasher.
        for j in (1..pool.len()).rev() {
            let k = rng.gen_range(0, j as u64 + 1) as usize;
            pool.swap(j, k);
        }
        pool.truncate(size.max(3));
        pool.sort_unstable();
        plan.push((at, pool));
    }
    plan
}

/// Run one configuration to completion.
pub fn run_config(cfg: SimConfig, workload: WorkloadConfig, ticks: u64) -> (Cluster, SeedResult) {
    run_config_with(
        cfg,
        workload,
        ticks,
        Options::default().linearizability_budget,
    )
}

pub fn run_config_with(
    cfg: SimConfig,
    workload: WorkloadConfig,
    ticks: u64,
    linearizability_budget: u64,
) -> (Cluster, SeedResult) {
    let seed = cfg.seed;
    let nodes = cfg.nodes;
    let mut sim = Cluster::new(cfg);
    let mut plan = membership_plan(seed, nodes, ticks).into_iter().peekable();

    // Retry behaviour is part of the search space: some clients give up after
    // one attempt, others resend until they are heard. Retries are the only
    // thing that exercises §8's session deduplication, and a client that never
    // retries is the control.
    let mut retry_rng = raft::Rng::derive(seed, 0x5245_5452_5900);
    let retry_after = retry_rng.gen_range(300, 1_200);
    let max_attempts = if retry_rng.chance(7, 10) {
        retry_rng.gen_range(2, 6) as u32
    } else {
        1
    };
    let mut driver = ClientDriver::new(seed, workload, retry_after, max_attempts);

    // Give the cluster a chance to elect someone before piling work on.
    sim.run_until_leader(5_000);

    while sim.now() < ticks {
        // Membership changes land partway through, on top of whatever faults
        // the schedule is already causing.
        if let Some((at, _)) = plan.peek() {
            if sim.now() >= *at {
                let (_, voters) = plan.next().expect("just peeked");
                sim.change_membership(voters);
            }
        }
        let gap = driver.step(&mut sim);
        sim.run_for(gap);
    }
    // Let the dust settle so late violations still surface, and so a
    // membership change late in the run has time to finish.
    sim.run_for(10_000);

    let linearizability = linearizability::check_with_budget(sim.history(), linearizability_budget);

    let result = SeedResult {
        seed,
        nodes,
        violations: sim.violations().to_vec(),
        stats: sim.stats().clone(),
        network: sim.network_stats().clone(),
        faults: sim.faults_injected().to_vec(),
        linearizability,
        history_completed: sim.history().completed(),
        history_pending: sim.history().pending(),
    };
    (sim, result)
}

pub fn run_seed(seed: u64, opts: &Options) -> SeedResult {
    let (cfg, workload) = config_for_seed(seed, opts);
    run_config_with(cfg, workload, opts.ticks, opts.linearizability_budget).1
}

/// Re-run a seed with tracing on. Free, because the run is reproducible.
pub fn trace_seed(seed: u64, opts: &Options) -> Cluster {
    let (mut cfg, workload) = config_for_seed(seed, opts);
    cfg.record_trace = true;
    run_config(cfg, workload, opts.ticks).0
}

// ---------------------------------------------------------------------------
// Minimization
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Minimized {
    pub seed: u64,
    /// What the shrink is preserving.
    pub target: Target,
    pub original_faults: usize,
    pub original_ticks: u64,
    pub faults: Vec<(u64, Fault)>,
    pub ticks: u64,
    /// How many candidate runs it took.
    pub attempts: usize,
    /// False if the failure could not be reproduced from the captured script at
    /// all, in which case the repro is the seed itself and nothing more.
    pub reproduced: bool,
}

impl Minimized {
    pub fn shrank_by(&self) -> usize {
        self.original_faults.saturating_sub(self.faults.len())
    }
}

/// What kind of failure a shrink is trying to preserve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// A specific Raft invariant broke.
    Invariant(Invariant),
    /// The client-visible history was impossible.
    Linearizability,
}

impl Target {
    pub fn name(self) -> String {
        match self {
            Target::Invariant(i) => format!("{} ({})", i.name(), i.paper_ref()),
            Target::Linearizability => "Linearizability".to_string(),
        }
    }
}

/// Does this configuration still fail in the same way?
fn still_fails(cfg: &SimConfig, workload: &WorkloadConfig, ticks: u64, target: Target) -> bool {
    let (_, result) = run_config(cfg.clone(), workload.clone(), ticks);
    match target {
        Target::Invariant(i) => result.violations.iter().any(|v| v.invariant == i),
        Target::Linearizability => result.linearizability.is_violation(),
    }
}

/// Shrink a failure to the smallest fault schedule that still reproduces it.
///
/// The strategy is greedy delta debugging over the fault list, plus a pass at
/// shortening the run. Removing a fault changes every random draw that follows
/// it, so a candidate may stop failing for reasons unrelated to the fault that
/// was dropped — which is fine: a removal is only kept when the failure
/// survives, so the result is always a genuine reproduction.
pub fn minimize(seed: u64, opts: &Options) -> Option<Minimized> {
    let original = run_seed(seed, opts);
    // Prefer an invariant failure: it names a specific rule and usually points
    // at the cause, where a linearizability failure is the symptom.
    let target = match original.first_violation() {
        Some(v) => Target::Invariant(v.invariant),
        None if original.linearizability.is_violation() => Target::Linearizability,
        None => return None,
    };
    let (base_cfg, workload) = config_for_seed(seed, opts);

    let mut attempts = 0usize;
    let mut script = original.faults.clone();
    let mut ticks = opts.ticks;

    // First: can the failure be reproduced from a fixed script at all? If the
    // generated schedule and the scripted replay disagree, shrinking would be
    // chasing a different run.
    let scripted = |script: &[(u64, Fault)], ticks: u64| {
        let mut cfg = base_cfg.clone();
        cfg.scripted_faults = Some(script.to_vec());
        cfg.record_trace = false;
        (cfg, ticks)
    };

    let (cfg, t) = scripted(&script, ticks);
    attempts += 1;
    let reproduced = still_fails(&cfg, &workload, t, target);
    if !reproduced {
        return Some(Minimized {
            seed,
            target,
            original_faults: original.faults.len(),
            original_ticks: opts.ticks,
            faults: original.faults,
            ticks,
            attempts,
            reproduced: false,
        });
    }

    // Shrink the run first: a shorter run makes every later candidate cheaper,
    // and the whole point of a repro is not to spend thousands of ticks after
    // the interesting moment.
    if let Some(v) = original.first_violation() {
        for slack in [500u64, 2_000, 5_000] {
            let candidate = v.tick + slack;
            if candidate >= ticks {
                continue;
            }
            let (cfg, _) = scripted(&script, candidate);
            attempts += 1;
            if still_fails(&cfg, &workload, candidate, target) {
                ticks = candidate;
                break;
            }
        }
    }

    // Delta debugging over the fault list: try removing large chunks first and
    // fall back to single faults. Chunks matter — a partition and its matching
    // heal usually have to go together or the cluster is left permanently
    // split, so removing them one at a time makes no progress.
    let mut chunk = (script.len() / 2).max(1);
    loop {
        let mut removed_anything = false;
        let mut i = 0;
        while i < script.len() && attempts < opts.minimize_budget {
            let end = (i + chunk).min(script.len());
            let mut candidate = script.clone();
            candidate.drain(i..end);
            let (cfg, t) = scripted(&candidate, ticks);
            attempts += 1;
            if still_fails(&cfg, &workload, t, target) {
                script = candidate;
                removed_anything = true;
                // Do not advance: the next chunk has slid into this position.
            } else {
                i = end;
            }
        }
        if attempts >= opts.minimize_budget {
            break;
        }
        if chunk == 1 {
            // A full single-fault pass that removed nothing means a local
            // minimum: every remaining fault is load-bearing.
            if !removed_anything {
                break;
            }
        } else {
            chunk /= 2;
        }
    }

    Some(Minimized {
        seed,
        target,
        original_faults: original.faults.len(),
        original_ticks: opts.ticks,
        faults: script,
        ticks,
        attempts,
        reproduced: true,
    })
}

// ---------------------------------------------------------------------------
// Parallel sweep
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct Coverage {
    pub seeds: u64,
    pub stats: RunStats,
    pub network: NetworkStats,
    pub schedules_with_faults: u64,
    pub operations_completed: u64,
    pub operations_pending: u64,
    pub histories_checked: u64,
    pub histories_undecided: u64,
}

impl Coverage {
    fn absorb(&mut self, r: &SeedResult) {
        self.seeds += 1;
        let s = &r.stats;
        self.stats.elections_started += s.elections_started;
        self.stats.leaders_elected += s.leaders_elected;
        self.stats.max_term = self.stats.max_term.max(s.max_term);
        self.stats.log_truncations += s.log_truncations;
        self.stats.entries_truncated += s.entries_truncated;
        self.stats.entries_applied += s.entries_applied;
        self.stats.client_responses += s.client_responses;
        self.stats.faults_injected += s.faults_injected;
        self.stats.crashes += s.crashes;
        self.stats.torn_steps += s.torn_steps;
        self.stats.restarts += s.restarts;
        self.stats.pauses += s.pauses;
        self.stats.messages_deferred += s.messages_deferred;
        self.stats.snapshots_taken += s.snapshots_taken;
        self.stats.snapshots_installed += s.snapshots_installed;
        self.stats.membership_changes += s.membership_changes;
        self.stats.reads_served += s.reads_served;
        let n = &r.network;
        self.network.sent += n.sent;
        self.network.delivered += n.delivered;
        self.network.dropped += n.dropped;
        self.network.partitioned += n.partitioned;
        self.network.duplicated += n.duplicated;
        self.network.reordered += n.reordered;
        self.network.max_delay = self.network.max_delay.max(n.max_delay);
        if s.faults_injected > 0 {
            self.schedules_with_faults += 1;
        }
        self.operations_completed += r.history_completed as u64;
        self.operations_pending += r.history_pending as u64;
        match r.linearizability {
            Verdict::Linearizable { .. } => self.histories_checked += 1,
            Verdict::Unknown { .. } => self.histories_undecided += 1,
            Verdict::NotLinearizable(_) => {}
        }
    }
}

pub struct Sweep {
    pub coverage: Coverage,
    pub failures: Vec<SeedResult>,
    pub elapsed_secs: f64,
}

/// Run `count` seeds starting at `start`, across `threads` OS threads.
pub fn sweep(start: u64, count: u64, threads: usize, opts: &Options) -> Sweep {
    let began = std::time::Instant::now();
    let next = Arc::new(AtomicU64::new(start));
    let end = start + count;
    let (tx, rx) = mpsc::channel::<SeedResult>();
    let opts = Arc::new(opts.clone());

    let threads = threads.max(1);
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let next = Arc::clone(&next);
        let opts = Arc::clone(&opts);
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || loop {
            let seed = next.fetch_add(1, Ordering::Relaxed);
            if seed >= end {
                break;
            }
            let result = run_seed(seed, &opts);
            if tx.send(result).is_err() {
                break;
            }
        }));
    }
    drop(tx);

    let mut coverage = Coverage::default();
    let mut failures = Vec::new();
    for result in rx {
        coverage.absorb(&result);
        if result.failed() {
            failures.push(result);
        }
    }
    for h in handles {
        let _ = h.join();
    }

    // Seeds finish out of order across threads; sort so a report is stable.
    failures.sort_by_key(|f| f.seed);
    Sweep {
        coverage,
        failures,
        elapsed_secs: began.elapsed().as_secs_f64(),
    }
}

/// Shared collector, used by the binary to stream progress.
pub type Progress = Arc<Mutex<Vec<String>>>;

/// A human-readable one-liner describing what a seed did.
pub fn describe(seed: u64, opts: &Options) -> String {
    let (cfg, workload) = config_for_seed(seed, opts);
    format!(
        "seed {seed}: {} nodes, batch {}, snapshot {}, drop {}permille, {} faults, \
         disk {}permille, skew {}permille, {} clients on {} keys",
        cfg.nodes,
        cfg.raft.max_entries_per_append,
        match cfg.snapshot_every {
            Some(n) => format!("every {n}"),
            None => "never".to_string(),
        },
        cfg.network.drop_permille,
        if cfg.faults.enabled {
            "scheduled"
        } else {
            "no"
        },
        cfg.disk.torn_step_permille,
        cfg.clock_skew_permille,
        workload.clients,
        workload.keys,
    )
}

/// Sanity check used by the binary and the tests: the workload must actually
/// commit things, or a clean sweep proves nothing.
/// The recorded history for a seed, for inspection.
pub fn history_for(seed: u64, opts: &Options) -> History {
    let (cfg, workload) = config_for_seed(seed, opts);
    let (sim, _) = run_config_with(cfg, workload, opts.ticks, opts.linearizability_budget);
    sim.history().clone()
}

pub fn committed_anything(seed: u64, opts: &Options) -> bool {
    let (cfg, workload) = config_for_seed(seed, opts);
    let (sim, _) = run_config(cfg, workload, opts.ticks);
    sim.node_ids()
        .into_iter()
        .any(|id| !sim.machine(id).is_empty())
}

/// Re-export so callers can build commands without depending on kvstore.
pub fn put(key: &str, value: &str) -> KvCommand {
    KvCommand::Put {
        key: key.into(),
        value: value.into(),
    }
}
