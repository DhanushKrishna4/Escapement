//! The wasm boundary.
//!
//! Deliberately thin. Everything interesting lives in `sim`, which knows
//! nothing about the browser and is tested natively; this file only marshals.
//!
//! # Why JSON strings rather than `JsValue`
//!
//! Structured values would need `serde-wasm-bindgen`, which is outside the
//! dependency budget this project set itself. Returning a JSON string and
//! letting JavaScript `JSON.parse` it costs one serialization per frame — a few
//! kilobytes at 60fps — and keeps the boundary to `wasm-bindgen` alone. If
//! rendering ever becomes the bottleneck, the fix is to send only what changed
//! rather than to add a dependency.

use std::collections::BTreeSet;

use kvstore::KvCommand;
use raft::{NodeId, Role};
use serde::{Deserialize, Serialize};
use sim::linearizability::Verdict;
use sim::{
    ClientDriver, Cluster, DiskConfig, Event, Fault, FaultConfig, NetworkConfig, NodeStatus,
    SimConfig, WorkloadConfig,
};
use wasm_bindgen::prelude::*;

/// What the UI sends to build a run. Friendlier than the full `SimConfig`:
/// presets by name, rather than every knob.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct UiConfig {
    pub nodes: usize,
    /// "perfect" | "flaky" | "long_tail" | "hostile"
    pub network: String,
    /// "none" | "occasional" | "aggressive" | "asymmetric_only" | "crash_only"
    pub faults: String,
    pub snapshot_every: Option<u64>,
    pub clock_skew_permille: u32,
    pub torn_step_permille: u32,
    /// Skip the ReadIndex round. A deliberate bug, exposed so the UI can show
    /// the linearizability checker catching something the invariants cannot.
    pub stale_reads: bool,
    /// Entries per AppendEntries. Load-bearing for several failures: with a
    /// large batch a leader's backfill and its own-term entry always travel
    /// together, which hides a whole class of bug.
    pub max_entries_per_append: usize,
    /// Deliberate Raft bugs, by name, for the same reason.
    pub bugs: Vec<String>,
    pub initial_voters: Option<Vec<NodeId>>,
}

impl Default for UiConfig {
    fn default() -> Self {
        UiConfig {
            nodes: 5,
            network: "perfect".into(),
            faults: "none".into(),
            snapshot_every: None,
            clock_skew_permille: 0,
            torn_step_permille: 0,
            max_entries_per_append: 64,
            stale_reads: false,
            bugs: Vec::new(),
            initial_voters: None,
        }
    }
}

impl UiConfig {
    fn into_sim(self, seed: u64) -> SimConfig {
        let network = match self.network.as_str() {
            "flaky" => NetworkConfig::flaky(),
            "long_tail" => NetworkConfig::long_tail(),
            "hostile" => NetworkConfig::hostile(),
            _ => NetworkConfig::perfect(),
        };
        let faults = match self.faults.as_str() {
            "occasional" => FaultConfig::occasional(),
            "aggressive" => FaultConfig::aggressive(),
            "asymmetric_only" => FaultConfig::asymmetric_only(),
            "crash_only" => FaultConfig::crash_only(),
            _ => FaultConfig::none(),
        };
        let mut bugs = raft::BugSwitches::default();
        for name in &self.bugs {
            match name.as_str() {
                "commit-rule" => bugs.commit_prior_term_entries = true,
                "double-vote" => bugs.vote_twice_per_term = true,
                "blind-commit" => bugs.trust_leader_commit_blindly = true,
                "no-persist" => bugs.skip_hard_state_persistence = true,
                // Regressions that were genuinely in this implementation.
                "compaction-anchor" => bugs.compaction_anchor_at_zero = true,
                "no-reconcile" => bugs.skip_snapshot_reconcile = true,
                "early-read-index" => bugs.read_index_at_arrival = true,
                _ => {}
            }
        }
        SimConfig {
            seed,
            nodes: self.nodes.clamp(1, 9),
            raft: raft::RaftConfig {
                bugs,
                max_entries_per_append: self.max_entries_per_append.clamp(1, 256),
                ..raft::RaftConfig::default()
            },
            network,
            faults,
            disk: DiskConfig {
                torn_step_permille: self.torn_step_permille,
                ..DiskConfig::reliable()
            },
            clock_skew_permille: self.clock_skew_permille,
            initial_voters: self.initial_voters,
            snapshot_every: self.snapshot_every,
            stale_reads: self.stale_reads,
            record_trace: true,
            scripted_faults: None,
            check_invariants: true,
        }
    }
}

// ---------------------------------------------------------------------------
// What the renderer reads
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EntryView {
    index: u64,
    term: u64,
    /// "noop" | "cmd" | "config"
    kind: &'static str,
    committed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeView {
    id: NodeId,
    role: &'static str,
    status: &'static str,
    term: u64,
    commit_index: u64,
    last_applied: u64,
    last_index: u64,
    log_start: u64,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    snapshot_index: u64,
    config: String,
    is_joint: bool,
    pending_reads: usize,
    /// A window of the tail of the log, for the log strip.
    log: Vec<EntryView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageView {
    from: NodeId,
    to: NodeId,
    kind: &'static str,
    sent_at: u64,
    arrives_at: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StateView {
    tick: u64,
    events_processed: u64,
    nodes: Vec<NodeView>,
    /// Messages currently on the wire, with the ticks needed to place them.
    in_flight: Vec<MessageView>,
    /// Directed links that are down, as `[from, to]` pairs.
    blocked_links: Vec<[NodeId; 2]>,
    leaders: Vec<NodeId>,
    stats: sim::RunStats,
    violations: Vec<String>,
    kv: Vec<(String, String)>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckView {
    linearizable: bool,
    /// "linearizable" | "violation" | "unknown"
    verdict: &'static str,
    detail: String,
    operations: usize,
    completed: usize,
    pending: usize,
    invariants_broken: Vec<String>,
    violations: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimelineView {
    tick: u64,
    severity: &'static str,
    label: String,
}

/// A fault the UI can inject.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum UiFault {
    Partition { a: Vec<NodeId>, b: Vec<NodeId> },
    Cut { from: NodeId, to: NodeId },
    Isolate { node: NodeId },
    Heal,
    Crash { node: NodeId },
    Restart { node: NodeId },
    Pause { node: NodeId, ticks: u64 },
    ChangeMembership { voters: Vec<NodeId> },
}

// ---------------------------------------------------------------------------
// The exported handle
// ---------------------------------------------------------------------------

/// Something the person watching did, recorded so it can be replayed.
///
/// Time travel works by re-running from the seed, which only reproduces what
/// you were looking at if the things *you* did are replayed too. The generated
/// fault schedule comes back on its own — it is a pure function of the seed —
/// but a partition somebody dragged out at tick 4,000 has to be remembered.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum UiAction {
    Fault(UiFault),
    Write {
        key: String,
        value: String,
        client: u32,
        seq: u64,
    },
    Read {
        key: String,
        client: u32,
        seq: u64,
    },
}

#[wasm_bindgen]
pub struct Sim {
    cluster: Cluster,
    /// The configuration this run was built from, kept so it can be rebuilt.
    config: SimConfig,
    workload_on: bool,
    driver: Option<ClientDriver>,
    /// Ticks until the built-in workload issues its next request.
    next_request_in: u64,
    /// Everything the user did, with the tick it happened at.
    actions: Vec<(u64, UiAction)>,
    seed: u64,
}

#[wasm_bindgen]
impl Sim {
    /// Build a run. `config_json` is a [`UiConfig`]; an empty string means
    /// defaults.
    #[wasm_bindgen(constructor)]
    pub fn new(seed: u64, config_json: &str) -> Result<Sim, JsValue> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        let ui: UiConfig = if config_json.trim().is_empty() {
            UiConfig::default()
        } else {
            serde_json::from_str(config_json).map_err(|e| JsValue::from_str(&e.to_string()))?
        };
        let config = ui.into_sim(seed);
        Ok(Sim {
            cluster: Cluster::new(config.clone()),
            config,
            workload_on: false,
            driver: None,
            next_request_in: 0,
            actions: Vec::new(),
            seed,
        })
    }

    /// Turn the built-in client workload on or off.
    ///
    /// Off by default: an empty cluster electing a leader is the clearest thing
    /// to look at first, and traffic can be added once that makes sense.
    #[wasm_bindgen(js_name = setWorkload)]
    pub fn set_workload(&mut self, enabled: bool) {
        self.workload_on = enabled;
        self.driver = enabled.then(|| Self::new_driver(self.seed));
    }

    /// Advance the virtual clock by `ticks` and return the new state.
    ///
    /// Called from a rAF loop on the JS side, so the tab stays responsive: the
    /// simulator does a bounded amount of work per frame rather than running to
    /// completion.
    pub fn step(&mut self, ticks: u32) -> String {
        self.advance(ticks as u64);
        self.snapshot_state()
    }

    /// Jump to a tick.
    ///
    /// Backwards means rebuilding from the seed and replaying — which is cheap,
    /// because the simulator does millions of events a second, and *exact*,
    /// because a run is a pure function of its configuration plus the things
    /// the user did. This is the payoff for having refused wall-clock time and
    /// ambient randomness everywhere else.
    pub fn seek(&mut self, tick: u64) -> String {
        if tick >= self.cluster.now() {
            self.advance(tick - self.cluster.now());
        } else {
            self.replay_to(tick);
        }
        self.snapshot_state()
    }

    /// Notable moments, for the timeline.
    pub fn timeline(&self) -> String {
        let entries: Vec<TimelineView> = self
            .cluster
            .timeline()
            .entries()
            .map(|e| TimelineView {
                tick: e.tick,
                severity: e.moment.severity(),
                label: describe_moment(&e.moment),
            })
            .collect();
        serde_json::to_string(&entries).unwrap_or_default()
    }

    /// Process exactly one event, for single-stepping.
    #[wasm_bindgen(js_name = stepOnce)]
    pub fn step_once(&mut self) -> String {
        self.cluster.step_once();
        self.snapshot_state()
    }

    /// Everything the renderer needs, as JSON.
    #[wasm_bindgen(js_name = snapshotState)]
    pub fn snapshot_state(&self) -> String {
        let view = self.build_state();
        serde_json::to_string(&view).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// Inject a fault from the UI. See [`UiFault`] for the shape.
    pub fn inject(&mut self, fault_json: &str) -> Result<(), JsValue> {
        let fault: UiFault =
            serde_json::from_str(fault_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let at = self.cluster.now();
        self.actions.push((at, UiAction::Fault(fault.clone())));
        self.apply_fault(fault);
        Ok(())
    }

    #[wasm_bindgen(js_name = clientRequest)]
    pub fn client_request(&mut self, key: &str, value: &str) {
        let (client, seq) = self.next_manual_request();
        let at = self.cluster.now();
        self.actions.push((
            at,
            UiAction::Write {
                key: key.to_string(),
                value: value.to_string(),
                client,
                seq,
            },
        ));
        self.submit_write(key, value, client, seq);
    }

    /// Submit a read from the UI. Goes through ReadIndex.
    #[wasm_bindgen(js_name = clientRead)]
    pub fn client_read(&mut self, key: &str) {
        let (client, seq) = self.next_manual_request();
        let at = self.cluster.now();
        self.actions.push((
            at,
            UiAction::Read {
                key: key.to_string(),
                client,
                seq,
            },
        ));
        self.submit_read(key, client, seq);
    }
}

impl Sim {
    fn apply_fault(&mut self, fault: UiFault) {
        match fault {
            UiFault::Partition { a, b } => self.cluster.partition(&a, &b),
            UiFault::Cut { from, to } => self.cluster.cut(from, to),
            UiFault::Isolate { node } => self.cluster.isolate(node),
            UiFault::Heal => self.cluster.heal(),
            UiFault::Crash { node } => self.cluster.crash(node),
            UiFault::Restart { node } => self.cluster.restart(node),
            UiFault::Pause { node, ticks } => self.cluster.pause(node, ticks),
            UiFault::ChangeMembership { voters } => {
                self.cluster.change_membership(voters);
            }
        }
    }

    fn new_driver(seed: u64) -> ClientDriver {
        ClientDriver::new(seed, WorkloadConfig::default(), 600, 4)
    }

    /// Manual requests use their own client id so they never collide with the
    /// built-in workload's sequence numbers, which §8's session table keys on.
    fn next_manual_request(&mut self) -> (u32, u64) {
        let seq = self
            .actions
            .iter()
            .filter(|(_, a)| matches!(a, UiAction::Write { .. } | UiAction::Read { .. }))
            .count() as u64;
        (99, seq)
    }

    fn submit_write(&mut self, key: &str, value: &str, client: u32, seq: u64) {
        let target = self.cluster.leader().unwrap_or(0);
        self.cluster.submit(
            target,
            client,
            seq,
            KvCommand::Put {
                key: key.to_string(),
                value: value.to_string(),
            },
        );
    }

    fn submit_read(&mut self, key: &str, client: u32, seq: u64) {
        let target = self.cluster.leader().unwrap_or(0);
        self.cluster.submit(
            target,
            client,
            seq,
            KvCommand::Get {
                key: key.to_string(),
            },
        );
    }

    fn apply_action(&mut self, action: UiAction) {
        match action {
            UiAction::Fault(f) => self.apply_fault(f),
            UiAction::Write {
                key,
                value,
                client,
                seq,
            } => self.submit_write(&key, &value, client, seq),
            UiAction::Read { key, client, seq } => self.submit_read(&key, client, seq),
        }
    }

    /// Advance the clock, feeding the built-in workload as it goes.
    fn advance(&mut self, ticks: u64) {
        if self.driver.is_none() {
            self.cluster.run_for(ticks);
            return;
        }
        let mut remaining = ticks;
        while remaining > 0 {
            if self.next_request_in == 0 {
                let driver = self.driver.as_mut().expect("checked above");
                self.next_request_in = driver.step(&mut self.cluster);
            }
            let chunk = remaining.min(self.next_request_in.max(1));
            self.cluster.run_for(chunk);
            self.next_request_in = self.next_request_in.saturating_sub(chunk);
            remaining -= chunk;
        }
    }

    /// Rebuild from the seed and re-run to `target`, replaying user actions at
    /// the ticks they originally happened.
    fn replay_to(&mut self, target: u64) {
        self.cluster = Cluster::new(self.config.clone());
        self.driver = self.workload_on.then(|| Self::new_driver(self.seed));
        self.next_request_in = 0;

        let actions = self.actions.clone();
        let mut next = 0usize;
        loop {
            while next < actions.len() && actions[next].0 <= self.cluster.now() {
                self.apply_action(actions[next].1.clone());
                next += 1;
            }
            if self.cluster.now() >= target {
                break;
            }
            let stop = actions
                .get(next)
                .map(|(t, _)| *t)
                .unwrap_or(target)
                .min(target);
            let delta = stop.saturating_sub(self.cluster.now());
            self.advance(delta.max(1));
        }
    }
}

#[wasm_bindgen]
impl Sim {
    /// The recorded client history, as JSON.
    pub fn history(&self) -> String {
        serde_json::to_string(self.cluster.history().operations())
            .unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// Run the linearizability check and report the invariant status.
    pub fn check(&self) -> String {
        let verdict = self.cluster.check_linearizability();
        let history = self.cluster.history();
        let (kind, detail) = match &verdict {
            Verdict::Linearizable { keys, operations } => (
                "linearizable",
                format!("{operations} operations across {keys} keys"),
            ),
            Verdict::NotLinearizable(e) => ("violation", e.to_string()),
            Verdict::Unknown { reason } => ("unknown", reason.clone()),
        };
        let view = CheckView {
            linearizable: verdict.is_linearizable(),
            verdict: kind,
            detail,
            operations: history.len(),
            completed: history.completed(),
            pending: history.pending(),
            invariants_broken: self
                .cluster
                .broken_invariants()
                .iter()
                .map(|i| format!("{} ({})", i.name(), i.paper_ref()))
                .collect(),
            violations: self
                .cluster
                .violations()
                .iter()
                .map(|v| v.to_string())
                .collect(),
        };
        serde_json::to_string(&view).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// Faults applied so far, for the timeline.
    pub fn faults(&self) -> String {
        let items: Vec<(u64, String)> = self
            .cluster
            .faults_injected()
            .iter()
            .map(|(at, f)| (*at, describe_fault(f)))
            .collect();
        serde_json::to_string(&items).unwrap_or_default()
    }

    #[wasm_bindgen(js_name = currentTick)]
    pub fn current_tick(&self) -> u64 {
        self.cluster.now()
    }
}

impl Sim {
    fn build_state(&self) -> StateView {
        /// How much of each log's tail to send. The whole log would grow
        /// without bound and the strip only shows so much anyway.
        const LOG_WINDOW: u64 = 48;

        let nodes: Vec<NodeView> = self
            .cluster
            .node_ids()
            .into_iter()
            .map(|id| {
                let node = self.cluster.node(id);
                let log = node.log();
                let from = log
                    .last_index()
                    .saturating_sub(LOG_WINDOW)
                    .max(log.first_index());
                let entries = (from..=log.last_index())
                    .filter_map(|i| {
                        log.get(i).map(|e| EntryView {
                            index: e.index,
                            term: e.term,
                            kind: match &e.payload {
                                raft::EntryPayload::Noop => "noop",
                                raft::EntryPayload::Command(_) => "cmd",
                                raft::EntryPayload::Config(_) => "config",
                            },
                            committed: e.index <= node.commit_index(),
                        })
                    })
                    .collect();
                NodeView {
                    id,
                    role: match node.role() {
                        Role::Follower => "follower",
                        Role::Candidate => "candidate",
                        Role::Leader => "leader",
                    },
                    status: match self.cluster.status(id) {
                        NodeStatus::Running => "running",
                        NodeStatus::Crashed => "crashed",
                        NodeStatus::Paused { .. } => "paused",
                    },
                    term: node.current_term(),
                    commit_index: node.commit_index(),
                    last_applied: node.last_applied(),
                    last_index: log.last_index(),
                    log_start: log.first_index(),
                    voted_for: node.voted_for(),
                    leader_id: node.leader_id(),
                    snapshot_index: node.snapshot().map(|s| s.index()).unwrap_or(0),
                    config: node.cluster().describe(),
                    is_joint: node.is_joint(),
                    pending_reads: node.pending_reads(),
                    log: entries,
                }
            })
            .collect();

        let in_flight: Vec<MessageView> = self
            .cluster
            .queue()
            .iter()
            .filter_map(|((tick, _), event)| match event {
                Event::Deliver {
                    from,
                    to,
                    msg,
                    sent_at,
                } => Some(MessageView {
                    from: *from,
                    to: *to,
                    kind: msg.kind(),
                    sent_at: *sent_at,
                    arrives_at: *tick,
                }),
                _ => None,
            })
            .collect();

        let blocked_links: Vec<[NodeId; 2]> = self
            .cluster
            .partitions()
            .blocked_links()
            .iter()
            .map(|(a, b)| [*a, *b])
            .collect();

        let leader = self.cluster.leader();
        let kv = leader
            .map(|id| {
                self.cluster
                    .machine(id)
                    .snapshot()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();

        StateView {
            tick: self.cluster.now(),
            events_processed: self.cluster.events_processed(),
            nodes,
            in_flight,
            blocked_links,
            leaders: self.cluster.leaders(),
            stats: self.cluster.stats().clone(),
            violations: self
                .cluster
                .violations()
                .iter()
                .map(|v| v.to_string())
                .collect(),
            kv,
        }
    }
}

fn describe_moment(m: &sim::Moment) -> String {
    use sim::Moment as M;
    match m {
        M::ElectionStarted { node, term } => format!("node {node} campaigns, term {term}"),
        M::LeaderElected { node, term } => format!("node {node} elected leader, term {term}"),
        M::Committed { node, index } => format!("node {node} committed through {index}"),
        M::LogTruncated {
            node,
            from,
            entries,
        } => format!("node {node} truncated {entries} entries from {from}"),
        M::Fault { description } => description.clone(),
        M::Crashed { node } => format!("node {node} crashed"),
        M::Restarted { node } => format!("node {node} restarted"),
        M::SnapshotTaken { node, through } => format!("node {node} snapshotted through {through}"),
        M::SnapshotInstalled { node, through } => {
            format!("node {node} installed a snapshot through {through}")
        }
        M::MembershipChanged { voters } => format!("membership -> {voters:?}"),
        M::Violation { invariant, detail } => format!("{invariant}: {detail}"),
    }
}

fn describe_fault(fault: &Fault) -> String {
    match fault {
        Fault::Partition { a, b } => format!("partition {a:?} | {b:?}"),
        Fault::AsymmetricCut { from, to } => format!("cut {from} to {to}"),
        Fault::Isolate { node } => format!("isolate {node}"),
        Fault::Heal => "heal".to_string(),
        Fault::Crash { node } => format!("crash {node}"),
        Fault::Restart { node } => format!("restart {node}"),
        Fault::Pause { node, ticks } => format!("pause {node} for {ticks}"),
    }
}

/// Names of the deliberate bugs the UI can switch on.
#[wasm_bindgen(js_name = availableBugs)]
pub fn available_bugs() -> String {
    let bugs: BTreeSet<&str> = ["commit-rule", "double-vote", "blind-commit", "no-persist"]
        .into_iter()
        .collect();
    serde_json::to_string(&bugs).unwrap_or_default()
}
