//! The simulator: owns the clock, the network, the disks and the nodes.
//!
//! The main loop is deliberately dull:
//!
//! ```text
//! pop the earliest (tick, seq) event
//! advance the virtual clock to that tick
//! deliver it to its node, collect the node's outputs
//! apply persists, schedule sends, apply committed entries
//! re-arm that node's timer
//! ```
//!
//! There is no concurrency, no clock and no entropy other than the seeded PRNG,
//! which is what makes a run a pure function of its [`SimConfig`].

use std::collections::{BTreeMap, BTreeSet};

use kvstore::{KvCommand, KvResult, KvStore};
use raft::{
    ClientId, ClientRequest, ClientResult, ClusterConfig, EntryPayload, Index, Input, LogId,
    NodeId, Output, PersistOp, RaftConfig, RaftNode, Rng, Role, Snapshot, Term, Tick,
};
use serde::{Deserialize, Serialize};

use crate::event::{Event, EventQueue, Seq};
use crate::faults::{DiskConfig, Fault, FaultConfig, Partitions};
use crate::history::History;
use crate::invariants::{Invariant, Invariants, Violation};
use crate::linearizability::{self, Verdict};
use crate::network::{Network, NetworkConfig, NetworkStats, Routed};
use crate::storage::Storage;
use crate::timeline::{Moment, Timeline};
use crate::trace::{Trace, TraceKind};

/// Everything that defines a run. Same config, same run, always.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: usize,
    pub raft: RaftConfig,
    pub network: NetworkConfig,
    /// Partitions and other disturbances over the course of the run.
    pub faults: FaultConfig,
    /// Disk behaviour. See [`DiskConfig`] for what is and is not modelled.
    pub disk: DiskConfig,
    /// Maximum per-node clock rate deviation, per mille.
    ///
    /// Every node runs on its own clock, drifting by up to this much from the
    /// simulator's. Nothing in Raft may depend on two nodes agreeing about how
    /// fast time passes, and a lease-based optimisation that quietly does will
    /// break here (step 12). 0 means every clock runs at exactly the same rate.
    pub clock_skew_permille: u32,
    /// Record every input and output. On by default; the fuzzer turns it off
    /// and re-runs failing seeds with it on.
    pub record_trace: bool,
    /// Replay this exact list of faults instead of generating a schedule.
    ///
    /// This is what makes repro minimization possible: a failing run's injected
    /// faults are captured, then removed one at a time and replayed to see
    /// whether the failure survives.
    pub scripted_faults: Option<Vec<(Tick, Fault)>>,
    /// Which nodes start as voters. `None` means all of them.
    ///
    /// Nodes outside the initial configuration still exist and still receive
    /// replication — they are the ones a membership change can add. They never
    /// campaign, because `start_election` refuses for a non-voter.
    pub initial_voters: Option<Vec<NodeId>>,
    /// Take a snapshot and compact the log every this many applied entries.
    ///
    /// `None` lets the log grow forever, which is the right control: it makes
    /// any behaviour difference attributable to compaction alone.
    pub snapshot_every: Option<u64>,
    /// DELIBERATE BUG, off by default: skip the ReadIndex round and let a
    /// leader answer reads straight from its own state machine.
    ///
    /// This exists to validate the linearizability checker, and it is the one
    /// bug in the project that **no internal invariant can catch**. Every Raft
    /// property still holds — one leader per term, logs matching, nothing
    /// committed and lost — because the read never enters the log at all. Only
    /// the client-visible history is wrong: a leader that has been deposed
    /// behind a partition happily serves values that the rest of the cluster
    /// has long since overwritten.
    ///
    /// Avoiding exactly this is what ReadIndex is for (step 12).
    pub stale_reads: bool,
    /// Run the safety checkers after every event. On by default: they are
    /// cheap (O(entries changed) per event) and they are the difference between
    /// "the run looked fine" and "the run was correct".
    pub check_invariants: bool,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            seed: 0,
            nodes: 3,
            raft: RaftConfig::default(),
            network: NetworkConfig::default(),
            faults: FaultConfig::default(),
            disk: DiskConfig::default(),
            clock_skew_permille: 0,
            record_trace: true,
            scripted_faults: None,
            initial_voters: None,
            snapshot_every: None,
            stale_reads: false,
            check_invariants: true,
        }
    }
}

impl SimConfig {
    pub fn with_seed(seed: u64) -> Self {
        SimConfig {
            seed,
            ..SimConfig::default()
        }
    }
}

/// PRNG stream labels. Each subsystem draws from its own derived stream so that
/// adding a draw in one place does not shift every other subsystem's sequence
/// and invalidate recorded seeds.
mod stream {
    pub const NETWORK: u64 = 0x4e45_5457_4f52_4b00;
    pub const FAULTS: u64 = 0x4641_554c_5453_0000;
    pub const CLOCKS: u64 = 0x434c_4f43_4b53_0000;
    pub const DISK: u64 = 0x4449_534b_0000_0000;
}

/// What a client learned about one request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOutcome {
    pub client: ClientId,
    pub seq: u64,
    pub node: NodeId,
    pub tick: Tick,
    pub result: OutcomeKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeKind {
    /// The command committed and the state machine produced this.
    Applied(KvResult),
    /// The node was not the leader; the request never entered the log.
    NotLeader { leader: Option<NodeId> },
}

/// A short description of a fault, for the timeline.
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

/// Rebuild a state machine from snapshot bytes.
fn decode_snapshot(snapshot: &Snapshot) -> KvStore {
    serde_json::from_slice(&snapshot.data)
        .expect("snapshots in this simulator are always produced by serializing a KvStore")
}

/// Whether a node is currently able to process anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Running,
    /// Dead. Messages addressed to it are discarded, and everything volatile is
    /// gone; only the contents of its disk survive.
    Crashed,
    /// Frozen until this tick. Messages addressed to it are held rather than
    /// lost, the way a kernel socket buffer holds them for a stopped process.
    Paused {
        until: Tick,
    },
}

/// What actually happened during a run.
///
/// A run reporting no violations is only evidence of anything alongside proof
/// that it reached the interesting states. If a fault preset never causes an
/// election or a log truncation, it is not testing replication under stress
/// however many seeds it burns.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStats {
    /// Elections started (a node became a candidate).
    pub elections_started: u64,
    /// Elections won.
    pub leaders_elected: u64,
    /// Highest term any node reached.
    pub max_term: Term,
    /// Log truncations: a follower discarding entries that conflicted with the
    /// leader's. This is the path that divergence and reconvergence run
    /// through, and the one a fault-free run never touches.
    pub log_truncations: u64,
    /// Entries discarded by those truncations.
    pub entries_truncated: u64,
    pub entries_applied: u64,
    pub client_responses: u64,
    pub faults_injected: u64,
    pub crashes: u64,
    pub torn_steps: u64,
    pub restarts: u64,
    pub pauses: u64,
    /// Messages held while a node was paused and redelivered on resume.
    pub messages_deferred: u64,
    pub snapshots_taken: u64,
    pub snapshots_installed: u64,
    pub membership_changes: u64,
    /// Reads answered through ReadIndex, without a log entry.
    pub reads_served: u64,
}

/// One entry as seen by a node's state machine. Recorded per node so that
/// State Machine Safety ("no two nodes apply different commands at the same
/// index") is a direct comparison rather than an inference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedEntry {
    pub index: Index,
    pub term: Term,
    pub command: Option<KvCommand>,
}

pub struct Cluster {
    cfg: SimConfig,
    now: Tick,
    queue: EventQueue,

    nodes: BTreeMap<NodeId, RaftNode>,
    storage: BTreeMap<NodeId, Storage>,
    machines: BTreeMap<NodeId, KvStore>,
    applied: BTreeMap<NodeId, Vec<AppliedEntry>>,

    net: Network,
    net_rng: Rng,
    fault_rng: Rng,
    disk_rng: Rng,
    status: BTreeMap<NodeId, NodeStatus>,
    /// Per-node clock rate, per mille of the simulator's. 1000 is exact.
    clock_rate: BTreeMap<NodeId, u64>,
    restarts: BTreeMap<NodeId, u64>,
    compaction_pending: BTreeSet<NodeId>,
    /// True while a disturbance is in effect, so the schedule alternates
    /// between breaking something and healing it.
    disturbed: bool,
    faults_injected: Vec<(Tick, Fault)>,

    trace: Trace,
    /// The timer this node currently has pending, so the queue does not fill
    /// with one stale timer per delivered message.
    timer_at: BTreeMap<NodeId, Tick>,

    checker: Invariants,
    history: History,
    timeline: Timeline,
    stats: RunStats,

    outcomes: Vec<ClientOutcome>,
    /// A client hears about a request once. A second leader re-applying the
    /// same entry must not produce a second answer.
    answered: BTreeSet<(ClientId, u64)>,

    events_processed: u64,
}

impl Cluster {
    pub fn new(cfg: SimConfig) -> Self {
        assert!(cfg.nodes > 0, "a cluster needs at least one node");
        let ids: Vec<NodeId> = (0..cfg.nodes as NodeId).collect();
        let cluster_cfg = match &cfg.initial_voters {
            Some(voters) => ClusterConfig::new(voters.clone()),
            None => ClusterConfig::new(ids.clone()),
        };

        let mut nodes = BTreeMap::new();
        let mut storage = BTreeMap::new();
        let mut machines = BTreeMap::new();
        let mut applied = BTreeMap::new();
        for id in &ids {
            nodes.insert(
                *id,
                RaftNode::new(*id, cluster_cfg.clone(), cfg.raft.clone(), cfg.seed, 0),
            );
            let mut disk = Storage::new();
            // The durable log mirrors the node's, so a regression that corrupts
            // one corrupts the other. It has to be told about the switch or the
            // contiguity guard fires on the disk instead of letting the
            // corruption play out where the checker can report it.
            disk.log
                .set_zero_anchor_bug(cfg.raft.bugs.compaction_anchor_at_zero);
            storage.insert(*id, disk);
            machines.insert(*id, KvStore::new());
            applied.insert(*id, Vec::new());
        }

        let trace = if cfg.record_trace {
            Trace::new()
        } else {
            Trace::disabled()
        };

        let mut sim = Cluster {
            net: Network::new(cfg.network.clone()),
            net_rng: Rng::derive(cfg.seed, stream::NETWORK),
            fault_rng: Rng::derive(cfg.seed, stream::FAULTS),
            disk_rng: Rng::derive(cfg.seed, stream::DISK),
            status: BTreeMap::new(),
            clock_rate: BTreeMap::new(),
            restarts: BTreeMap::new(),
            compaction_pending: BTreeSet::new(),
            disturbed: false,
            faults_injected: Vec::new(),
            cfg,
            now: 0,
            queue: EventQueue::new(),
            nodes,
            storage,
            machines,
            applied,
            trace,
            checker: Invariants::new(),
            history: History::new(),
            timeline: Timeline::new(),
            stats: RunStats::default(),
            timer_at: BTreeMap::new(),
            outcomes: Vec::new(),
            answered: BTreeSet::new(),
            events_processed: 0,
        };

        // Each node gets its own clock rate, fixed for the run.
        let mut clock_rng = Rng::derive(sim.cfg.seed, stream::CLOCKS);
        let skew = sim.cfg.clock_skew_permille as u64;
        for id in sim.node_ids() {
            let rate = if skew == 0 {
                1000
            } else {
                clock_rng.gen_range(1000 - skew.min(900), 1000 + skew + 1)
            };
            sim.clock_rate.insert(id, rate);
            sim.status.insert(id, NodeStatus::Running);
            sim.restarts.insert(id, 0);
        }

        // Prime one timer per node. From here the invariant is "every node
        // always has at least one timer pending", maintained by `rearm_timer`.
        for id in ids {
            let at = sim.nodes[&id].next_deadline().max(1);
            sim.queue.push(at, Event::Timer { node: id });
            sim.timer_at.insert(id, at);
        }

        // The fault schedule is generated lazily: each disturbance schedules
        // its own repair, and each repair schedules the next disturbance. That
        // is equivalent to generating a whole schedule up front from the seed
        // -- the draws come from a dedicated stream in a fixed order -- but it
        // needs no horizon, so a run can be extended without changing what
        // already happened.
        match &sim.cfg.scripted_faults {
            // A fixed script: replay exactly these, generate nothing.
            Some(script) => {
                for (at, fault) in script.clone() {
                    sim.queue.push(at.max(1), Event::Fault { fault });
                }
            }
            None if sim.cfg.faults.enabled => sim.schedule_next_fault(),
            None => {}
        }
        sim
    }

    fn schedule_next_fault(&mut self) {
        let delay = if self.disturbed {
            self.cfg.faults.outage_span(&mut self.fault_rng)
        } else {
            self.cfg.faults.healthy_span(&mut self.fault_rng)
        };
        let at = self.now + delay.max(1);
        let fault = if self.disturbed {
            Fault::Heal
        } else {
            let ids = self.node_ids();
            match self.cfg.faults.next_fault(&ids, &mut self.fault_rng) {
                Some(f) => f,
                None => return,
            }
        };
        self.queue.push(at, Event::Fault { fault });
    }

    fn apply_fault(&mut self, seq: Seq, fault: Fault) {
        let ids = self.node_ids();
        self.net.partitions_mut().apply(&fault, &ids);
        self.apply_node_fault(seq, &fault);

        // A crash stays a crash until the schedule repairs it, so `Heal` also
        // restarts whatever it killed.
        self.disturbed = !matches!(fault, Fault::Heal);
        self.stats.faults_injected += 1;
        self.faults_injected.push((self.now, fault.clone()));
        self.timeline.push(
            self.now,
            Moment::Fault {
                description: describe_fault(&fault),
            },
        );
        self.trace
            .push(self.now, seq, 0, TraceKind::Fault { fault });
        // A scripted run replays its list and generates nothing further.
        if self.cfg.faults.enabled && self.cfg.scripted_faults.is_none() {
            self.schedule_next_fault();
        }
    }

    fn apply_node_fault(&mut self, seq: Seq, fault: &Fault) {
        match fault {
            Fault::Crash { node } => self.crash_node(seq, *node),
            Fault::Restart { node } => self.restart_node(seq, *node),
            Fault::Pause { node, ticks } => {
                if self.is_available(*node) {
                    let until = self.now + (*ticks).max(1);
                    self.status.insert(*node, NodeStatus::Paused { until });
                    self.stats.pauses += 1;
                    self.rearm_timer(*node);
                }
            }
            // Healing repairs everything, including nodes that are down.
            Fault::Heal => {
                for id in self.node_ids() {
                    match self.status[&id] {
                        NodeStatus::Crashed => self.restart_node(seq, id),
                        NodeStatus::Paused { .. } => {
                            self.status.insert(id, NodeStatus::Running);
                            self.rearm_timer(id);
                        }
                        NodeStatus::Running => {}
                    }
                }
            }
            Fault::Partition { .. } | Fault::AsymmetricCut { .. } | Fault::Isolate { .. } => {}
        }
    }

    fn crash_node(&mut self, seq: Seq, node: NodeId) {
        if !self.is_available(node) {
            return;
        }
        self.status.insert(node, NodeStatus::Crashed);
        self.stats.crashes += 1;
        self.timeline.push(self.now, Moment::Crashed { node });
        self.timer_at.remove(&node);
        self.trace.push(self.now, seq, node, TraceKind::Crashed);
    }

    /// Rebuild a node from its disk and nothing else.
    ///
    /// This is the test that the node persists exactly what Figure 2 requires.
    /// Everything volatile is rebuilt from scratch: the role, `commitIndex`,
    /// `lastApplied`, and the state machine, which is emptied so the log has to
    /// replay into it. If the node were persisting too little, recovery would
    /// lose something; if the simulator kept anything it should not, the test
    /// would pass for the wrong reason.
    fn restart_node(&mut self, seq: Seq, node: NodeId) {
        if self.status[&node] != NodeStatus::Crashed {
            return;
        }
        // Recovery fixes the durable log up against the durable snapshot
        // before anything reads either of them.
        let reconcile = !self.cfg.raft.bugs.skip_snapshot_reconcile;
        self.storage
            .get_mut(&node)
            .expect("every node has storage")
            .recover(reconcile);
        let mut disk = self.storage[&node].clone();
        disk.log
            .set_zero_anchor_bug(self.cfg.raft.bugs.compaction_anchor_at_zero);
        let restarts = self.restarts.entry(node).or_insert(0);
        *restarts += 1;
        // A fresh process draws fresh election timeouts rather than replaying
        // the ones it had before it died.
        let seed = self.cfg.seed.wrapping_add(*restarts * 0x9E37_79B9);
        // The membership a recovering node starts from is whatever its
        // snapshot recorded; the log's config entries are replayed on top by
        // `restore`. Using the simulator's node list here instead would
        // resurrect a configuration the cluster has already left behind.
        let cluster_cfg = match &disk.snapshot {
            Some(snap) => snap.config.clone(),
            None => match &self.cfg.initial_voters {
                Some(voters) => ClusterConfig::new(voters.clone()),
                None => ClusterConfig::new(self.node_ids()),
            },
        };
        let local_now = self.local_time(node, self.now);

        self.nodes.insert(
            node,
            RaftNode::restore(
                node,
                cluster_cfg,
                self.cfg.raft.clone(),
                seed,
                local_now,
                disk.current_term,
                disk.voted_for,
                disk.log.clone(),
                disk.snapshot.clone(),
            ),
        );
        // Volatile, and therefore gone. With a snapshot the state machine comes
        // back from it and the remaining log replays on top; without one the
        // whole log replays into an empty store.
        let restored = match &disk.snapshot {
            Some(snap) => decode_snapshot(snap),
            None => KvStore::new(),
        };
        self.machines.insert(node, restored);
        self.status.insert(node, NodeStatus::Running);
        self.stats.restarts += 1;
        self.timeline.push(self.now, Moment::Restarted { node });

        // The checker has to know, or it will report the legitimate reset of
        // commitIndex and lastApplied as violations.
        let applied_from = disk.snapshot.as_ref().map(|s| s.index()).unwrap_or(0);
        self.checker.note_restart(node, applied_from);

        self.trace.push(
            self.now,
            seq,
            node,
            TraceKind::Restarted {
                current_term: disk.current_term,
                voted_for: disk.voted_for,
                last_index: disk.log.last_index(),
            },
        );
        self.rearm_timer(node);
    }

    /// The simulator's tick, as this node's own clock sees it.
    fn local_time(&self, node: NodeId, global: Tick) -> Tick {
        let rate = self.clock_rate.get(&node).copied().unwrap_or(1000);
        global * rate / 1000
    }

    /// The simulator tick at which this node's clock will read `local`.
    ///
    /// Rounds UP. Rounding down would schedule the timer a hair early, the node
    /// would find its deadline not yet reached, do nothing, and be rescheduled
    /// one tick later -- burning an event per tick until the clocks agreed.
    fn global_time(&self, node: NodeId, local: Tick) -> Tick {
        let rate = self.clock_rate.get(&node).copied().unwrap_or(1000);
        (local * 1000).div_ceil(rate)
    }

    // --- clock and main loop ---------------------------------------------

    pub fn now(&self) -> Tick {
        self.now
    }

    pub fn config(&self) -> &SimConfig {
        &self.cfg
    }

    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// Process exactly one event. Returns false only if the queue is empty,
    /// which with timers always re-armed means "never" in practice.
    pub fn step_once(&mut self) -> bool {
        let Some(((tick, seq), event)) = self.queue.pop_next() else {
            return false;
        };
        debug_assert!(
            tick >= self.now,
            "event queue produced an event in the past"
        );
        self.now = tick;
        self.events_processed += 1;

        match event {
            Event::Timer { node } => {
                // Only clear the pending marker if this is the timer we are
                // tracking. A stale timer from a superseded deadline must not
                // make us schedule an extra one, or the queue grows by one
                // event per delivery.
                if self.timer_at.get(&node) == Some(&tick) {
                    self.timer_at.remove(&node);
                }
                if self.resume_if_due(node) || self.is_available(node) {
                    self.deliver(seq, node, Input::Tick);
                }
            }
            Event::Deliver { from, to, msg, .. } => match self.status[&to] {
                NodeStatus::Running => self.deliver(seq, to, Input::Message { from, msg }),
                // A crashed node is not there to receive anything.
                NodeStatus::Crashed => {
                    self.trace
                        .push(self.now, seq, to, TraceKind::LostToCrash { from });
                }
                // A frozen node's messages wait in the buffer rather than
                // vanishing, and hit it all at once when it wakes up.
                NodeStatus::Paused { until } => {
                    self.stats.messages_deferred += 1;
                    self.queue.push(
                        until.max(self.now + 1),
                        Event::Deliver {
                            from,
                            to,
                            msg,
                            sent_at: self.now,
                        },
                    );
                }
            },
            Event::Client { node, req } => {
                if self.is_available(node) && !self.serve_stale_read(seq, node, &req) {
                    self.deliver(seq, node, Input::ClientRequest(req));
                }
            }
            Event::Compact { node } => self.compact_node(seq, node),
            Event::Fault { fault } => self.apply_fault(seq, fault),
        }
        true
    }

    /// The deliberate stale-read bug. Returns true if the read was answered
    /// locally, bypassing Raft entirely.
    fn serve_stale_read(&mut self, seq: Seq, node: NodeId, req: &ClientRequest) -> bool {
        if !self.cfg.stale_reads || self.nodes[&node].role() != Role::Leader {
            return false;
        }
        let Ok(cmd) = KvCommand::decode(&req.command) else {
            return false;
        };
        if !cmd.is_read_only() {
            return false;
        }
        let result = self
            .machines
            .get_mut(&node)
            .expect("every node has a state machine")
            .apply(&cmd);
        let tick = self.now;
        self.trace.push(
            tick,
            seq,
            node,
            TraceKind::Applied {
                index: 0,
                term: self.nodes[&node].current_term(),
                command: Some(cmd),
                result: Some(result.clone()),
            },
        );
        self.record_outcome(node, req.client, req.seq, OutcomeKind::Applied(result));
        true
    }

    /// Mirror any violations the checker just found into the timeline, so a
    /// failure is something you can click on rather than something you have to
    /// scroll a log for.
    fn record_new_violations(&mut self, before: usize) {
        let fresh: Vec<(Tick, String, String)> = self.checker.violations()[before..]
            .iter()
            .map(|v| (v.tick, v.invariant.name().to_string(), v.detail.clone()))
            .collect();
        for (tick, invariant, detail) in fresh {
            self.timeline
                .push(tick, Moment::Violation { invariant, detail });
        }
    }

    fn is_available(&self, node: NodeId) -> bool {
        matches!(self.status[&node], NodeStatus::Running)
    }

    /// Wake a paused node whose time has come. Returns true if it just resumed.
    fn resume_if_due(&mut self, node: NodeId) -> bool {
        if let NodeStatus::Paused { until } = self.status[&node] {
            if self.now >= until {
                self.status.insert(node, NodeStatus::Running);
                return true;
            }
        }
        false
    }

    /// Run until the virtual clock reaches `deadline`.
    pub fn run_until(&mut self, deadline: Tick) {
        while let Some(next) = self.queue.next_tick() {
            if next > deadline {
                break;
            }
            if !self.step_once() {
                break;
            }
        }
        self.now = self.now.max(deadline);
    }

    /// Run `ticks` further ticks of virtual time.
    pub fn run_for(&mut self, ticks: Tick) {
        let deadline = self.now + ticks;
        self.run_until(deadline);
    }

    // --- delivering to a node ---------------------------------------------

    fn deliver(&mut self, seq: Seq, node: NodeId, input: Input) {
        self.trace
            .push(self.now, seq, node, TraceKind::In(input.clone()));

        let (was_candidate, was_leader, commit_before) = {
            let n = &self.nodes[&node];
            (
                n.role() == Role::Candidate,
                n.role() == Role::Leader,
                n.commit_index(),
            )
        };

        let local_now = self.local_time(node, self.now);
        let outputs = {
            let n = self
                .nodes
                .get_mut(&node)
                .expect("event addressed to a node that does not exist");
            n.step(input, local_now)
        };

        {
            let tick = self.now;
            let (role, term, commit) = {
                let n = &self.nodes[&node];
                (n.role(), n.current_term(), n.commit_index())
            };
            if role == Role::Candidate && !was_candidate {
                self.stats.elections_started += 1;
                self.timeline
                    .push(tick, Moment::ElectionStarted { node, term });
            }
            if role == Role::Leader && !was_leader {
                self.stats.leaders_elected += 1;
                self.timeline
                    .push(tick, Moment::LeaderElected { node, term });
            }
            self.stats.max_term = self.stats.max_term.max(term);
            self.timeline.note_commit(tick, node, commit);
        }

        // The lowest log index this step touched, so the Log Matching scan only
        // looks at what actually changed. Every log mutation emits a matching
        // persist, so the persist stream is a complete record of them.
        let mut dirty_from: Option<Index> = None;
        let mark_dirty = |index: Index, dirty: &mut Option<Index>| {
            *dirty = Some(dirty.map_or(index, |d: Index| d.min(index)));
        };

        // Decide up front whether this step's writes are torn. Outputs are
        // ordered persists-first, so "stop after k outputs" is exactly "k
        // persists landed and nothing was ever sent".
        let tear_at = self.pick_tear_point(&outputs);

        for (i, out) in outputs.into_iter().enumerate() {
            if Some(i) == tear_at {
                self.tear_step(seq, node);
                return;
            }
            self.trace
                .push(self.now, seq, node, TraceKind::Out(out.clone()));
            match out {
                // Durable first, and the node's output ordering guarantees this
                // arm runs before any Send from the same step.
                Output::Persist(op) => {
                    match &op {
                        PersistOp::Append(entries) => {
                            if let Some(first) = entries.first() {
                                mark_dirty(first.index, &mut dirty_from);
                            }
                        }
                        PersistOp::TruncateFrom(index) => {
                            let held = self.storage[&node].log.last_index();
                            let lost = held.saturating_sub(*index) + 1;
                            self.stats.log_truncations += 1;
                            self.stats.entries_truncated += lost;
                            self.timeline.push(
                                self.now,
                                Moment::LogTruncated {
                                    node,
                                    from: *index,
                                    entries: lost,
                                },
                            );
                            if self.cfg.check_invariants {
                                let tick = self.now;
                                self.checker
                                    .observe_truncation(tick, node, *index, commit_before);
                            }
                            mark_dirty(*index, &mut dirty_from);
                        }
                        PersistOp::HardState { .. } => {}
                        // Compaction removes entries below the live range but
                        // changes none of the ones that remain, so there is
                        // nothing for the Log Matching scan to re-examine.
                        // `ResetLog` empties the log outright; whatever arrives
                        // afterwards marks itself dirty when it is appended.
                        PersistOp::Snapshot(_) | PersistOp::Compact(_) | PersistOp::ResetLog(_) => {
                        }
                    }
                    self.storage
                        .get_mut(&node)
                        .expect("every node has storage")
                        .apply(&op);
                }
                Output::Send { to, msg } => {
                    match self.net.route(self.now, node, to, &mut self.net_rng) {
                        Routed::Once(at) => {
                            self.queue.push(
                                at,
                                Event::Deliver {
                                    from: node,
                                    to,
                                    msg,
                                    sent_at: self.now,
                                },
                            );
                        }
                        Routed::Twice(first, second) => {
                            // The same message, delivered twice at independently
                            // sampled times. Raft has to be idempotent under
                            // this: a repeated AppendEntries must not truncate a
                            // log that has moved on, and a repeated vote must
                            // not be counted twice.
                            self.queue.push(
                                first,
                                Event::Deliver {
                                    from: node,
                                    to,
                                    msg: msg.clone(),
                                    sent_at: self.now,
                                },
                            );
                            self.queue.push(
                                second,
                                Event::Deliver {
                                    from: node,
                                    to,
                                    msg,
                                    sent_at: self.now,
                                },
                            );
                            self.trace
                                .push(self.now, seq, node, TraceKind::Duplicated { to });
                        }
                        Routed::Dropped => {
                            self.trace
                                .push(self.now, seq, node, TraceKind::Dropped { to });
                        }
                        Routed::Partitioned => {
                            self.trace
                                .push(self.now, seq, node, TraceKind::Partitioned { to });
                        }
                    }
                }
                Output::Apply {
                    index,
                    term,
                    payload,
                    client,
                    respond,
                } => {
                    self.apply_entry(seq, node, index, term, payload, client, respond);
                }
                Output::ServeRead {
                    client,
                    seq: cseq,
                    command,
                    read_index,
                } => {
                    // ReadIndex has confirmed leadership and the state machine
                    // has caught up, so this is safe to answer locally — which
                    // is the whole point: no log entry, no replication round.
                    let kv = KvCommand::decode(&command)
                        .expect("the kvstore is the only source of commands here");
                    let result = self
                        .machines
                        .get_mut(&node)
                        .expect("every node has a state machine")
                        .apply(&kv);
                    self.stats.reads_served += 1;
                    self.trace.push(
                        self.now,
                        seq,
                        node,
                        TraceKind::ReadServed {
                            read_index,
                            result: result.clone(),
                        },
                    );
                    self.record_outcome(node, client, cseq, OutcomeKind::Applied(result));
                }
                Output::RestoreSnapshot { snapshot } => {
                    // The entries that produced this state are gone from the
                    // log, so the state machine cannot be replayed into shape —
                    // it is replaced outright.
                    let through = snapshot.index();
                    self.machines.insert(node, decode_snapshot(&snapshot));
                    self.stats.snapshots_installed += 1;
                    self.timeline
                        .push(self.now, Moment::SnapshotInstalled { node, through });
                    if self.cfg.check_invariants {
                        self.checker.note_snapshot_installed(node, through);
                    }
                    self.trace.push(
                        self.now,
                        seq,
                        node,
                        TraceKind::SnapshotInstalled {
                            through,
                            bytes: snapshot.len(),
                        },
                    );
                }
                Output::ClientResponse {
                    client,
                    seq: cseq,
                    result,
                } => {
                    let kind = match result {
                        ClientResult::NotLeader { leader } => OutcomeKind::NotLeader { leader },
                        // Membership changes are not client operations and are
                        // not recorded in the history.
                        ClientResult::ChangeInProgress => continue,
                    };
                    self.record_outcome(node, client, cseq, kind);
                }
            }
        }

        if self.cfg.check_invariants {
            let tick = self.now;
            let before = self.checker.violations().len();
            self.checker
                .observe_node(tick, &self.nodes[&node], dirty_from);
            self.record_new_violations(before);
        }

        self.consider_compaction(node);
        self.rearm_timer(node);
    }

    /// How many outputs of this step survive before the process dies, or `None`
    /// if it completes normally.
    fn pick_tear_point(&mut self, outputs: &[Output]) -> Option<usize> {
        let permille = self.cfg.disk.torn_step_permille;
        if permille == 0 {
            return None;
        }
        let persists = outputs
            .iter()
            .take_while(|o| matches!(o, Output::Persist(_)))
            .count();
        if persists == 0 {
            return None;
        }
        if !self.disk_rng.chance(permille as u64, 1000) {
            return None;
        }
        // 0 means "died before writing anything"; `persists` means "every write
        // landed but nothing was sent". Both are real crash points.
        Some(self.disk_rng.gen_range(0, persists as u64 + 1) as usize)
    }

    fn tear_step(&mut self, seq: Seq, node: NodeId) {
        self.stats.torn_steps += 1;
        self.trace.push(self.now, seq, node, TraceKind::TornStep);
        // The in-memory node has already applied the whole step, but that state
        // never became visible to anyone -- no message left. It is discarded
        // wholesale, so it is deliberately NOT shown to the checker: recording
        // facts about state nobody observed could only weaken or mislead it.
        self.crash_node(seq, node);
        let delay = self.cfg.disk.restart_delay(&mut self.disk_rng).max(1);
        self.queue.push(
            self.now + delay,
            Event::Fault {
                fault: Fault::Restart { node },
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_entry(
        &mut self,
        seq: Seq,
        node: NodeId,
        index: Index,
        term: Term,
        payload: EntryPayload,
        client: Option<(ClientId, u64)>,
        respond: bool,
    ) {
        let (command, result) = match payload {
            // The leader's election no-op and membership entries carry nothing
            // for the state machine, but they are still recorded: State Machine
            // Safety compares the full applied sequence, including these.
            EntryPayload::Noop | EntryPayload::Config(_) => (None, None),
            EntryPayload::Command(cmd) => {
                let kv = KvCommand::decode(&cmd)
                    .expect("the kvstore is the only source of commands in this simulator");
                // Through the session table (§8), so a retry that reached the
                // log twice is applied once.
                let result = self
                    .machines
                    .get_mut(&node)
                    .expect("every node has a state machine")
                    .apply_for(client, &kv);
                (Some(kv), Some(result))
            }
        };

        self.stats.entries_applied += 1;
        self.applied
            .get_mut(&node)
            .expect("every node has an applied log")
            .push(AppliedEntry {
                index,
                term,
                command: command.clone(),
            });

        if self.cfg.check_invariants {
            let tick = self.now;
            self.checker
                .observe_apply(tick, node, index, term, &command);
        }

        self.trace.push(
            self.now,
            seq,
            node,
            TraceKind::Applied {
                index,
                term,
                command,
                result: result.clone(),
            },
        );

        if let (true, Some((client, cseq)), Some(result)) = (respond, client, result) {
            self.record_outcome(node, client, cseq, OutcomeKind::Applied(result));
        }
    }

    fn record_outcome(&mut self, node: NodeId, client: ClientId, seq: u64, result: OutcomeKind) {
        // A `NotLeader` refusal does not close a request: the client is
        // expected to retry elsewhere with the same sequence number.
        if matches!(result, OutcomeKind::Applied(_)) && !self.answered.insert((client, seq)) {
            return;
        }
        let tick = self.now;
        match &result {
            OutcomeKind::Applied(r) => self.history.complete(client, seq, tick, r.clone()),
            // A refusal is real information: the command never entered any log,
            // so unlike silence it does not have to be considered as possibly
            // having taken effect.
            // A refusal is a failed attempt, not the end of the operation: the
            // client retries elsewhere with the same sequence number.
            OutcomeKind::NotLeader { .. } => self.history.refuse(client, seq),
        }
        self.stats.client_responses += 1;
        self.outcomes.push(ClientOutcome {
            client,
            seq,
            node,
            tick: self.now,
            result,
        });
    }

    /// Schedule a compaction if this node has applied enough since its last
    /// snapshot.
    ///
    /// Queued as an event rather than done inline so that compaction is ordered
    /// with everything else, shows up in the trace, and cannot recurse into the
    /// delivery it was triggered from.
    fn consider_compaction(&mut self, node: NodeId) {
        let Some(every) = self.cfg.snapshot_every else {
            return;
        };
        if self.compaction_pending.contains(&node) {
            return;
        }
        let n = &self.nodes[&node];
        let applied = n.last_applied();
        let covered = n.snapshot().map(|s| s.index()).unwrap_or(0);
        if applied == 0 || applied < covered + every {
            return;
        }
        self.compaction_pending.insert(node);
        self.queue.push(self.now + 1, Event::Compact { node });
    }

    /// Build a snapshot of this node's state machine as of `lastApplied` and
    /// hand it to the node so it can drop the log prefix.
    fn compact_node(&mut self, seq: Seq, node: NodeId) {
        self.compaction_pending.remove(&node);
        if !self.is_available(node) {
            return;
        }
        let n = &self.nodes[&node];
        let applied = n.last_applied();
        let covered = n.snapshot().map(|s| s.index()).unwrap_or(0);
        if applied == 0 || applied <= covered {
            return;
        }
        // The entry at `lastApplied` must still be describable, or there is
        // nothing to anchor the snapshot to.
        let Some(term) = n.log().term_at(applied) else {
            return;
        };
        let data =
            serde_json::to_vec(&self.machines[&node]).expect("the kvstore is always serializable");
        // The membership placeholder is overwritten by the node, which is the
        // only thing that knows the configuration in force at that index.
        let snapshot = Snapshot::new(
            LogId::new(applied, term),
            self.nodes[&node].cluster().clone(),
            data,
        );
        self.stats.snapshots_taken += 1;
        self.timeline.push(
            self.now,
            Moment::SnapshotTaken {
                node,
                through: applied,
            },
        );
        self.deliver(seq, node, Input::Compact(snapshot));
    }

    fn rearm_timer(&mut self, node: NodeId) {
        match self.status[&node] {
            // A dead node has no timers. Restarting re-arms it.
            NodeStatus::Crashed => return,
            // A frozen node wakes at the end of its pause, not before.
            NodeStatus::Paused { until } => {
                let at = until.max(self.now + 1);
                if self.timer_at.get(&node) != Some(&at) {
                    self.queue.push(at, Event::Timer { node });
                    self.timer_at.insert(node, at);
                }
                return;
            }
            NodeStatus::Running => {}
        }
        // The node's deadline is in its own clock; convert back to simulator
        // time. Never schedule in the past: a deadline already passed fires on
        // the next tick instead, which keeps the queue's tick order monotonic.
        let local = self.nodes[&node].next_deadline();
        let deadline = self.global_time(node, local).max(self.now + 1);
        if self.timer_at.get(&node) == Some(&deadline) {
            return;
        }
        self.queue.push(deadline, Event::Timer { node });
        self.timer_at.insert(node, deadline);
    }

    // --- driving clients --------------------------------------------------

    /// Submit a command to `node`. Takes effect at the current tick, ordered
    /// after everything already scheduled for it.
    pub fn submit(&mut self, node: NodeId, client: ClientId, seq: u64, cmd: KvCommand) {
        assert!(self.nodes.contains_key(&node), "no such node: {node}");
        // The operation is invoked the moment the client sends it, not when the
        // node happens to process it. If no response ever comes back it stays
        // PENDING, which is the case the linearizability checker has to reason
        // about in both directions.
        self.history.invoke(client, seq, cmd.clone(), self.now);
        self.queue.push(
            self.now,
            Event::Client {
                node,
                req: ClientRequest {
                    client,
                    seq,
                    // Raft cannot inspect an opaque command, so the application
                    // says whether it only reads. That is what routes it
                    // through ReadIndex instead of the log.
                    read_only: cmd.is_read_only(),
                    command: cmd.encode(),
                },
            },
        );
    }

    // --- inspection --------------------------------------------------------

    pub fn node(&self, id: NodeId) -> &RaftNode {
        &self.nodes[&id]
    }

    pub fn nodes(&self) -> impl Iterator<Item = (&NodeId, &RaftNode)> {
        self.nodes.iter()
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn storage(&self, id: NodeId) -> &Storage {
        &self.storage[&id]
    }

    pub fn machine(&self, id: NodeId) -> &KvStore {
        &self.machines[&id]
    }

    pub fn applied(&self, id: NodeId) -> &[AppliedEntry] {
        &self.applied[&id]
    }

    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Every safety violation found so far, in the order they were found.
    pub fn violations(&self) -> &[Violation] {
        self.checker.violations()
    }

    pub fn checker(&self) -> &Invariants {
        &self.checker
    }

    /// Which invariants are currently broken. For the verification panel.
    pub fn broken_invariants(&self) -> Vec<Invariant> {
        self.checker.broken()
    }

    /// Panic with the full explanation if anything is broken. The message names
    /// the property, the nodes involved, and the usual cause.
    pub fn assert_no_violations(&self) {
        if self.checker.is_clean() {
            return;
        }
        let report: Vec<String> = self.violations().iter().map(|v| v.to_string()).collect();
        panic!(
            "{} safety violation(s) with seed {}:\n{}",
            report.len(),
            self.cfg.seed,
            report.join("\n")
        );
    }

    pub fn outcomes(&self) -> &[ClientOutcome] {
        &self.outcomes
    }

    pub fn queue(&self) -> &EventQueue {
        &self.queue
    }

    /// Coverage counters for the network. A run reporting no violations only
    /// means something alongside evidence that the faults actually fired.
    pub fn network_stats(&self) -> &NetworkStats {
        self.net.stats()
    }

    /// What the run actually exercised: elections, truncations, entries applied.
    pub fn stats(&self) -> &RunStats {
        &self.stats
    }

    /// Every client operation, with the time it was invoked and what — if
    /// anything — came back.
    pub fn history(&self) -> &History {
        &self.history
    }

    /// Is the recorded history linearizable?
    ///
    /// Separate from the per-event invariant checks: those verify Raft's
    /// internal properties, this verifies the promise made to clients.
    pub fn check_linearizability(&self) -> Verdict {
        linearizability::check(&self.history)
    }

    /// Current link state.
    pub fn partitions(&self) -> &Partitions {
        self.net.partitions()
    }

    pub fn is_reachable(&self, from: NodeId, to: NodeId) -> bool {
        self.net.partitions().reachable(from, to)
    }

    /// Notable moments, for the timeline view.
    pub fn timeline(&self) -> &Timeline {
        &self.timeline
    }

    /// Every fault applied so far, with the tick it happened at. The timeline
    /// view reads this.
    pub fn faults_injected(&self) -> &[(Tick, Fault)] {
        &self.faults_injected
    }

    /// Apply a fault right now. This is what the UI's click-to-partition does,
    /// and what handcrafted scenario tests use.
    ///
    /// Messages already in flight are unaffected: they were handed to the
    /// network before the link went down, which is also what happens in a real
    /// one.
    pub fn inject(&mut self, fault: Fault) {
        let seq = self.events_processed;
        let ids = self.node_ids();
        self.net.partitions_mut().apply(&fault, &ids);
        self.apply_node_fault(seq, &fault);
        self.stats.faults_injected += 1;
        self.faults_injected.push((self.now, fault.clone()));
        self.timeline.push(
            self.now,
            Moment::Fault {
                description: describe_fault(&fault),
            },
        );
        self.trace
            .push(self.now, seq, 0, TraceKind::Fault { fault });
    }

    pub fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        self.inject(Fault::Partition {
            a: a.to_vec(),
            b: b.to_vec(),
        });
    }

    /// One-way cut: `from` can no longer reach `to`.
    pub fn cut(&mut self, from: NodeId, to: NodeId) {
        self.inject(Fault::AsymmetricCut { from, to });
    }

    pub fn isolate(&mut self, node: NodeId) {
        self.inject(Fault::Isolate { node });
    }

    pub fn heal(&mut self) {
        self.inject(Fault::Heal);
    }

    /// Ask the current leader to change the cluster membership (§6).
    ///
    /// Returns false if there is nobody to ask. The change itself is
    /// asynchronous: the leader appends C_old,new, and moves to C_new on its
    /// own once that commits.
    pub fn change_membership(&mut self, voters: impl IntoIterator<Item = NodeId>) -> bool {
        let Some(leader) = self.leader() else {
            return false;
        };
        let voters: BTreeSet<NodeId> = voters.into_iter().collect();
        let seq = self.events_processed;
        self.stats.membership_changes += 1;
        self.timeline.push(
            self.now,
            Moment::MembershipChanged {
                voters: voters.iter().copied().collect(),
            },
        );
        self.trace.push(
            self.now,
            seq,
            leader,
            TraceKind::MembershipChange {
                voters: voters.iter().copied().collect(),
            },
        );
        self.deliver(seq, leader, Input::ChangeMembership(voters));
        true
    }

    /// The client gave up on this request.
    pub fn abandon_request(&mut self, client: ClientId, seq: u64) {
        let tick = self.now;
        self.history.abandon(client, seq, tick);
    }

    /// The membership each node currently believes it is in.
    pub fn configuration(&self, node: NodeId) -> &raft::ClusterConfig {
        self.nodes[&node].cluster()
    }

    pub fn crash(&mut self, node: NodeId) {
        self.inject(Fault::Crash { node });
    }

    pub fn restart(&mut self, node: NodeId) {
        self.inject(Fault::Restart { node });
    }

    pub fn pause(&mut self, node: NodeId, ticks: Tick) {
        self.inject(Fault::Pause { node, ticks });
    }

    pub fn status(&self, node: NodeId) -> NodeStatus {
        self.status[&node]
    }

    /// This node's clock rate, per mille of the simulator's.
    pub fn clock_rate(&self, node: NodeId) -> u64 {
        self.clock_rate.get(&node).copied().unwrap_or(1000)
    }

    /// The current leader, if the cluster believes it has exactly one.
    ///
    /// Returns the leader of the highest term seen: during a transition two
    /// nodes can call themselves leader in *different* terms (the old one has
    /// simply not noticed yet), which is legal. Two leaders in the *same* term
    /// would be an Election Safety violation and is what the invariant checker
    /// in step 4 will look for.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.role() == Role::Leader && self.status[&n.id()] != NodeStatus::Crashed)
            .max_by_key(|n| n.current_term())
            .map(|n| n.id())
    }

    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.role() == Role::Leader && self.status[&n.id()] != NodeStatus::Crashed)
            .map(|n| n.id())
            .collect()
    }

    /// Run until a leader exists or `deadline` passes.
    pub fn run_until_leader(&mut self, deadline: Tick) -> Option<NodeId> {
        while self.now < deadline {
            if !self.step_once() {
                break;
            }
            if let Some(id) = self.leader() {
                return Some(id);
            }
        }
        self.leader()
    }
}
