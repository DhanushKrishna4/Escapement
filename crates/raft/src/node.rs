//! The Raft node: a pure state machine.
//!
//! `step(input, now) -> Vec<Output>` is the entire interface. The node holds no
//! sockets, no timers, no clock and no entropy source of its own. It is handed
//! an input and the current virtual time, and it returns a list of things it
//! wants the environment to do.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::config::ClusterConfig;
use crate::log::{EntryPayload, Log, LogEntry};
use crate::message::{
    AppendEntriesReq, AppendEntriesResp, ConflictHint, InstallSnapshotReq, InstallSnapshotResp,
    RaftMessage, RequestVoteReq, RequestVoteResp,
};
use crate::rand::Rng;
use crate::snapshot::Snapshot;
use crate::{ClientId, Command, Index, LogId, NodeId, Term, Tick};

// ---------------------------------------------------------------------------
// Inputs and outputs
// ---------------------------------------------------------------------------

/// Everything that can happen *to* a node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Input {
    /// A message arrived from a peer.
    Message { from: NodeId, msg: RaftMessage },
    /// Virtual time has advanced; check timers. The new time is the `now`
    /// argument to `step`, never read from a clock.
    Tick,
    /// A client submitted a command.
    ClientRequest(ClientRequest),
    /// Change the cluster membership to this voter set (§6).
    ///
    /// The leader turns it into a joint configuration first; the transition to
    /// C_new happens on its own once C_old,new commits.
    ChangeMembership(std::collections::BTreeSet<NodeId>),
    /// The application has captured its state machine through
    /// `snapshot.last_included` and the log prefix can be discarded (§7).
    ///
    /// Compaction is driven from outside because the state machine lives
    /// outside: Raft has no idea what is in the snapshot and must not.
    Compact(Snapshot),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRequest {
    pub client: ClientId,
    /// Per-client monotonic sequence number. Not used for deduplication yet
    /// (step 12), but recorded in the entry from the start so that histories
    /// can match responses to invocations.
    pub seq: u64,
    pub command: Command,
    /// Whether this command only reads.
    ///
    /// Raft cannot tell — commands are opaque bytes and must stay that way — so
    /// the application says. A read-only request goes through ReadIndex instead
    /// of the log.
    pub read_only: bool,
}

/// Everything a node can ask its environment to do.
///
/// ORDERING CONTRACT: within one `step`, all `Persist` outputs come first, then
/// `Send`, then `Apply` / `ClientResponse`. The simulator must make every
/// `Persist` durable before it delivers any `Send` from the same step. This is
/// what lets the node treat "I persisted my vote" as true when the vote reply
/// leaves -- without it, a node could grant a vote, crash, forget, and vote
/// again in the same term, electing two leaders.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Output {
    /// Make this durable before anything else in this step becomes visible.
    Persist(PersistOp),
    /// Deliver this message to `to`. Best effort: the node assumes nothing.
    Send { to: NodeId, msg: RaftMessage },
    /// This committed entry may now be applied to the replicated state machine.
    Apply {
        index: Index,
        term: Term,
        payload: EntryPayload,
        /// The client request that produced this entry, if any.
        client: Option<(ClientId, u64)>,
        /// Whether this node believes it owes the client an answer -- true only
        /// on a leader. Followers apply the same entry silently.
        respond: bool,
    },
    /// Serve this read-only command from the state machine, now.
    ///
    /// Emitted only once ReadIndex has established two things: that this node
    /// was still the leader at the moment the read arrived (confirmed by a
    /// heartbeat round reaching a quorum), and that the state machine has
    /// caught up to the commit index as of that moment. Reading before either
    /// is what lets a deposed leader serve values the cluster has overwritten.
    ServeRead {
        client: ClientId,
        seq: u64,
        command: Command,
        read_index: Index,
    },
    /// Replace the state machine wholesale with this snapshot.
    ///
    /// Emitted when a follower installs a snapshot from the leader. Everything
    /// the state machine held is discarded — the entries that produced it are
    /// gone from the log and cannot be replayed.
    RestoreSnapshot { snapshot: Snapshot },
    /// A direct answer to a client that did not require replication.
    ClientResponse {
        client: ClientId,
        seq: u64,
        result: ClientResult,
    },
}

/// A durable-state mutation.
///
/// Modelled as deltas rather than a full state snapshot so that a trace shows
/// exactly what hit the disk and when, and so that crash recovery replays the
/// same operations a real implementation would write to a WAL.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistOp {
    /// currentTerm and votedFor -- the two scalars §5.1/§5.2 require to survive
    /// a crash. Losing either can elect two leaders in one term.
    HardState {
        current_term: Term,
        voted_for: Option<NodeId>,
    },
    /// Append entries to the durable log.
    Append(Vec<LogEntry>),
    /// Delete the entry at this index and everything after it.
    TruncateFrom(Index),
    /// Save the state machine snapshot.
    ///
    /// Always emitted *before* the compaction that relies on it, so a crash in
    /// between leaves the snapshot stored and the log merely un-trimmed — which
    /// is redundant, not lossy. The reverse order would discard entries whose
    /// replacement had not yet reached the disk.
    Snapshot(Snapshot),
    /// Discard the log prefix through this entry, remembering it so the
    /// `prevLogIndex` check can still be answered at exactly that position.
    Compact(LogId),
    /// Discard the log entirely and restart it after this entry.
    ResetLog(LogId),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientResult {
    /// This node is not the leader. `leader` is a hint, possibly stale.
    NotLeader { leader: Option<NodeId> },
    /// A membership change is already in flight. §6 allows only one at a time:
    /// overlapping changes can produce two configurations with no overlapping
    /// quorum, which is the thing joint consensus exists to prevent.
    ChangeInProgress,
}

/// Collects outputs and enforces the persist-before-send ordering contract.
#[derive(Default)]
struct Outputs {
    /// At most one per step: currentTerm and votedFor are a single logical
    /// record, and a step that changes both (a higher-term RequestVote bumps
    /// the term and then grants a vote) must not write the disk twice.
    hard_state: Option<Output>,
    persists: Vec<Output>,
    sends: Vec<Output>,
    effects: Vec<Output>,
}

impl Outputs {
    fn persist(&mut self, op: PersistOp) {
        self.persists.push(Output::Persist(op));
    }

    fn hard_state(&mut self, current_term: Term, voted_for: Option<NodeId>) {
        self.hard_state = Some(Output::Persist(PersistOp::HardState {
            current_term,
            voted_for,
        }));
    }

    fn send(&mut self, to: NodeId, msg: RaftMessage) {
        self.sends.push(Output::Send { to, msg });
    }

    fn effect(&mut self, out: Output) {
        self.effects.push(out);
    }

    fn finish(mut self) -> Vec<Output> {
        // Hard state before log writes: after a crash, currentTerm must be at
        // least the term of every entry on disk. The reverse order could leave
        // a recovered node holding an entry from a term it does not believe in
        // yet, and it would then happily vote in that term again.
        let mut all: Vec<Output> = self.hard_state.into_iter().collect();
        all.append(&mut self.persists);
        all.append(&mut self.sends);
        all.append(&mut self.effects);
        all
    }
}

// ---------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------

/// A read-only request waiting to be served (§6.4).
#[derive(Clone, Debug)]
struct PendingRead {
    request: ClientRequest,
    /// The commit index when the request arrived. Only used by the
    /// `read_index_at_arrival` regression switch; the correct path ignores it.
    arrival_index: Index,
    /// The commit index this read must see, assigned only once the leader
    /// actually knows what is committed.
    ///
    /// NOT captured when the request arrives. A leader that has just been
    /// elected — or has just restarted — has `commitIndex` 0 until an entry of
    /// its own term commits, and a read pinned to 0 requires nothing of the
    /// state machine at all. That is exactly the bug the fuzzer found: a node
    /// restarted, took a read with `read_index = 0`, and answered it from an
    /// empty store before replaying a single entry.
    read_index: Option<Index>,
    /// The confirmation round this read is waiting on, started at the same
    /// moment `read_index` is assigned.
    round: Option<u64>,
    /// Who has acknowledged that round.
    acks: BTreeSet<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Deliberately broken behaviour, for validating the invariant checkers.
///
/// WHY THIS EXISTS: a checker that always says "OK" passes every test until the
/// day it matters. The only way to know the checkers work is to break the
/// algorithm on purpose and watch them fire. Every switch here reproduces a
/// specific, real, historically-common Raft bug.
///
/// All default to `false`. Nothing in the normal build path reads these except
/// the two clearly-marked sites below.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BugSwitches {
    /// Break §5.4.2: commit any entry replicated on a majority, regardless of
    /// which term it came from. This is the Figure 8 safety bug — the single
    /// most commonly botched part of Raft.
    pub commit_prior_term_entries: bool,
    /// Break §5.2: ignore `votedFor` and grant a vote to every candidate that
    /// asks in a term. Lets two candidates win the same term.
    pub vote_twice_per_term: bool,
    /// Break §5.3: take `leaderCommit` at face value instead of capping it at
    /// the last entry this node actually holds. The follower then marks
    /// entries committed that it has never seen.
    pub trust_leader_commit_blindly: bool,
    /// Break §5.1/§5.2 durability: never write `currentTerm` and `votedFor` to
    /// disk. Invisible until the node crashes, at which point it comes back
    /// believing it is in term 0 having voted for nobody — free to vote a
    /// second time in a term it has already voted in.
    pub skip_hard_state_persistence: bool,

    // --- regressions -----------------------------------------------------
    //
    // The three below are not hypotheticals. Each one is a bug that was
    // actually in this implementation, that the fuzzer found, and that is kept
    // switchable so the writeup can link to a live reproduction instead of
    // asking you to take its word for it.
    /// §7. Treat index 0 as the "before the log" anchor even after compaction.
    ///
    /// A leader probing at `prevLogIndex = 0` then passes the consistency check
    /// against a follower that compacted long ago, and the follower appends
    /// entries 1.. onto a log that begins at, say, 90. Found on seed 11278;
    /// minimizes to zero faults.
    pub compaction_anchor_at_zero: bool,
    /// §7. Skip reconciling the durable log against the durable snapshot on
    /// recovery.
    ///
    /// They are separate artifacts and a crash can land between writing them.
    /// Without the fix-up, a node can come back with a snapshot at index 120
    /// and a log ending at 119 — `commitIndex` past the end of its own log.
    /// Found on seed 2367, and only reachable with torn steps.
    pub skip_snapshot_reconcile: bool,
    /// §6.4. The pair of read-path mistakes that shipped together: capture a
    /// read's index when the request arrives rather than when the leader
    /// actually knows what is committed, AND resolve reads before applying
    /// entries rather than after.
    ///
    /// One switch, because it took both to fail. A node that has just restarted
    /// has `commitIndex` 0, so a read pinned to 0 requires nothing of the state
    /// machine — and resolving before applying then emits the answer ahead of
    /// the entries that would have brought the store up to date. Either fix
    /// alone hides it, which is why re-introducing only the first one found
    /// nothing across 3,000 seeds.
    ///
    /// Caught by the linearizability checker with every Raft invariant still
    /// holding.
    pub read_index_at_arrival: bool,
}

/// Timing parameters, in virtual ticks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Election timeout is drawn uniformly from `[min, max)`.
    ///
    /// The spread is what resolves split votes (§5.2): with identical timeouts
    /// a symmetric cluster can re-tie forever. The randomness comes from the
    /// per-node PRNG seeded by the simulator, so it is still perfectly
    /// reproducible.
    pub election_timeout_min: Tick,
    pub election_timeout_max: Tick,
    /// Must be comfortably shorter than `election_timeout_min`, or followers
    /// time out before the leader's heartbeat arrives and the cluster churns.
    pub heartbeat_interval: Tick,
    /// Cap on entries per AppendEntries, so one message cannot carry an
    /// unbounded log.
    pub max_entries_per_append: usize,
    /// Deliberate bugs, for checker validation. All off by default.
    pub bugs: BugSwitches,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            max_entries_per_append: 64,
            bugs: BugSwitches::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RaftNode {
    id: NodeId,
    cluster: ClusterConfig,
    cfg: RaftConfig,

    // --- persistent state (§5.1, Figure 2) -------------------------------
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,

    // --- volatile state --------------------------------------------------
    /// The most recent snapshot, kept so the leader can ship it to a follower
    /// whose entries have already been compacted away.
    snapshot: Option<Snapshot>,

    /// The membership in force below the log — from the snapshot, or whatever
    /// the node was started with.
    base_config: ClusterConfig,
    /// Index of the log entry that produced the effective configuration, or the
    /// snapshot boundary when it came from `base_config`.
    config_index: Index,

    role: Role,
    commit_index: Index,
    last_applied: Index,
    leader_id: Option<NodeId>,

    // --- volatile state on candidates ------------------------------------
    votes_granted: BTreeSet<NodeId>,

    // --- ReadIndex (§6.4) -------------------------------------------------
    /// Reads waiting for leadership confirmation and for the state machine to
    /// catch up.
    pending_reads: Vec<PendingRead>,
    /// Monotonic id for heartbeat confirmation rounds.
    read_round: u64,

    // --- volatile state on leaders ---------------------------------------
    // BTreeMap, never HashMap: these are iterated to decide the commit index
    // and to pick send order, so a randomly-seeded hasher here would make the
    // same seed produce different runs.
    next_index: BTreeMap<NodeId, Index>,
    match_index: BTreeMap<NodeId, Index>,

    // --- timers, in virtual ticks ----------------------------------------
    now: Tick,
    /// When this node last had contact with a leader it believes in.
    ///
    /// Used for the §6 disruption guard below. A leader keeps this current
    /// itself: being the leader and heartbeating is contact.
    last_heard_from_leader: Tick,
    election_deadline: Tick,
    heartbeat_deadline: Tick,

    /// Set whenever `current_term` or `voted_for` changes during a step, so
    /// the step emits exactly one `HardState` persist at the end.
    hard_state_dirty: bool,

    /// Election-timeout randomness. Seeded by the simulator from the run seed,
    /// so it is deterministic and replayable; the node never reaches for the
    /// operating system.
    rng: Rng,
}

impl RaftNode {
    pub fn new(id: NodeId, cluster: ClusterConfig, cfg: RaftConfig, seed: u64, now: Tick) -> Self {
        assert!(
            cfg.election_timeout_max > cfg.election_timeout_min,
            "election timeout range must be non-empty"
        );
        let cluster_for_base = cluster.clone();
        let mut node = RaftNode {
            id,
            cluster,
            cfg,
            current_term: 0,
            voted_for: None,
            log: Log::new(),
            snapshot: None,
            base_config: cluster_for_base,
            config_index: 0,
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            votes_granted: BTreeSet::new(),
            pending_reads: Vec::new(),
            read_round: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            now,
            last_heard_from_leader: 0,
            election_deadline: 0,
            heartbeat_deadline: 0,
            hard_state_dirty: false,
            rng: Rng::derive(seed, id as u64),
        };
        node.log
            .set_zero_anchor_bug(node.cfg.bugs.compaction_anchor_at_zero);
        node.reset_election_deadline();
        node
    }

    /// Rebuild a node from what survived a crash.
    ///
    /// Figure 2 splits the state precisely, and this is where that split gets
    /// tested: `currentTerm`, `votedFor` and the log are persistent and come
    /// back; `commitIndex`, `lastApplied`, the role, and every leader-side index
    /// are volatile and start again from nothing.
    ///
    /// In particular `commitIndex` returns to 0 even though entries genuinely
    /// are committed. That is correct — the node relearns what is committed
    /// from the leader — and it means a recovering node re-applies its whole
    /// log to the state machine, which is exactly how a real implementation
    /// rebuilds a volatile state machine after a restart.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        id: NodeId,
        cluster: ClusterConfig,
        cfg: RaftConfig,
        seed: u64,
        now: Tick,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: Log,
        snapshot: Option<Snapshot>,
    ) -> Self {
        let mut node = RaftNode::new(id, cluster, cfg, seed, now);
        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;

        if let Some(snap) = &snapshot {
            // RECONCILE THE TWO DURABLE ARTIFACTS.
            //
            // The snapshot and the log are written separately, so a crash can
            // land between them and they can disagree. The fuzzer found this
            // (seed 2761): a follower persisted a snapshot at index 120 and
            // died before the log reset that should have followed, so recovery
            // paired a snapshot at 120 with a log ending at 119 and came back
            // with `commitIndex` past the end of its own log.
            //
            // The rule is the same one `on_install_snapshot` uses: if the log
            // can confirm the snapshot's boundary entry, everything after it
            // agrees and is worth keeping; if it cannot, the log is stale or
            // describes a history that never happened, and the snapshot wins.
            // DELIBERATE REGRESSION (off by default): skipping this leaves the
            // recovered log and the recovered snapshot free to disagree.
            if !node.cfg.bugs.skip_snapshot_reconcile {
                node.log.reconcile_with_snapshot(snap.last_included);
            }
            node.base_config = snap.config.clone();

            // With a snapshot, `commitIndex` and `lastApplied` restart at its
            // index rather than at 0. Everything it covers is by construction
            // committed and already applied -- the entries are gone, so they
            // could not be replayed even if the node wanted to.
            node.commit_index = snap.index();
            node.last_applied = snap.index();
        }
        node.snapshot = snapshot;
        // The configuration is reconstructed from the recovered log, falling
        // back to the snapshot's. It is not part of the hard state.
        node.refresh_config();
        node
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    // --- read-only accessors, for the simulator and the visualizer -------

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    pub fn commit_index(&self) -> Index {
        self.commit_index
    }
    pub fn last_applied(&self) -> Index {
        self.last_applied
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn log(&self) -> &Log {
        &self.log
    }
    pub fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// Index of the entry that produced the effective configuration.
    pub fn config_index(&self) -> Index {
        self.config_index
    }

    pub fn is_joint(&self) -> bool {
        self.cluster.is_joint()
    }

    /// Reads waiting on leadership confirmation.
    pub fn pending_reads(&self) -> usize {
        self.pending_reads.len()
    }
    pub fn match_index(&self) -> &BTreeMap<NodeId, Index> {
        &self.match_index
    }
    pub fn next_index(&self) -> &BTreeMap<NodeId, Index> {
        &self.next_index
    }

    /// The next virtual time at which this node has something to do.
    ///
    /// The simulator uses this to schedule one timer event per node instead of
    /// delivering a tick to every node on every tick. That is a pure
    /// performance decision and does not change behaviour: `Input::Tick` is a
    /// no-op when no deadline has been reached.
    pub fn next_deadline(&self) -> Tick {
        match self.role {
            Role::Leader => self.heartbeat_deadline,
            Role::Follower | Role::Candidate => self.election_deadline,
        }
    }

    // --- the one entry point ---------------------------------------------

    pub fn step(&mut self, input: Input, now: Tick) -> Vec<Output> {
        debug_assert!(now >= self.now, "virtual time must not go backwards");
        self.now = now;
        let mut out = Outputs::default();

        match input {
            Input::Tick => self.on_tick(&mut out),
            Input::Message { from, msg } => self.on_message(from, msg, &mut out),
            Input::ClientRequest(req) => self.on_client_request(req, &mut out),
            Input::Compact(snapshot) => self.on_compact(snapshot, &mut out),
            Input::ChangeMembership(voters) => self.on_change_membership(voters, &mut out),
        }

        // §6 housekeeping that depends on what just committed: leaving a joint
        // configuration, and a departing leader stepping down.
        self.after_commit(&mut out);

        // DELIBERATE REGRESSION (off by default): resolving before applying,
        // which emits a read's answer ahead of the entries that would have
        // brought the store up to date.
        if self.cfg.bugs.read_index_at_arrival {
            self.resolve_reads(&mut out);
        }

        // Applying is driven by commitIndex regardless of which input moved it,
        // so it lives here rather than in each handler.
        self.advance_apply(&mut out);

        // §6.4, and AFTER applying on purpose. Outputs are delivered in order,
        // so a read that becomes ready in this step must be emitted behind the
        // `Apply`s that bring the state machine up to the index it is waiting
        // for. Resolving first would hand the application a read to answer from
        // a store it has not finished updating.
        if !self.cfg.bugs.read_index_at_arrival {
            self.resolve_reads(&mut out);
        }

        // One durable write per step, covering whatever the handlers changed.
        //
        // DELIBERATE BUG (off by default, see `BugSwitches`): with
        // `skip_hard_state_persistence` the write is simply not emitted, which
        // is only observable after a crash.
        if self.hard_state_dirty && !self.cfg.bugs.skip_hard_state_persistence {
            out.hard_state(self.current_term, self.voted_for);
        }
        self.hard_state_dirty = false;
        out.finish()
    }

    // --- timers -----------------------------------------------------------

    fn on_tick(&mut self, out: &mut Outputs) {
        match self.role {
            Role::Leader => {
                if self.now >= self.heartbeat_deadline {
                    self.heartbeat_deadline = self.now + self.cfg.heartbeat_interval;
                    self.last_heard_from_leader = self.now;
                    self.broadcast_append_entries(out);
                }
                // NOTE: a leader that cannot reach a quorum stays leader here.
                // That is the paper's behaviour and it is safe -- it simply
                // cannot commit anything. Stepping down on a lost quorum
                // (CheckQuorum) is only needed once leases back ReadIndex
                // (step 12).
            }
            Role::Follower | Role::Candidate => {
                if self.now >= self.election_deadline {
                    // §5.2: a follower that hears nothing starts an election;
                    // a candidate that times out starts another one, which is
                    // how split votes eventually resolve.
                    self.start_election(out);
                }
            }
        }
    }

    fn reset_election_deadline(&mut self) {
        let timeout = self
            .rng
            .gen_range(self.cfg.election_timeout_min, self.cfg.election_timeout_max);
        self.election_deadline = self.now + timeout;
    }

    // --- role transitions -------------------------------------------------

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.current_term {
            // §5.1: a new term means our vote for the old term is irrelevant
            // and we have not voted in the new one.
            self.current_term = term;
            self.voted_for = None;
            self.hard_state_dirty = true;
            self.reset_election_deadline();
        }
        if self.role != Role::Follower {
            self.role = Role::Follower;
            self.reset_election_deadline();
        }
        self.leader_id = leader;
        self.votes_granted.clear();
        self.next_index.clear();
        self.match_index.clear();
        // A read confirmed by a leadership we no longer have is worthless.
        self.pending_reads.clear();
        // Deliberately no failure responses to clients whose entries we
        // appended while leader. Those entries may still commit under the next
        // leader, so reporting failure would be a lie. The client simply never
        // hears back, which is the PENDING case the linearizability checker has
        // to consider both ways (step 9).
    }

    fn start_election(&mut self, out: &mut Outputs) {
        if !self.cluster.contains(self.id) {
            // A node that is not a voter must never campaign. Reachable once
            // membership changes land (step 11); harmless to guard now.
            self.reset_election_deadline();
            return;
        }

        // §5.2: increment term, vote for self, reset timer, request votes.
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes_granted.clear();
        self.votes_granted.insert(self.id);
        self.hard_state_dirty = true;
        self.reset_election_deadline();

        let last = self.log.last_log_id();
        for peer in self.cluster.voters() {
            if peer == self.id {
                continue;
            }
            out.send(
                peer,
                RaftMessage::RequestVote(RequestVoteReq {
                    term: self.current_term,
                    candidate_id: self.id,
                    last_log_index: last.index,
                    last_log_term: last.term,
                }),
            );
        }

        // A single-node cluster is its own majority and wins immediately.
        if self.cluster.is_quorum(&self.votes_granted) {
            self.become_leader(out);
        }
    }

    fn become_leader(&mut self, out: &mut Outputs) {
        debug_assert_eq!(self.role, Role::Candidate, "only candidates win elections");
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.votes_granted.clear();

        // §5.3: optimistically assume every follower matches us, and discover
        // the truth by probing backwards on rejection.
        let next = self.log.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for peer in self.cluster.voters() {
            if peer == self.id {
                continue;
            }
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }

        // §5.4.2: append a no-op entry of our own term.
        //
        // WHY: the commit rule forbids committing an entry from an earlier term
        // by counting replicas. A leader that inherits uncommitted entries from
        // a previous term therefore cannot advance commitIndex at all until an
        // entry of its own term commits. The no-op provides that entry
        // immediately, and committing it commits everything before it
        // indirectly (which is safe, because Log Matching guarantees those
        // entries are identical wherever they appear).
        let entry = LogEntry {
            term: self.current_term,
            index: self.log.last_index() + 1,
            payload: EntryPayload::Noop,
            client: None,
        };
        self.log.append(std::slice::from_ref(&entry));
        out.persist(PersistOp::Append(vec![entry]));

        self.heartbeat_deadline = self.now + self.cfg.heartbeat_interval;
        self.last_heard_from_leader = self.now;
        self.broadcast_append_entries(out);
        // Covers the single-node case, where our own append is already a quorum.
        self.maybe_advance_commit();
    }

    // --- message dispatch -------------------------------------------------

    /// §6: ignore a vote request from a server that a live leader has not
    /// heard about.
    ///
    /// "If a server receives a RequestVote RPC within the minimum election
    /// timeout of hearing from a current leader, it does not update its term or
    /// grant its vote."
    ///
    /// WHY THIS IS NOT OPTIONAL ONCE MEMBERSHIP CAN CHANGE: a server removed by
    /// a configuration change stops receiving heartbeats but does not
    /// necessarily learn it was removed -- it may still hold only the joint
    /// entry, which includes it. It times out forever, and every RequestVote it
    /// sends carries a higher term, which under §5.1 deposes a perfectly
    /// healthy leader. Without this guard a single stranded server drives the
    /// term into the hundreds and the cluster never settles. That is exactly
    /// what happened here the first time membership changes ran end to end.
    ///
    /// A candidate is deliberately not protected: its own election timeout has
    /// already elapsed, so by definition it has not heard from a leader
    /// recently.
    fn ignores_vote_requests(&self) -> bool {
        self.leader_id.is_some()
            && self.role != Role::Candidate
            && self.now < self.last_heard_from_leader + self.cfg.election_timeout_min
    }

    fn on_message(&mut self, from: NodeId, msg: RaftMessage, out: &mut Outputs) {
        if matches!(msg, RaftMessage::RequestVote(_)) && self.ignores_vote_requests() {
            // Not even a reply: answering with our lower term would tell the
            // disrupting server nothing it can use, and staying silent is what
            // the paper prescribes.
            return;
        }

        let msg_term = msg.term();

        // §5.1: "If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower." This single rule is what
        // keeps at most one leader per term functioning.
        if msg_term > self.current_term {
            let leader = match &msg {
                // An AppendEntries from a higher term identifies the leader of
                // that term. A RequestVote does not -- the candidate has not
                // won anything yet.
                RaftMessage::AppendEntries(m) => Some(m.leader_id),
                _ => None,
            };
            self.become_follower(msg_term, leader);
        }

        // §5.1: reject requests from stale terms, and drop stale responses.
        if msg_term < self.current_term {
            self.reject_stale(from, &msg, out);
            return;
        }

        // Past this point msg_term == self.current_term. Every (role, message)
        // pair is listed explicitly; there is no catch-all arm, so adding a
        // message type or a role fails to compile until it is handled.
        match (self.role, msg) {
            // Any role can be asked for a vote.
            (_, RaftMessage::RequestVote(req)) => self.on_request_vote(from, req, out),

            (Role::Follower, RaftMessage::AppendEntries(req)) => {
                self.on_append_entries(from, req, out)
            }
            (Role::Candidate, RaftMessage::AppendEntries(req)) => {
                // §5.2: another server has won this term. Concede and process
                // the entries -- it is a valid leader.
                let leader = req.leader_id;
                self.become_follower(self.current_term, Some(leader));
                self.on_append_entries(from, req, out);
            }
            (Role::Leader, RaftMessage::AppendEntries(req)) => {
                // Two leaders in the same term violates Election Safety (§5.2),
                // which quorum intersection makes impossible -- so reaching
                // here means vote counting or vote persistence is broken.
                //
                // Deliberately NOT a panic. The invariant checker reports this
                // with the nodes, the term and the likely cause, and a fuzzer
                // that gets a violation plus a minimized repro is far more
                // useful than one that gets an abort. Concede and carry on so
                // the run stays observable.
                let leader = req.leader_id;
                self.become_follower(self.current_term, Some(leader));
                self.on_append_entries(from, req, out);
            }

            (Role::Follower, RaftMessage::InstallSnapshot(req)) => {
                self.on_install_snapshot(from, req, out)
            }
            (Role::Candidate, RaftMessage::InstallSnapshot(req)) => {
                // §5.2: someone else has won this term. Concede, then take the
                // snapshot — it comes from a valid leader.
                let leader = req.leader_id;
                self.become_follower(self.current_term, Some(leader));
                self.on_install_snapshot(from, req, out);
            }
            (Role::Leader, RaftMessage::InstallSnapshot(req)) => {
                // Two leaders in one term. As with AppendEntries: not a panic,
                // because the invariant checker reports this far more usefully
                // than an abort would.
                let leader = req.leader_id;
                self.become_follower(self.current_term, Some(leader));
                self.on_install_snapshot(from, req, out);
            }
            (Role::Leader, RaftMessage::InstallSnapshotResp(resp)) => {
                self.on_install_snapshot_resp(from, resp, out)
            }
            (Role::Follower | Role::Candidate, RaftMessage::InstallSnapshotResp(_)) => {
                // A response to a request a previous incarnation of us sent.
            }

            (Role::Candidate, RaftMessage::RequestVoteResp(resp)) => {
                self.on_request_vote_resp(from, resp, out)
            }
            (Role::Follower | Role::Leader, RaftMessage::RequestVoteResp(_)) => {
                // We already resolved this election (won it, or someone else
                // did). A late vote changes nothing. Explicitly ignored.
            }

            (Role::Leader, RaftMessage::AppendEntriesResp(resp)) => {
                self.on_append_entries_resp(from, resp, out)
            }
            (Role::Follower | Role::Candidate, RaftMessage::AppendEntriesResp(_)) => {
                // We are no longer the leader that sent the request (we stepped
                // down within the same term is impossible, so this is a
                // response to a previous incarnation). Nothing to do.
            }
        }
    }

    fn reject_stale(&mut self, from: NodeId, msg: &RaftMessage, out: &mut Outputs) {
        match msg {
            RaftMessage::RequestVote(_) => {
                // Reply so the stale candidate learns the real term and steps
                // down instead of retrying forever.
                out.send(
                    from,
                    RaftMessage::RequestVoteResp(RequestVoteResp {
                        term: self.current_term,
                        vote_granted: false,
                    }),
                );
            }
            RaftMessage::AppendEntries(req) => {
                // Same, for a deposed leader that has not noticed yet.
                out.send(
                    from,
                    RaftMessage::AppendEntriesResp(AppendEntriesResp {
                        term: self.current_term,
                        success: false,
                        match_index: 0,
                        conflict: None,
                        probed_index: req.prev_log_index,
                        read_round: req.read_round,
                    }),
                );
            }
            RaftMessage::InstallSnapshot(req) => {
                // Same again, for a deposed leader shipping a stale snapshot.
                out.send(
                    from,
                    RaftMessage::InstallSnapshotResp(InstallSnapshotResp {
                        term: self.current_term,
                        last_included_index: req.last_included_index,
                    }),
                );
            }
            RaftMessage::RequestVoteResp(_)
            | RaftMessage::AppendEntriesResp(_)
            | RaftMessage::InstallSnapshotResp(_) => {
                // A response to a request we sent in an earlier term. There is
                // nothing to answer and acting on it would be acting on stale
                // information. Dropped, explicitly.
            }
        }
    }

    // --- RequestVote ------------------------------------------------------

    fn on_request_vote(&mut self, from: NodeId, req: RequestVoteReq, out: &mut Outputs) {
        // Terms are equal here; higher and lower were handled by the caller.
        let candidate_last = LogId::new(req.last_log_index, req.last_log_term);

        // §5.2: one vote per term. Re-granting to the *same* candidate makes
        // RequestVote idempotent, so a duplicated request cannot cost a
        // candidate a vote it has already won.
        let can_vote = match self.voted_for {
            None => true,
            // DELIBERATE BUG (off by default, see `BugSwitches`): with the
            // switch on this node votes for anyone who asks, so two candidates
            // can each collect a majority in the same term.
            Some(v) => v == req.candidate_id || self.cfg.bugs.vote_twice_per_term,
        };

        // §5.4.1 election restriction. This is the check that makes Leader
        // Completeness hold: a majority of voters refuse anyone whose log could
        // be missing a committed entry, and any winner needs a majority, so
        // every leader's log contains every committed entry.
        let up_to_date = candidate_last.at_least_as_up_to_date_as(&self.log.last_log_id());

        let granted = can_vote && up_to_date && self.cluster.contains(req.candidate_id);

        if granted {
            self.voted_for = Some(req.candidate_id);
            // Persisted before the reply leaves (see the ordering contract on
            // `Output`): granting a vote, crashing, and forgetting it would let
            // us vote twice in one term and elect two leaders.
            self.hard_state_dirty = true;
            // §5.2: reset the timer only when a vote is actually granted.
            // Resetting on every RequestVote would let a node with a hopelessly
            // stale log suppress elections indefinitely just by asking.
            self.reset_election_deadline();
        }

        out.send(
            from,
            RaftMessage::RequestVoteResp(RequestVoteResp {
                term: self.current_term,
                vote_granted: granted,
            }),
        );
    }

    fn on_request_vote_resp(&mut self, from: NodeId, resp: RequestVoteResp, out: &mut Outputs) {
        if !resp.vote_granted {
            // A denial carries no new information at an equal term. Ignored.
            return;
        }
        // A set, so a duplicated response cannot be counted twice -- which
        // would otherwise let a single voter manufacture a quorum.
        self.votes_granted.insert(from);
        if self.cluster.is_quorum(&self.votes_granted) {
            self.become_leader(out);
        }
    }

    // --- AppendEntries ----------------------------------------------------

    fn broadcast_append_entries(&mut self, out: &mut Outputs) {
        self.broadcast_for(0, out)
    }

    fn broadcast_for(&mut self, read_round: u64, out: &mut Outputs) {
        for peer in self.cluster.voters() {
            if peer == self.id {
                continue;
            }
            self.send_append_entries_for(peer, read_round, out);
        }
    }

    fn send_append_entries(&mut self, to: NodeId, out: &mut Outputs) {
        self.send_append_entries_for(to, 0, out)
    }

    fn send_append_entries_for(&mut self, to: NodeId, read_round: u64, out: &mut Outputs) {
        let next = self
            .next_index
            .get(&to)
            .copied()
            .unwrap_or(self.log.last_index() + 1);
        let prev_log_index = next.saturating_sub(1);
        let Some(prev_log_term) = self.log.term_at(prev_log_index) else {
            // The entry this follower needs has been compacted away, so there
            // is nothing to describe the gap with. Ship the snapshot instead
            // (§7).
            self.send_snapshot(to, out);
            return;
        };
        let entries = self.log.entries_from(next, self.cfg.max_entries_per_append);
        out.send(
            to,
            RaftMessage::AppendEntries(AppendEntriesReq {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
                read_round,
            }),
        );
    }

    fn on_append_entries(&mut self, from: NodeId, req: AppendEntriesReq, out: &mut Outputs) {
        debug_assert_eq!(
            self.role,
            Role::Follower,
            "callers convert to follower before processing entries"
        );
        self.leader_id = Some(req.leader_id);
        self.last_heard_from_leader = self.now;
        // We heard from the current term's leader, so we are not starting an
        // election (§5.2).
        self.reset_election_deadline();

        // §5.3 consistency check: we must already hold `prev_log_index` with
        // `prev_log_term`. This is the induction step of the Log Matching
        // Property -- if it holds for the entry before, appending keeps every
        // preceding entry identical to the leader's.
        if self.log.term_at(req.prev_log_index) != Some(req.prev_log_term) {
            let hint = self.log.conflict_hint(req.prev_log_index);
            out.send(
                from,
                RaftMessage::AppendEntriesResp(AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                    conflict: Some(hint),
                    probed_index: req.prev_log_index,
                    read_round: req.read_round,
                }),
            );
            return;
        }

        // Walk the incoming entries and only touch the log where it genuinely
        // disagrees.
        //
        // WHY NOT JUST TRUNCATE AND APPEND: this message may be a delayed
        // duplicate of one we already applied, carrying entries we have since
        // extended past. Truncating unconditionally would delete -- possibly
        // committed -- entries that the leader still has. Truncate only at the
        // first index where the terms actually differ (§5.3).
        let mut agreed_upto = req.prev_log_index;
        for (i, entry) in req.entries.iter().enumerate() {
            match self.log.term_at(entry.index) {
                Some(t) if t == entry.term => {
                    // Already have exactly this entry; Log Matching says the
                    // payload is identical too.
                    agreed_upto = entry.index;
                }
                Some(_) => {
                    // Genuine conflict. Delete from here on and take the
                    // leader's version.
                    self.log.truncate_from(entry.index);
                    out.persist(PersistOp::TruncateFrom(entry.index));
                    // The configuration in force may have just been truncated
                    // away; fall back to whatever now stands.
                    if self.config_index >= entry.index {
                        self.refresh_config();
                    }
                    let rest = &req.entries[i..];
                    self.log.append(rest);
                    out.persist(PersistOp::Append(rest.to_vec()));
                    self.adopt_configs(rest);
                    agreed_upto = self.log.last_index();
                    break;
                }
                None => {
                    // Past the end of our log: pure append.
                    let rest = &req.entries[i..];
                    self.log.append(rest);
                    out.persist(PersistOp::Append(rest.to_vec()));
                    self.adopt_configs(rest);
                    agreed_upto = self.log.last_index();
                    break;
                }
            }
        }

        // §5.3: commitIndex = min(leaderCommit, index of last new entry).
        //
        // Capping at `agreed_upto` rather than our own last index matters: the
        // leader may have committed entries we have not received, and marking
        // an index committed that we do not hold would let us apply the wrong
        // entry there later.
        if req.leader_commit > self.commit_index {
            // DELIBERATE BUG (off by default, see `BugSwitches`): with the
            // switch on, the cap is dropped and the follower believes it has
            // committed entries it does not hold.
            let capped = if self.cfg.bugs.trust_leader_commit_blindly {
                req.leader_commit
            } else {
                req.leader_commit.min(agreed_upto)
            };
            self.commit_index = capped.max(self.commit_index);
        }

        out.send(
            from,
            RaftMessage::AppendEntriesResp(AppendEntriesResp {
                term: self.current_term,
                success: true,
                match_index: agreed_upto,
                conflict: None,
                probed_index: req.prev_log_index,
                // Echo the confirmation round: this acknowledgement is what
                // proves the sender was still leader when it asked.
                read_round: req.read_round,
            }),
        );
    }

    fn on_append_entries_resp(&mut self, from: NodeId, resp: AppendEntriesResp, out: &mut Outputs) {
        if !self.cluster.contains(from) {
            // Not a voter: nothing it says affects commitment. Explicitly
            // ignored (relevant once membership changes land).
            return;
        }

        if resp.success {
            self.note_read_ack(from, resp.read_round);
            // `max`, not assignment: responses can arrive out of order or
            // duplicated, and matchIndex must never regress -- it is what the
            // commit rule counts.
            let known = self.match_index.get(&from).copied().unwrap_or(0);
            let m = known.max(resp.match_index);
            self.match_index.insert(from, m);
            self.next_index.insert(from, m + 1);
            self.maybe_advance_commit();

            // Keep streaming if the follower is still behind, rather than
            // waiting for the next heartbeat.
            if m < self.log.last_index() {
                self.send_append_entries(from, out);
            }
            return;
        }

        // Rejected: our prevLogIndex guess was wrong. Back up and retry.
        let next = self
            .next_index
            .get(&from)
            .copied()
            .unwrap_or(self.log.last_index() + 1);

        // Only the rejection answering the probe we are currently outstanding on
        // may move nextIndex. A duplicated or overtaken-by-a-newer-one rejection
        // describes a guess we have already corrected, and acting on it again
        // would walk nextIndex back a second time for the same information.
        if resp.probed_index + 1 != next {
            return;
        }

        // A follower whose log *begins* after our probe has compacted past it.
        // This hint moves nextIndex FORWARD, which the backward-only clamp below
        // would otherwise refuse -- leaving the leader probing at an index the
        // follower can never confirm, forever.
        if let Some(ConflictHint {
            term: None,
            first_index,
        }) = resp.conflict
        {
            if first_index > next {
                self.next_index.insert(from, first_index);
                self.send_append_entries(from, out);
                return;
            }
        }

        let candidate_next = match resp.conflict {
            // §5.3 optimization: skip a whole conflicting term per round trip
            // instead of one entry.
            Some(ConflictHint {
                term: Some(t),
                first_index,
            }) => match self.log.last_index_of_term(t) {
                // We have that term too: everything up to our last entry of it
                // is consistent, so resume just after it.
                Some(idx) => idx + 1,
                // We never had that term: the follower's entire run of it is
                // garbage, so jump before all of it.
                None => first_index,
            },
            // Follower's log is shorter than our probe, or it could not say
            // why: jump to where its log ends.
            Some(ConflictHint {
                term: None,
                first_index,
            }) => first_index,
            None => next.saturating_sub(1),
        };

        // Clamp so a stale or duplicated rejection can only move nextIndex
        // backwards, and never below the first real index. This also
        // guarantees termination: every rejection strictly decreases
        // nextIndex until it bottoms out at 1.
        //
        // FOLLOW-UP (step 5, when the network starts duplicating): a duplicated
        // rejection currently costs one extra probe, because the leader cannot
        // tell it apart from a fresh one. Echoing the probed prevLogIndex in
        // the response and ignoring rejections that do not match the
        // outstanding probe would make rejections idempotent. Safe today --
        // the perfect network never duplicates -- just slower than it needs to
        // be under a lossy one.
        let new_next = candidate_next.clamp(1, next.saturating_sub(1).max(1));
        self.next_index.insert(from, new_next);
        self.send_append_entries(from, out);
    }

    // --- commitment and application ---------------------------------------

    /// Advance `commitIndex` to the highest index replicated on a majority
    /// *that belongs to the current term* (§5.4.2).
    ///
    /// THE SUBTLE ONE. Counting replicas on an entry from a previous term and
    /// calling it committed is unsafe: Figure 8 in the paper shows a sequence
    /// where such an entry is present on a majority and is still subsequently
    /// overwritten by a new leader. Only an entry from the leader's own term
    /// may be committed by replica count; earlier entries become committed
    /// indirectly, when a current-term entry above them commits, and that is
    /// safe because Log Matching makes everything below identical.
    fn maybe_advance_commit(&mut self) {
        let mut n = self.log.last_index();
        while n > self.commit_index {
            let own_term = self.log.term_at(n) == Some(self.current_term);
            // DELIBERATE BUG (off by default, see `BugSwitches`): with the
            // switch on, replica count alone commits an entry, which is exactly
            // the Figure 8 violation.
            if !own_term && !self.cfg.bugs.commit_prior_term_entries {
                // Entries are ordered by term, so once we drop below the
                // current term nothing lower can qualify either.
                break;
            }

            let reached_quorum = {
                let me = self.id;
                let own_last = self.log.last_index();
                let match_index = &self.match_index;
                // During a joint configuration this needs a majority of C_old
                // *and* of C_new, which `is_quorum_by` enforces.
                self.cluster.is_quorum_by(|id| {
                    if id == me {
                        own_last >= n
                    } else {
                        match_index.get(&id).copied().unwrap_or(0) >= n
                    }
                })
            };

            if reached_quorum {
                self.commit_index = n;
                return;
            }
            n -= 1;
        }
    }

    fn advance_apply(&mut self, out: &mut Outputs) {
        while self.last_applied < self.commit_index {
            let index = self.last_applied + 1;
            let Some(entry) = self.log.get(index) else {
                // commitIndex ran past the end of the log, which means the
                // commit rule above is broken. Stop applying rather than
                // panicking: the `CommitIndexWithinLog` checker reports it with
                // a diagnostic, and the run stays observable for the fuzzer.
                return;
            };
            let apply = Output::Apply {
                index,
                term: entry.term,
                payload: entry.payload.clone(),
                client: entry.client,
                // Only a leader answers clients. Followers apply silently.
                respond: self.role == Role::Leader,
            };
            self.last_applied = index;
            out.effect(apply);
        }
    }

    // --- ReadIndex (§6.4) -------------------------------------------------

    /// Begin a linearizable read.
    ///
    /// A leader cannot simply answer from its own state machine. It may have
    /// been deposed minutes ago behind a partition and not know it, in which
    /// case its state is arbitrarily stale — every Raft invariant still holds
    /// and the client is still told a lie. ReadIndex closes that:
    ///
    /// 1. Record `readIndex = commitIndex` *now*.
    /// 2. Confirm we are still the leader, by hearing back from a quorum on a
    ///    fresh heartbeat round. A round id is carried so a delayed
    ///    acknowledgement from an older round -- which proves nothing about the
    ///    present -- cannot be mistaken for confirmation.
    /// 3. Wait for the state machine to reach `readIndex`.
    /// 4. Only then serve.
    ///
    /// Step 1 must also wait until an entry from *this* term has committed,
    /// because until then the leader does not reliably know what is committed
    /// (§5.4.2). The no-op appended on election is what makes that quick.
    fn begin_read(&mut self, request: ClientRequest, out: &mut Outputs) {
        // DELIBERATE REGRESSION (off by default): pinning the index AND
        // starting the confirmation round the moment the request arrives.
        //
        // Starting the round early is the part that makes the failure
        // reachable: acknowledgements pile up while the leader still does not
        // know what is committed, so the instant it finds out, the read is
        // already confirmed and gets served in that same step — ahead of the
        // entries that would have brought the state machine up to date.
        let early = self.cfg.bugs.read_index_at_arrival;
        let mut acks = BTreeSet::new();
        let round = early.then(|| {
            acks.insert(self.id);
            self.read_round += 1;
            self.read_round
        });
        self.pending_reads.push(PendingRead {
            request,
            arrival_index: self.commit_index,
            read_index: early.then_some(self.commit_index),
            round,
            acks,
        });
        if let Some(round) = round {
            self.broadcast_for(round, out);
        }
        self.resolve_reads(out);
    }

    /// Advance every pending read as far as it can go, and serve the ones that
    /// are ready.
    fn resolve_reads(&mut self, out: &mut Outputs) {
        if self.pending_reads.is_empty() {
            return;
        }
        if self.role != Role::Leader {
            // A read confirmed by a leadership we no longer have is worthless.
            // The client hears nothing and retries, which is correct: claiming
            // failure would be a lie and answering would be worse.
            self.pending_reads.clear();
            return;
        }

        // §5.4.2: until an entry of our own term has committed, we do not
        // reliably know what is committed, so there is no honest read index to
        // assign yet. The no-op appended on election makes this brief.
        if self.log.term_at(self.commit_index) != Some(self.current_term) {
            return;
        }

        // Newly eligible reads all take the current commit index and share one
        // confirmation round.
        let mut fresh_round = None;
        let at_arrival = self.cfg.bugs.read_index_at_arrival;
        for read in &mut self.pending_reads {
            if read.read_index.is_none() {
                let round = *fresh_round.get_or_insert_with(|| {
                    self.read_round += 1;
                    self.read_round
                });
                // DELIBERATE REGRESSION (off by default): pinning the read to
                // the commit index it arrived at, which on a freshly restarted
                // node is 0 and therefore no constraint at all.
                read.read_index = Some(if at_arrival {
                    read.arrival_index
                } else {
                    self.commit_index
                });
                read.round = Some(round);
                read.acks.insert(self.id);
            }
        }
        if let Some(round) = fresh_round {
            self.broadcast_for(round, out);
        }

        let (cluster, applied) = (&self.cluster, self.last_applied);
        let mut ready = Vec::new();
        self.pending_reads.retain(|read| {
            let Some(read_index) = read.read_index else {
                return true;
            };
            let confirmed = cluster.is_quorum_by(|id| read.acks.contains(&id));
            if confirmed && applied >= read_index {
                ready.push((read.request.clone(), read_index));
                false
            } else {
                true
            }
        });

        for (request, read_index) in ready {
            out.effect(Output::ServeRead {
                client: request.client,
                seq: request.seq,
                command: request.command,
                read_index,
            });
        }
    }

    /// Record that `from` acknowledged a confirmation round.
    fn note_read_ack(&mut self, from: NodeId, round: u64) {
        if round == 0 {
            return;
        }
        for read in &mut self.pending_reads {
            // A response to round R also confirms every earlier round: the
            // leader was evidently still leading when it sent R.
            if read.round.is_some_and(|r| r <= round) {
                read.acks.insert(from);
            }
        }
    }

    // --- membership (§6) --------------------------------------------------

    /// Adopt any configuration carried by these entries.
    ///
    /// §6, AND THE PART THAT IS EASY TO GET BACKWARDS: a configuration takes
    /// effect the moment its entry is **appended**, not when it commits. A node
    /// must obey a config entry sitting uncommitted in its log.
    ///
    /// The reason is that the entry might commit, and if a node waited for the
    /// commit before counting the new voters, the very quorum needed to commit
    /// it could be one it refuses to count. Obeying on append also means the
    /// configuration can be *un*-adopted if the entry is later truncated away,
    /// which `refresh_config` handles.
    fn adopt_configs(&mut self, entries: &[LogEntry]) {
        for entry in entries {
            if let EntryPayload::Config(cfg) = &entry.payload {
                self.cluster = cfg.clone();
                self.config_index = entry.index;
            }
        }
    }

    /// Recompute the effective configuration by scanning back for the last
    /// config entry. Used after a truncation removed the one in force.
    fn refresh_config(&mut self) {
        let first = self.log.first_index();
        let mut i = self.log.last_index();
        while i >= first && i > 0 {
            if let Some(EntryPayload::Config(cfg)) = self.log.get(i).map(|e| &e.payload) {
                self.cluster = cfg.clone();
                self.config_index = i;
                return;
            }
            i -= 1;
        }
        // Nothing left in the log: fall back to what the snapshot recorded.
        self.cluster = self.base_config.clone();
        self.config_index = self.log.last_included().index;
    }

    /// The configuration in force at `index`, for recording in a snapshot.
    fn config_at(&self, index: Index) -> ClusterConfig {
        let first = self.log.first_index();
        let mut i = index.min(self.log.last_index());
        while i >= first && i > 0 {
            if let Some(EntryPayload::Config(cfg)) = self.log.get(i).map(|e| &e.payload) {
                return cfg.clone();
            }
            i -= 1;
        }
        self.base_config.clone()
    }

    fn on_change_membership(
        &mut self,
        voters: std::collections::BTreeSet<NodeId>,
        out: &mut Outputs,
    ) {
        if self.role != Role::Leader {
            out.effect(Output::ClientResponse {
                client: 0,
                seq: 0,
                result: ClientResult::NotLeader {
                    leader: self.leader_id,
                },
            });
            return;
        }
        // §6 allows one change at a time. Overlapping changes can produce two
        // configurations whose quorums do not intersect, which is precisely
        // what joint consensus exists to rule out. A change is still in flight
        // if we are joint, or if the current configuration is not yet committed.
        if self.cluster.is_joint() || self.config_index > self.commit_index {
            out.effect(Output::ClientResponse {
                client: 0,
                seq: 0,
                result: ClientResult::ChangeInProgress,
            });
            return;
        }
        if voters.is_empty() {
            return;
        }

        let joint = ClusterConfig::joint(self.cluster.old_voters().clone(), voters);
        let entry = LogEntry {
            term: self.current_term,
            index: self.log.last_index() + 1,
            payload: EntryPayload::Config(joint),
            client: None,
        };
        self.log.append(std::slice::from_ref(&entry));
        out.persist(PersistOp::Append(vec![entry.clone()]));
        self.adopt_configs(std::slice::from_ref(&entry));

        // The new voters have to be replicated to immediately -- they are part
        // of every quorum from this moment.
        self.reset_replication_state();
        self.broadcast_append_entries(out);
        self.maybe_advance_commit();
    }

    /// Make sure every voter in the current configuration has replication
    /// state, without disturbing what is already known about the others.
    fn reset_replication_state(&mut self) {
        let next = self.log.last_index() + 1;
        for peer in self.cluster.voters() {
            if peer == self.id {
                continue;
            }
            self.next_index.entry(peer).or_insert(next);
            self.match_index.entry(peer).or_insert(0);
        }
    }

    /// §6 follow-ups that depend on what has just committed.
    fn after_commit(&mut self, out: &mut Outputs) {
        if self.role != Role::Leader {
            return;
        }

        // Once C_old,new is committed, the leader appends C_new. From here the
        // old configuration alone can no longer make decisions.
        if self.cluster.is_joint() && self.config_index <= self.commit_index {
            if let Some(new) = self.cluster.to_new() {
                let entry = LogEntry {
                    term: self.current_term,
                    index: self.log.last_index() + 1,
                    payload: EntryPayload::Config(new),
                    client: None,
                };
                self.log.append(std::slice::from_ref(&entry));
                out.persist(PersistOp::Append(vec![entry.clone()]));
                self.adopt_configs(std::slice::from_ref(&entry));
                self.reset_replication_state();
                self.broadcast_append_entries(out);
                self.maybe_advance_commit();
            }
        }

        // §6: a leader that is not in C_new steps down once C_new is committed.
        // It has to stay leader until then, because it is the one replicating
        // the entry that removes it.
        if !self.cluster.is_joint()
            && self.config_index <= self.commit_index
            && !self.cluster.contains(self.id)
        {
            self.become_follower(self.current_term, None);
        }
    }

    // --- snapshots (§7) ---------------------------------------------------

    fn on_compact(&mut self, mut snapshot: Snapshot, out: &mut Outputs) {
        let through = snapshot.last_included;
        // The caller cannot know the membership -- only Raft tracks it -- so
        // the node fills it in for the boundary it is compacting through.
        snapshot.config = self.config_at(through.index);

        // Never discard an entry the state machine has not absorbed: the
        // snapshot is supposed to *be* those entries' effect, and compacting
        // past `lastApplied` would delete work that never happened.
        if through.index > self.last_applied {
            debug_assert!(
                false,
                "node {} asked to compact through {} but has only applied {}",
                self.id, through.index, self.last_applied
            );
            return;
        }
        if through.index <= self.log.last_included().index {
            // An older snapshot; nothing to do.
            return;
        }
        if self.log.term_at(through.index) != Some(through.term) {
            debug_assert!(
                false,
                "snapshot does not match the log at {}",
                through.index
            );
            return;
        }

        // Snapshot first, then trim. A crash between the two leaves the
        // snapshot saved and the log merely un-trimmed.
        out.persist(PersistOp::Snapshot(snapshot.clone()));
        out.persist(PersistOp::Compact(through));
        self.base_config = snapshot.config.clone();
        self.snapshot = Some(snapshot);
        self.log.compact(through);
    }

    fn send_snapshot(&mut self, to: NodeId, out: &mut Outputs) {
        let Some(snapshot) = &self.snapshot else {
            // The follower needs an entry we no longer hold and we have no
            // snapshot to send. Unreachable: the only thing that discards
            // entries is taking a snapshot.
            debug_assert!(
                false,
                "node {} compacted without keeping a snapshot",
                self.id
            );
            return;
        };
        out.send(
            to,
            RaftMessage::InstallSnapshot(InstallSnapshotReq {
                term: self.current_term,
                leader_id: self.id,
                last_included_index: snapshot.index(),
                last_included_term: snapshot.term(),
                config: snapshot.config.clone(),
                data: snapshot.data.clone(),
            }),
        );
    }

    fn on_install_snapshot(&mut self, from: NodeId, req: InstallSnapshotReq, out: &mut Outputs) {
        self.leader_id = Some(req.leader_id);
        self.last_heard_from_leader = self.now;
        self.reset_election_deadline();
        let through = LogId::new(req.last_included_index, req.last_included_term);

        // Already have everything it covers. Answering with our own watermark
        // rather than the snapshot's keeps the leader from walking us
        // backwards.
        if through.index <= self.commit_index {
            out.send(
                from,
                RaftMessage::InstallSnapshotResp(InstallSnapshotResp {
                    term: self.current_term,
                    last_included_index: self.commit_index,
                }),
            );
            return;
        }

        let snapshot = Snapshot::new(through, req.config, req.data);
        out.persist(PersistOp::Snapshot(snapshot.clone()));

        // §7: if we hold a matching entry at the boundary, everything after it
        // agrees with the leader by Log Matching and is worth keeping. If not,
        // our tail describes a history that never happened and goes entirely.
        if self.log.term_at(through.index) == Some(through.term) {
            self.log.compact(through);
            out.persist(PersistOp::Compact(through));
        } else {
            self.log.reset_to(through);
            out.persist(PersistOp::ResetLog(through));
        }

        // Everything below the boundary is gone, so the snapshot's membership
        // becomes the floor the log's config entries build on.
        self.base_config = snapshot.config.clone();
        self.refresh_config();
        self.snapshot = Some(snapshot.clone());
        self.commit_index = self.commit_index.max(through.index);
        // The snapshot *is* the state machine at this point, so the entries it
        // covers must never be applied again.
        self.last_applied = self.last_applied.max(through.index);
        out.effect(Output::RestoreSnapshot { snapshot });

        out.send(
            from,
            RaftMessage::InstallSnapshotResp(InstallSnapshotResp {
                term: self.current_term,
                last_included_index: through.index,
            }),
        );
    }

    fn on_install_snapshot_resp(
        &mut self,
        from: NodeId,
        resp: InstallSnapshotResp,
        out: &mut Outputs,
    ) {
        if !self.cluster.contains(from) {
            return;
        }
        // `max`, as with AppendEntries: a duplicated or reordered response must
        // never pull matchIndex backwards.
        let known = self.match_index.get(&from).copied().unwrap_or(0);
        let m = known.max(resp.last_included_index);
        self.match_index.insert(from, m);
        self.next_index.insert(from, m + 1);
        self.maybe_advance_commit();
        if m < self.log.last_index() {
            self.send_append_entries(from, out);
        }
    }

    // --- clients ----------------------------------------------------------

    fn on_client_request(&mut self, req: ClientRequest, out: &mut Outputs) {
        if self.role != Role::Leader {
            // §8: redirect. The hint may be stale; that is the client's problem
            // to retry, and retries are why sequence numbers exist.
            out.effect(Output::ClientResponse {
                client: req.client,
                seq: req.seq,
                result: ClientResult::NotLeader {
                    leader: self.leader_id,
                },
            });
            return;
        }

        if req.read_only {
            // Reads never enter the log (§6.4). That is the entire point: a
            // linearizable read costs one round of heartbeats instead of a full
            // replication round trip.
            self.begin_read(req, out);
            return;
        }

        let entry = LogEntry {
            term: self.current_term,
            index: self.log.last_index() + 1,
            payload: EntryPayload::Command(req.command),
            client: Some((req.client, req.seq)),
        };
        self.log.append(std::slice::from_ref(&entry));
        out.persist(PersistOp::Append(vec![entry]));

        self.broadcast_append_entries(out);
        // Single-node clusters commit on their own append.
        self.maybe_advance_commit();
    }
}

impl RaftNode {
    /// Convenience for tests and the simulator: the last log position.
    pub fn last_log_id(&self) -> LogId {
        self.log.last_log_id()
    }

    /// Build a command from bytes without going through the kvstore. Test aid.
    pub fn command(bytes: impl Into<Vec<u8>>) -> Command {
        Command(bytes.into())
    }
}
