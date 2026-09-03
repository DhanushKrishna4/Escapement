//! Raft's safety properties, checked continuously during a run.
//!
//! These are the cheap checks. They run after every single event, they catch
//! most bugs long before a linearizability check would, and — the whole point —
//! they say *which* property broke and *why*, instead of "this history is not
//! linearizable".
//!
//! Everything here is incremental. A node's log only changes where it was
//! appended or truncated, so the simulator passes a `dirty_from` hint and the
//! scan touches only those entries. Per event the cost is O(entries changed),
//! which is what makes "every tick" affordable at millions of events per
//! second.
//!
//! The checkers are validated against deliberately broken nodes — see
//! `RaftConfig::bugs` and `tests/checker_validation.rs`. A checker that always
//! says OK passes every test until it matters.

use std::collections::BTreeMap;
use std::fmt;

use kvstore::KvCommand;
use raft::{EntryPayload, Index, LogEntry, NodeId, RaftNode, Role, Term, Tick};
use serde::{Deserialize, Serialize};

/// Cheap content hash of a log entry, so two nodes' entries can be compared
/// without storing every payload twice.
pub type Digest = u64;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Content digest of an entry, excluding its index and term — those are
/// compared directly, and keeping them out means the digest answers exactly the
/// question "is this the same entry?".
pub fn entry_digest(e: &LogEntry) -> Digest {
    let mut h = FNV_OFFSET;
    match &e.payload {
        EntryPayload::Noop => h = fnv(h, &[0]),
        EntryPayload::Command(c) => {
            h = fnv(h, &[1]);
            h = fnv(h, &c.0);
        }
        EntryPayload::Config(cfg) => {
            h = fnv(h, &[2]);
            h = fnv(h, cfg.describe().as_bytes());
        }
    }
    if let Some((client, seq)) = e.client {
        h = fnv(h, &client.to_le_bytes());
        h = fnv(h, &seq.to_le_bytes());
    }
    h
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Invariant {
    ElectionSafety,
    LogMatching,
    LeaderCompleteness,
    StateMachineSafety,
    CommittedEntriesStable,
    CommittedPrefixNeverTruncated,
    CommitIndexMonotonic,
    CommitIndexWithinLog,
    AppliedInOrder,
    SingleVotePerTerm,
    TermMonotonic,
}

impl Invariant {
    pub fn name(self) -> &'static str {
        match self {
            Invariant::ElectionSafety => "Election Safety",
            Invariant::LogMatching => "Log Matching",
            Invariant::LeaderCompleteness => "Leader Completeness",
            Invariant::StateMachineSafety => "State Machine Safety",
            Invariant::CommittedEntriesStable => "Committed Entries Are Stable",
            Invariant::CommittedPrefixNeverTruncated => "Committed Prefix Never Truncated",
            Invariant::CommitIndexMonotonic => "Commit Index Monotonic",
            Invariant::CommitIndexWithinLog => "Commit Index Within Log",
            Invariant::AppliedInOrder => "Applied In Order",
            Invariant::SingleVotePerTerm => "Single Vote Per Term",
            Invariant::TermMonotonic => "Term Monotonic",
        }
    }

    pub fn paper_ref(self) -> &'static str {
        match self {
            Invariant::ElectionSafety => "§5.2",
            Invariant::LogMatching => "§5.3",
            Invariant::LeaderCompleteness => "§5.4",
            Invariant::StateMachineSafety => "§5.4.3",
            Invariant::CommittedEntriesStable => "§5.3",
            Invariant::CommittedPrefixNeverTruncated => "§5.3",
            Invariant::CommitIndexMonotonic => "Figure 2",
            Invariant::CommitIndexWithinLog => "Figure 2",
            Invariant::AppliedInOrder => "§5.4.3",
            Invariant::SingleVotePerTerm => "§5.2",
            Invariant::TermMonotonic => "§5.1",
        }
    }

    /// What a violation of this property usually means, so a failure report
    /// points at the code rather than just at the symptom.
    pub fn usual_cause(self) -> &'static str {
        match self {
            Invariant::ElectionSafety => {
                "a quorum voted twice in one term: check the one-vote-per-term rule, \
                 that the vote is persisted before the reply is sent, and that \
                 duplicate vote responses are not counted twice"
            }
            Invariant::LogMatching => {
                "a follower appended without the prevLogIndex/prevLogTerm check passing, \
                 or truncated somewhere other than the first genuinely conflicting entry"
            }
            Invariant::LeaderCompleteness => {
                "the election restriction (§5.4.1) is wrong — most often comparing last \
                 log index before last log term instead of term first"
            }
            Invariant::StateMachineSafety => {
                "usually downstream of a Leader Completeness or commit-rule break; \
                 check §5.4.2 first"
            }
            Invariant::CommittedEntriesStable => {
                "two nodes committed different entries at one index: the classic cause is a \
                 leader committing a previous-term entry by counting replicas (§5.4.2)"
            }
            Invariant::CommittedPrefixNeverTruncated => {
                "a follower discarded entries it had already committed, which means the \
                 prevLogIndex/prevLogTerm check let it accept a conflicting entry below its \
                 commit index"
            }
            Invariant::CommitIndexMonotonic => {
                "commitIndex was assigned rather than advanced — check for a missing max()"
            }
            Invariant::CommitIndexWithinLog => {
                "a follower took leaderCommit without capping it at the last entry it \
                 actually holds"
            }
            Invariant::AppliedInOrder => "the apply loop skipped or repeated an index",
            Invariant::SingleVotePerTerm => {
                "votedFor was not checked, or was lost across a term change"
            }
            Invariant::TermMonotonic => "currentTerm went backwards — check persistence",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    pub tick: Tick,
    pub invariant: Invariant,
    /// The specifics: which nodes, which index, what each of them had.
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[tick {}] {} ({}) violated: {}\n    likely cause: {}",
            self.tick,
            self.invariant.name(),
            self.invariant.paper_ref(),
            self.detail,
            self.invariant.usual_cause()
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogFact {
    prev_term: Term,
    digest: Digest,
    node: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommittedFact {
    /// The term the entry was created in.
    term: Term,
    /// The term it was *committed* in, which is not the same thing.
    ///
    /// §5.4.2 lets a leader commit entries from earlier terms indirectly, by
    /// committing one of its own on top. So an entry created in term 33 can sit
    /// uncommitted through several elections and only become committed in, say,
    /// term 38. Leader Completeness is about the commit term: an entry
    /// committed in term T appears in the logs of leaders of terms *greater
    /// than T*, and a leader of term 37 owes nothing to an entry that was not
    /// committed until 38.
    commit_term: Term,
    digest: Digest,
    node: NodeId,
    tick: Tick,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AppliedFact {
    term: Term,
    command: Option<KvCommand>,
    node: NodeId,
}

/// Accumulated knowledge about the run, and the violations found so far.
#[derive(Clone, Debug, Default)]
pub struct Invariants {
    /// Election Safety: which node was leader of each term.
    leader_of_term: BTreeMap<Term, NodeId>,
    /// Single Vote Per Term: what each node voted, per term.
    vote_of_term: BTreeMap<(NodeId, Term), NodeId>,
    /// Term Monotonic: highest term seen per node.
    max_term: BTreeMap<NodeId, Term>,

    /// Log Matching: for each (index, term) ever observed anywhere, what the
    /// entry was and what preceded it. Two nodes holding the same (index, term)
    /// must agree on both.
    log_facts: BTreeMap<(Index, Term), LogFact>,

    /// Every entry any node has ever considered committed.
    committed: BTreeMap<Index, CommittedFact>,
    /// How far each node's commit index has been checked.
    commit_watermark: BTreeMap<NodeId, Index>,

    /// State Machine Safety: what was applied at each index, by whom.
    applied: BTreeMap<Index, AppliedFact>,
    applied_watermark: BTreeMap<NodeId, Index>,

    /// Terms whose leader has already been checked for Leader Completeness, so
    /// the O(committed) scan happens once per election rather than per event.
    completeness_checked: BTreeMap<Term, NodeId>,

    violations: Vec<Violation>,
    checks: u64,
    restarts: u64,
    snapshots_installed: u64,
}

impl Invariants {
    pub fn new() -> Self {
        Invariants::default()
    }

    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }

    pub fn checks(&self) -> u64 {
        self.checks
    }

    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }

    /// Which invariants are currently violated. For the verification panel's
    /// green/red lights.
    pub fn broken(&self) -> Vec<Invariant> {
        let mut set: Vec<Invariant> = self.violations.iter().map(|v| v.invariant).collect();
        set.sort();
        set.dedup();
        set
    }

    fn fail(&mut self, tick: Tick, invariant: Invariant, detail: String) {
        self.violations.push(Violation {
            tick,
            invariant,
            detail,
        });
    }

    /// A node came back from a crash.
    ///
    /// This is where Figure 2's split between persistent and volatile state
    /// gets enforced, and getting it backwards would make the checker either
    /// blind or wrong:
    ///
    /// * `commitIndex` and `lastApplied` are **volatile** and restart at 0. A
    ///   recovering node genuinely does un-know what was committed and re-apply
    ///   its whole log, so those watermarks must be cleared or every restart
    ///   would look like a violation.
    /// * `currentTerm` and `votedFor` are **persistent**. Their watermarks are
    ///   deliberately NOT cleared: a node that comes back in a lower term, or
    ///   votes again in a term it already voted in, has failed to persist
    ///   something the paper requires, and that is exactly the bug this is
    ///   here to catch.
    /// * Everything global — which entries were committed, what was applied at
    ///   each index, the log facts — is knowledge about the cluster, not about
    ///   the node, and survives untouched.
    ///
    /// `applied_from` is the index a snapshot already accounts for, or 0 when
    /// the node has none. A node that recovers with a snapshot does *not*
    /// replay the entries below it — they are gone — so its watermarks restart
    /// there rather than at zero.
    pub fn note_restart(&mut self, node: NodeId, applied_from: Index) {
        if applied_from == 0 {
            self.commit_watermark.remove(&node);
            self.applied_watermark.remove(&node);
        } else {
            self.commit_watermark.insert(node, applied_from);
            self.applied_watermark.insert(node, applied_from);
        }
        self.restarts += 1;
    }

    /// A node installed a snapshot from the leader, jumping its state machine
    /// forward without applying the entries in between.
    ///
    /// Those entries are never applied on this node, so `AppliedInOrder` has to
    /// expect the sequence to resume after the snapshot rather than to
    /// continue from where it left off.
    pub fn note_snapshot_installed(&mut self, node: NodeId, through: Index) {
        let applied = self.applied_watermark.entry(node).or_insert(0);
        *applied = (*applied).max(through);
        let commit = self.commit_watermark.entry(node).or_insert(0);
        *commit = (*commit).max(through);
        self.snapshots_installed += 1;
    }

    pub fn snapshots_installed(&self) -> u64 {
        self.snapshots_installed
    }

    pub fn restarts(&self) -> u64 {
        self.restarts
    }

    /// Check everything observable about one node after it has taken a step.
    ///
    /// Only the node that stepped can have changed, so this is called for that
    /// node alone. `dirty_from` is the lowest log index the step touched, or
    /// `None` if the log did not change.
    pub fn observe_node(&mut self, tick: Tick, node: &RaftNode, dirty_from: Option<Index>) {
        self.checks += 1;
        let id = node.id();
        let term = node.current_term();

        // --- §5.1: terms never go backwards -------------------------------
        let prev_term = self.max_term.get(&id).copied().unwrap_or(0);
        if term < prev_term {
            self.fail(
                tick,
                Invariant::TermMonotonic,
                format!("node {id} was in term {prev_term} and is now in term {term}"),
            );
        }
        self.max_term.insert(id, term.max(prev_term));

        // --- §5.2: at most one leader per term ----------------------------
        if node.role() == Role::Leader {
            if let Some(other) = self.leader_of_term.insert(term, id) {
                if other != id {
                    self.fail(
                        tick,
                        Invariant::ElectionSafety,
                        format!("nodes {other} and {id} are both leader of term {term}"),
                    );
                }
            }
        }

        // --- §5.2: one vote per term --------------------------------------
        if let Some(candidate) = node.voted_for() {
            if let Some(previous) = self.vote_of_term.insert((id, term), candidate) {
                if previous != candidate {
                    self.fail(
                        tick,
                        Invariant::SingleVotePerTerm,
                        format!(
                            "node {id} voted for {previous} and then for {candidate} in term {term}"
                        ),
                    );
                }
            }
        }

        // --- Figure 2: commitIndex is monotone and within the log ----------
        let commit = node.commit_index();
        let watermark = self.commit_watermark.get(&id).copied().unwrap_or(0);
        if commit < watermark {
            self.fail(
                tick,
                Invariant::CommitIndexMonotonic,
                format!("node {id} commitIndex fell from {watermark} to {commit}"),
            );
        }
        if commit > node.log().last_index() {
            self.fail(
                tick,
                Invariant::CommitIndexWithinLog,
                format!(
                    "node {id} has commitIndex {commit} but its log ends at {}",
                    node.log().last_index()
                ),
            );
        }

        // --- §5.4: a new leader holds every committed entry ---------------
        //
        // Checked once per term, at the first event where we see this node as
        // leader of it. Costs O(entries committed so far) but only per
        // election, which is rare.
        if node.role() == Role::Leader && !self.completeness_checked.contains_key(&term) {
            self.completeness_checked.insert(term, id);
            self.check_leader_completeness(tick, node);
        }

        // --- §5.3: Log Matching over whatever changed ----------------------
        if let Some(from) = dirty_from {
            self.check_log_range(tick, node, from);
        }

        // --- record newly committed entries --------------------------------
        self.record_commits(tick, node, watermark);
    }

    fn check_leader_completeness(&mut self, tick: Tick, node: &RaftNode) {
        let id = node.id();
        let term = node.current_term();
        let log = node.log();
        // Collected first so the loop does not borrow `self` while failing.
        let missing: Vec<String> = self
            .committed
            .iter()
            // Only entries committed in an EARLIER term.
            //
            // §5.4 says an entry committed in term T appears in the logs of
            // leaders of terms *greater than* T. A leader of term 12 is not
            // required to hold an entry from term 13 — that entry did not exist
            // when it was elected, and it is simply a stale leader that has not
            // noticed yet. Omitting this filter made the checker report exactly
            // that as a violation, which is how a 50,000-seed sweep found it.
            .filter(|(_, fact)| fact.commit_term < term)
            // Compare against the term the entry was COMMITTED in, not the term
            // it was created in. A leader elected before the commit happened
            // owes nothing to it, and using the creation term reported exactly
            // that as a violation — a 50,000-seed sweep found a leader of term
            // 37 blamed for entries another node committed in term 38.
            //
            // An index below the start of the log is not missing — it is inside
            // this node's snapshot, which by construction contains it. Without
            // this, every compacted leader looks like a Leader Completeness
            // violation, which is exactly the trap §7 warns about.
            .filter(|(index, _)| !log.is_compacted(**index))
            .filter_map(|(index, fact)| {
                // Entries committed by this very node are trivially present.
                match log.get(*index) {
                    None => Some(format!(
                        "index {index} (term {}, committed by node {}) is absent",
                        fact.term, fact.node
                    )),
                    Some(entry) if entry.term != fact.term => Some(format!(
                        "index {index} should be term {} (committed by node {}) but is term {}",
                        fact.term, fact.node, entry.term
                    )),
                    Some(entry) if entry_digest(entry) != fact.digest => Some(format!(
                        "index {index} term {} has different contents than the entry node {} committed",
                        fact.term, fact.node
                    )),
                    Some(_) => None,
                }
            })
            .take(4)
            .collect();

        if !missing.is_empty() {
            self.fail(
                tick,
                Invariant::LeaderCompleteness,
                format!(
                    "node {id} became leader of term {term} without every committed entry: {}",
                    missing.join("; ")
                ),
            );
        }
    }

    fn check_log_range(&mut self, tick: Tick, node: &RaftNode, from: Index) {
        let id = node.id();
        let log = node.log();
        let start = from.max(log.first_index());
        for index in start..=log.last_index() {
            let Some(entry) = log.get(index) else {
                continue;
            };
            let digest = entry_digest(entry);
            let prev_term = log.term_at(index - 1).unwrap_or(0);

            // Log Matching: same (index, term) anywhere means the same entry,
            // and the same history before it.
            match self.log_facts.get(&(index, entry.term)) {
                Some(fact) if fact.digest != digest => {
                    let other = fact.node;
                    self.fail(
                        tick,
                        Invariant::LogMatching,
                        format!(
                            "nodes {other} and {id} both have an entry at index {index} in term {} \
                             but with different contents",
                            entry.term
                        ),
                    );
                }
                Some(fact) if fact.prev_term != prev_term => {
                    let (other, other_prev) = (fact.node, fact.prev_term);
                    self.fail(
                        tick,
                        Invariant::LogMatching,
                        format!(
                            "nodes {other} and {id} agree at index {index} (term {}) but disagree \
                             about index {}: term {other_prev} vs term {prev_term}",
                            entry.term,
                            index - 1
                        ),
                    );
                }
                Some(_) => {}
                None => {
                    self.log_facts.insert(
                        (index, entry.term),
                        LogFact {
                            prev_term,
                            digest,
                            node: id,
                        },
                    );
                }
            }
        }
        // NOTE: it is tempting to also assert here that a node's entry at a
        // committed index matches what was committed. That check is UNSOUND and
        // was removed after the simulator produced a counterexample.
        //
        // A lagging follower is perfectly entitled to hold a stale entry at an
        // index that a majority has already committed -- it simply has not been
        // caught up yet, and the leader will truncate it on contact. Raft
        // guarantees that a committed entry survives *in the cluster* and
        // appears in every future leader, not that no replica anywhere ever
        // holds an older value. The sound versions of that idea are Leader
        // Completeness (checked when a node becomes leader) and
        // `CommittedPrefixNeverTruncated` (below).
    }

    /// A node is about to discard the entry at `from` and everything after it.
    ///
    /// Truncating at or below the node's own `commitIndex` means it is throwing
    /// away entries it had already concluded were committed, which is a real
    /// loss of committed state. Raft's consistency check makes this impossible:
    /// a follower's committed prefix always matches the leader's, so a conflict
    /// can only ever be found above it.
    ///
    /// Unlike the check this replaced, it is about what a node *does to itself*,
    /// not about how it compares to other nodes, so a lagging replica does not
    /// trip it.
    pub fn observe_truncation(
        &mut self,
        tick: Tick,
        node: NodeId,
        from: Index,
        commit_index: Index,
    ) {
        self.checks += 1;
        if from <= commit_index {
            self.fail(
                tick,
                Invariant::CommittedPrefixNeverTruncated,
                format!(
                    "node {node} truncated from index {from}, discarding entries it had \
                     already committed (its commitIndex was {commit_index})"
                ),
            );
        }
    }

    fn record_commits(&mut self, tick: Tick, node: &RaftNode, watermark: Index) {
        let id = node.id();
        let commit = node.commit_index();
        let log = node.log();
        for index in (watermark + 1)..=commit {
            let Some(entry) = log.get(index) else {
                continue;
            };
            let digest = entry_digest(entry);
            match self.committed.get(&index) {
                Some(fact) if fact.term != entry.term || fact.digest != digest => {
                    let (other, other_term) = (fact.node, fact.term);
                    self.fail(
                        tick,
                        Invariant::CommittedEntriesStable,
                        format!(
                            "node {id} committed a term {} entry at index {index}, but node \
                             {other} had already committed a term {other_term} entry there",
                            entry.term
                        ),
                    );
                }
                Some(fact) => {
                    // Another node reporting the same commitment in an earlier
                    // term tightens the bound, and a tighter bound means fewer
                    // missed detections.
                    if node.current_term() < fact.commit_term {
                        let updated = CommittedFact {
                            commit_term: node.current_term(),
                            ..fact.clone()
                        };
                        self.committed.insert(index, updated);
                    }
                }
                None => {
                    self.committed.insert(
                        index,
                        CommittedFact {
                            term: entry.term,
                            commit_term: node.current_term(),
                            digest,
                            node: id,
                            tick,
                        },
                    );
                }
            }
        }
        self.commit_watermark.insert(id, commit.max(watermark));
    }

    /// Check an entry as it reaches a state machine.
    pub fn observe_apply(
        &mut self,
        tick: Tick,
        node: NodeId,
        index: Index,
        term: Term,
        command: &Option<KvCommand>,
    ) {
        self.checks += 1;

        // Entries reach the state machine one at a time, in order, with no gaps.
        // After a restart the watermark is cleared and the node replays from the
        // beginning, which is correct rather than a gap.
        let expected = self.applied_watermark.get(&node).copied().unwrap_or(0) + 1;
        if index != expected {
            self.fail(
                tick,
                Invariant::AppliedInOrder,
                format!("node {node} applied index {index} when it should have applied {expected}"),
            );
        }
        self.applied_watermark.insert(node, index);

        // State Machine Safety: every node applies the same thing at the same
        // index.
        match self.applied.get(&index) {
            Some(fact) if fact.command != *command || fact.term != term => {
                let detail = format!(
                    "at index {index}, node {} applied {:?} (term {}) but node {node} applied {:?} (term {term})",
                    fact.node, fact.command, fact.term, command
                );
                self.fail(tick, Invariant::StateMachineSafety, detail);
            }
            Some(_) => {}
            None => {
                self.applied.insert(
                    index,
                    AppliedFact {
                        term,
                        command: command.clone(),
                        node,
                    },
                );
            }
        }
    }
}
