//! The replicated log.
//!
//! Stored as a contiguous `Vec` plus the index of its first entry. The offset
//! is here from day one so that snapshotting (step 10) becomes "raise the
//! offset and drop the prefix" rather than a rewrite: after compaction the log
//! still has to answer `term_at(prev_log_index)` for the entry immediately
//! before its first live entry, which is what `last_included` remembers.

use serde::{Deserialize, Serialize};

use crate::{ClientId, Command, Index, LogId, Term};

/// What a log entry carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// The empty entry a new leader appends to its own log on election.
    ///
    /// WHY: §5.4.2 forbids a leader from committing entries from earlier terms
    /// by counting replicas. A leader that inherits uncommitted entries from a
    /// previous term therefore cannot advance `commitIndex` at all until it has
    /// an entry of its *own* term committed. The no-op provides one
    /// immediately, so progress does not have to wait for a client write.
    Noop,
    /// An opaque command for the replicated state machine.
    Command(Command),
    /// A membership change (§6).
    ///
    /// Either C_old,new (joint) or C_new. A node adopts this configuration the
    /// moment the entry is APPENDED, not when it commits — see
    /// `RaftNode::adopt_configs`.
    Config(crate::ClusterConfig),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: Index,
    pub payload: EntryPayload,
    /// Which client request produced this entry, if any.
    ///
    /// Carried in the entry (rather than only in the leader's memory) so that
    /// after a leader change the new leader can still tell the simulator which
    /// request an applied entry corresponds to. A client whose leader died mid
    /// flight gets no response either way -- that is the PENDING case the
    /// linearizability checker must reason about.
    pub client: Option<(ClientId, u64)>,
}

impl LogEntry {
    pub fn log_id(&self) -> LogId {
        LogId::new(self.index, self.term)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Log {
    /// Entries with contiguous indices, `entries[0].index == start_index`.
    entries: Vec<LogEntry>,
    /// Index of the first entry physically present. 1 when nothing has been
    /// compacted away.
    start_index: Index,
    /// The (index, term) of the last entry discarded by compaction; `ZERO`
    /// when nothing has been. Kept so `term_at(start_index - 1)` still works,
    /// which the `prevLogIndex` check needs after truncation.
    last_included: LogId,
    /// DELIBERATE REGRESSION, off by default. See
    /// `BugSwitches::compaction_anchor_at_zero`.
    #[serde(default, skip)]
    zero_anchor_bug: bool,
}

impl Log {
    pub fn new() -> Self {
        Log {
            entries: Vec::new(),
            start_index: 1,
            last_included: LogId::ZERO,
            zero_anchor_bug: false,
        }
    }

    /// Re-introduce the compaction anchor bug, for the writeup's live repro.
    pub fn set_zero_anchor_bug(&mut self, on: bool) {
        self.zero_anchor_bug = on;
    }

    pub fn first_index(&self) -> Index {
        self.start_index
    }

    pub fn last_index(&self) -> Index {
        self.start_index + self.entries.len() as Index - 1
    }

    pub fn last_log_id(&self) -> LogId {
        match self.entries.last() {
            Some(e) => e.log_id(),
            None => self.last_included,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn get(&self, index: Index) -> Option<&LogEntry> {
        if index < self.start_index {
            return None;
        }
        self.entries.get((index - self.start_index) as usize)
    }

    /// Term of the entry at `index`, or `None` if this log cannot answer.
    ///
    /// The anchor is always `last_included`, which is `(0, 0)` for a log that
    /// has never been compacted. That is what makes index 0 answer `Some(0)` on
    /// a fresh log -- the sentinel before the start, so a leader replicating
    /// from scratch passes the `prevLogIndex` check with no special case.
    ///
    /// AFTER COMPACTION, INDEX 0 IS NOT AN ANCHOR ANY MORE, and this is not a
    /// detail. Treating it as one lets a leader probing at `prevLogIndex = 0`
    /// satisfy the consistency check against a follower that compacted long
    /// ago; the follower then appends entries starting at index 1 onto a log
    /// that begins at, say, 9, and its log is silently corrupt. The fuzzer
    /// found exactly that (seed 11278): a node whose slot 10 held an entry
    /// claiming to be index 1. Anchoring on `last_included` instead of on a
    /// hardcoded 0 is the whole fix.
    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.last_included.index {
            return Some(self.last_included.term);
        }
        // DELIBERATE REGRESSION (off by default): index 0 answering as the
        // before-the-log anchor even once the log has been compacted past it.
        if self.zero_anchor_bug && index == 0 {
            return Some(0);
        }
        if index < self.start_index {
            // Compacted away: we genuinely cannot say.
            return None;
        }
        self.get(index).map(|e| e.term)
    }

    /// All entries at `index` and beyond, capped at `max` entries.
    pub fn entries_from(&self, index: Index, max: usize) -> Vec<LogEntry> {
        if index > self.last_index() {
            return Vec::new();
        }
        let start = index.max(self.start_index);
        let offset = (start - self.start_index) as usize;
        let end = (offset + max).min(self.entries.len());
        self.entries[offset..end].to_vec()
    }

    /// Append entries to the end of the log. The caller has already resolved
    /// any conflict; indices must be contiguous with what is present.
    pub fn append(&mut self, entries: &[LogEntry]) {
        for e in entries {
            // The zero-anchor regression is reproduced faithfully, which means
            // reproducing the silence too: when that bug shipped, this check
            // was a `debug_assert` and release builds -- where the fuzzer runs
            // -- skipped it entirely. With the guard on, the corruption plays
            // out and the invariant checker reports it, which is what the
            // writeup's live link needs to show.
            if self.zero_anchor_bug {
                self.entries.push(e.clone());
                continue;
            }
            // A hard assert, not a debug one. A non-contiguous append silently
            // corrupts the log -- every later `get` reads the wrong slot -- and
            // the damage shows up far from the cause. One comparison per entry
            // is a small price for turning that into an immediate, obvious
            // failure. Release builds need this most, because that is where the
            // fuzzer runs.
            assert_eq!(
                e.index,
                self.last_index() + 1,
                "log append must be contiguous (log starts at {}, holds {} entries)",
                self.start_index,
                self.entries.len()
            );
            self.entries.push(e.clone());
        }
    }

    /// Delete the entry at `index` and everything after it.
    ///
    /// §5.3: only ever called at the first position where a follower's log
    /// actually disagrees with the leader's. Truncating on any weaker
    /// condition -- for example on every `AppendEntries` whose entries the
    /// follower already has -- would let a delayed or duplicated message
    /// delete committed entries.
    pub fn truncate_from(&mut self, index: Index) {
        debug_assert!(
            index > self.last_included.index,
            "a compacted entry is committed and must never be truncated"
        );
        if index <= self.start_index {
            self.entries.clear();
            return;
        }
        if index > self.last_index() {
            return;
        }
        let offset = (index - self.start_index) as usize;
        self.entries.truncate(offset);
    }

    /// The last entry discarded by compaction, or `ZERO` if nothing has been.
    pub fn last_included(&self) -> LogId {
        self.last_included
    }

    /// Is this index covered by a snapshot rather than held as an entry?
    pub fn is_compacted(&self, index: Index) -> bool {
        index != 0 && index <= self.last_included.index
    }

    /// Discard everything up to and including `through`, keeping the suffix.
    ///
    /// §7. The entry at `through` is *remembered* rather than kept, so
    /// `term_at(through)` still answers and a follower probing at exactly that
    /// index can still pass the consistency check. Dropping that memory is the
    /// classic compaction bug: the log looks fine and replication silently
    /// wedges, because the leader can no longer describe the entry before the
    /// one it wants to send.
    pub fn compact(&mut self, through: LogId) {
        if through.index < self.start_index {
            // Already compacted past this point.
            return;
        }
        debug_assert_eq!(
            self.term_at(through.index),
            Some(through.term),
            "refusing to compact through an entry we do not have"
        );
        let keep_from = (through.index + 1 - self.start_index) as usize;
        let keep_from = keep_from.min(self.entries.len());
        self.entries.drain(..keep_from);
        self.start_index = through.index + 1;
        self.last_included = through;
    }

    /// Throw the log away entirely and restart it after `through`.
    ///
    /// Used when an incoming snapshot describes a history this node does not
    /// have, so no suffix is worth keeping.
    pub fn reset_to(&mut self, through: LogId) {
        self.entries.clear();
        self.start_index = through.index + 1;
        self.last_included = through;
    }

    /// Make this log consistent with a snapshot boundary.
    ///
    /// The snapshot and the log are separate durable artifacts, so a crash can
    /// land between writing one and writing the other and leave them
    /// disagreeing. Recovery has to reconcile them, and the rule is the same one
    /// a follower uses when a leader ships it a snapshot: if the log can confirm
    /// the boundary entry, everything after it agrees and is kept; if it cannot,
    /// the log is stale or describes a history that never happened, and the
    /// snapshot wins.
    ///
    /// Idempotent, so it is safe to run on both the durable log and the
    /// recovered one.
    pub fn reconcile_with_snapshot(&mut self, boundary: LogId) {
        if self.term_at(boundary.index) == Some(boundary.term) {
            self.compact(boundary);
        } else {
            self.reset_to(boundary);
        }
    }

    /// Given a `prevLogIndex` whose term did not match, produce the hint that
    /// lets the leader skip back a whole term at once instead of one entry per
    /// round trip (§5.3, "Optimization").
    pub fn conflict_hint(&self, prev_log_index: Index) -> crate::message::ConflictHint {
        // Case 1: our log is simply too short. Tell the leader where our log
        // ends so it can jump straight there.
        if prev_log_index > self.last_index() {
            return crate::message::ConflictHint {
                term: None,
                first_index: self.last_index() + 1,
            };
        }
        // Case 2: we have an entry there, but from a different term. Report
        // that term and the first index we hold for it, so the leader can drop
        // the whole term in one step.
        let conflicting_term = match self.term_at(prev_log_index) {
            Some(t) => t,
            // Compacted away; ask for everything we have.
            None => {
                return crate::message::ConflictHint {
                    term: None,
                    first_index: self.start_index,
                }
            }
        };
        let mut first = prev_log_index;
        while first > self.start_index && self.term_at(first - 1) == Some(conflicting_term) {
            first -= 1;
        }
        crate::message::ConflictHint {
            term: Some(conflicting_term),
            first_index: first,
        }
    }

    /// Index just after the last entry the leader holds for `term`, or `None`
    /// if it holds no entry from that term. Used to interpret a conflict hint.
    pub fn last_index_of_term(&self, term: Term) -> Option<Index> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.term == term)
            .map(|e| e.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: Index, term: Term) -> LogEntry {
        LogEntry {
            term,
            index,
            payload: EntryPayload::Noop,
            client: None,
        }
    }

    fn log_of(terms: &[Term]) -> Log {
        let mut log = Log::new();
        let entries: Vec<LogEntry> = terms
            .iter()
            .enumerate()
            .map(|(i, &t)| entry(i as Index + 1, t))
            .collect();
        log.append(&entries);
        log
    }

    #[test]
    fn empty_log_reports_sentinel() {
        let log = Log::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_log_id(), LogId::ZERO);
        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.term_at(1), None);
    }

    #[test]
    fn term_lookup() {
        let log = log_of(&[1, 1, 2, 3]);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(4), Some(3));
        assert_eq!(log.term_at(5), None);
        assert_eq!(log.last_log_id(), LogId::new(4, 3));
    }

    #[test]
    fn truncate_removes_suffix() {
        let mut log = log_of(&[1, 1, 2, 3]);
        log.truncate_from(3);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.term_at(3), None);
    }

    #[test]
    fn truncate_beyond_end_is_a_noop() {
        let mut log = log_of(&[1, 2]);
        log.truncate_from(9);
        assert_eq!(log.last_index(), 2);
    }

    #[test]
    fn entries_from_respects_cap() {
        let log = log_of(&[1, 1, 1, 1, 1]);
        assert_eq!(log.entries_from(2, 2).len(), 2);
        assert_eq!(log.entries_from(2, 99).len(), 4);
        assert_eq!(log.entries_from(6, 99).len(), 0);
    }

    #[test]
    fn conflict_hint_when_follower_is_short() {
        let log = log_of(&[1, 1]);
        let hint = log.conflict_hint(5);
        assert_eq!(hint.term, None);
        assert_eq!(hint.first_index, 3);
    }

    #[test]
    fn conflict_hint_reports_first_index_of_conflicting_term() {
        // Follower has term 4 at indices 3..=5; leader probed index 5.
        let log = log_of(&[1, 2, 4, 4, 4]);
        let hint = log.conflict_hint(5);
        assert_eq!(hint.term, Some(4));
        assert_eq!(hint.first_index, 3);
    }

    #[test]
    fn reconciling_keeps_a_confirmable_suffix() {
        let mut log = log_of(&[1, 1, 2, 2, 3]);
        log.reconcile_with_snapshot(LogId::new(3, 2));
        assert_eq!(log.first_index(), 4);
        assert_eq!(log.last_index(), 5, "the consistent suffix survives");
    }

    #[test]
    fn reconciling_discards_a_log_that_cannot_confirm_the_boundary() {
        // The torn-write case: a snapshot landed at index 9 and the log write
        // that should have followed did not.
        let mut log = log_of(&[1, 1, 2]);
        log.reconcile_with_snapshot(LogId::new(9, 4));
        assert!(log.is_empty());
        assert_eq!(log.last_index(), 9);
        assert_eq!(log.term_at(9), Some(4));
    }

    #[test]
    fn reconciling_is_idempotent() {
        let mut log = log_of(&[1, 1, 2, 2, 3]);
        log.reconcile_with_snapshot(LogId::new(3, 2));
        let once = log.clone();
        log.reconcile_with_snapshot(LogId::new(3, 2));
        assert_eq!(log, once, "running recovery twice must change nothing");
    }

    #[test]
    fn index_zero_stops_being_an_anchor_once_the_log_is_compacted() {
        // The regression from seed 11278. On a fresh log, index 0 is the
        // sentinel that lets replication start from scratch. After compaction
        // it must answer `None`, or a leader probing at prevLogIndex 0 passes
        // the consistency check and the follower appends entries 1.. onto a log
        // that starts much later.
        let mut log = log_of(&[1, 1, 1]);
        assert_eq!(log.term_at(0), Some(0), "a fresh log anchors at 0");

        log.compact(LogId::new(2, 1));
        assert_eq!(log.term_at(0), None, "a compacted log must not anchor at 0");
        assert_eq!(log.term_at(1), None, "nor anywhere else below the boundary");
        assert_eq!(log.term_at(2), Some(1), "only at the boundary itself");
    }

    #[test]
    #[should_panic(expected = "contiguous")]
    fn a_non_contiguous_append_fails_loudly() {
        let mut log = log_of(&[1, 1, 1]);
        log.compact(LogId::new(3, 1));
        // What the bug used to do silently.
        log.append(&[entry(1, 1)]);
    }

    #[test]
    fn compaction_keeps_the_boundary_answerable() {
        let mut log = log_of(&[1, 1, 2, 2, 3]);
        log.compact(LogId::new(3, 2));

        assert_eq!(log.first_index(), 4);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.len(), 2);
        // The boundary entry is gone but still describable, which is what the
        // prevLogIndex check needs.
        assert_eq!(log.term_at(3), Some(2));
        assert!(log.get(3).is_none());
        assert!(log.is_compacted(3));
        // Everything below it is genuinely unanswerable.
        assert_eq!(log.term_at(2), None);
        // And the surviving suffix is untouched.
        assert_eq!(log.term_at(4), Some(2));
        assert_eq!(log.term_at(5), Some(3));
        assert_eq!(log.last_log_id(), LogId::new(5, 3));
    }

    #[test]
    fn compacting_the_whole_log_leaves_a_usable_boundary() {
        let mut log = log_of(&[1, 1, 2]);
        log.compact(LogId::new(3, 2));
        assert!(log.is_empty());
        assert_eq!(log.first_index(), 4);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_log_id(), LogId::new(3, 2));
        assert_eq!(log.term_at(3), Some(2));
    }

    #[test]
    fn appending_after_compaction_is_contiguous() {
        let mut log = log_of(&[1, 1, 2]);
        log.compact(LogId::new(3, 2));
        log.append(&[entry(4, 3), entry(5, 3)]);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.entries_from(4, 10).len(), 2);
        assert_eq!(log.term_at(4), Some(3));
    }

    #[test]
    fn compacting_backwards_is_a_noop() {
        let mut log = log_of(&[1, 1, 2, 2]);
        log.compact(LogId::new(3, 2));
        log.compact(LogId::new(1, 1));
        assert_eq!(
            log.first_index(),
            4,
            "an older snapshot must not un-compact"
        );
    }

    #[test]
    fn resetting_replaces_the_whole_log() {
        let mut log = log_of(&[1, 1, 2]);
        log.reset_to(LogId::new(9, 4));
        assert!(log.is_empty());
        assert_eq!(log.last_index(), 9);
        assert_eq!(log.term_at(9), Some(4));
        assert_eq!(log.term_at(3), None, "the old history is gone");
        log.append(&[entry(10, 4)]);
        assert_eq!(log.last_index(), 10);
    }

    #[test]
    fn a_conflict_hint_after_compaction_points_at_the_live_range() {
        let mut log = log_of(&[1, 1, 2, 2, 3]);
        log.compact(LogId::new(3, 2));
        // Probed below the snapshot: we cannot say anything about that term.
        let hint = log.conflict_hint(2);
        assert_eq!(hint.term, None);
        assert_eq!(hint.first_index, 4);
    }

    #[test]
    fn last_index_of_term() {
        let log = log_of(&[1, 1, 2, 2, 3]);
        assert_eq!(log.last_index_of_term(1), Some(2));
        assert_eq!(log.last_index_of_term(2), Some(4));
        assert_eq!(log.last_index_of_term(9), None);
    }
}
