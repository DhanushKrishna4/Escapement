//! Simulated durable storage.
//!
//! One of these per node. It is the *only* thing that survives a crash, which
//! is what makes crash tests meaningful: if the node persists too little,
//! recovery loses something the paper says it must keep; if it persists a
//! volatile field it should not, the test would pass for the wrong reason.

use raft::{Index, Log, NodeId, PersistOp, Snapshot, Term};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Storage {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Log,
    /// The most recent state machine snapshot. Durable, because after
    /// compaction it is the only record of everything below the log's start.
    pub snapshot: Option<Snapshot>,
    /// How many persist operations have hit this disk. Coverage statistic, and
    /// a cheap way to see write amplification in a trace.
    pub writes: u64,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            current_term: 0,
            voted_for: None,
            log: Log::new(),
            snapshot: None,
            writes: 0,
        }
    }

    pub fn apply(&mut self, op: &PersistOp) {
        self.writes += 1;
        match op {
            PersistOp::HardState {
                current_term,
                voted_for,
            } => {
                self.current_term = *current_term;
                self.voted_for = *voted_for;
            }
            PersistOp::Append(entries) => self.log.append(entries),
            PersistOp::TruncateFrom(index) => self.log.truncate_from(*index),
            PersistOp::Snapshot(snapshot) => self.snapshot = Some(snapshot.clone()),
            PersistOp::Compact(through) => self.log.compact(*through),
            PersistOp::ResetLog(through) => self.log.reset_to(*through),
        }
    }

    /// Fix the durable log up against the durable snapshot, the way a real
    /// implementation does on startup.
    ///
    /// These are two separate artifacts and a crash can land between writing
    /// them. Doing this on the disk rather than only on the recovered node
    /// matters: the node's log and this one have to stay in step, or the very
    /// next `Append` lands at an index the durable log is not expecting. The
    /// fuzzer found exactly that (seed 2367) once the in-memory side alone was
    /// being reconciled.
    pub fn recover(&mut self, reconcile: bool) {
        if !reconcile {
            return;
        }
        if let Some(snapshot) = &self.snapshot {
            self.log.reconcile_with_snapshot(snapshot.last_included);
        }
    }

    pub fn last_index(&self) -> Index {
        self.log.last_index()
    }
}
