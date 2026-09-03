//! A record of the moments worth looking at.
//!
//! The full trace holds everything — millions of records for a long run — which
//! is exactly what the determinism test needs and exactly what a scrubbable
//! timeline does not. This is the other view: the handful of events a person
//! would point at, with the tick to jump to.
//!
//! Bounded on purpose. A run that churns for a million ticks would otherwise
//! accumulate an unbounded list nobody can read, and the interesting part is
//! almost always the recent past plus the violations.

use std::collections::VecDeque;

use raft::{Index, NodeId, Term, Tick};
use serde::{Deserialize, Serialize};

/// The most entries kept. Older ones are dropped, except that violations are
/// also mirrored into a list of their own so a failure is never scrolled away.
pub const CAPACITY: usize = 2_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Moment {
    ElectionStarted {
        node: NodeId,
        term: Term,
    },
    LeaderElected {
        node: NodeId,
        term: Term,
    },
    /// A leader's commit index moved. Recorded sparsely -- every commit would
    /// swamp everything else.
    Committed {
        node: NodeId,
        index: Index,
    },
    LogTruncated {
        node: NodeId,
        from: Index,
        entries: u64,
    },
    Fault {
        description: String,
    },
    Crashed {
        node: NodeId,
    },
    Restarted {
        node: NodeId,
    },
    SnapshotTaken {
        node: NodeId,
        through: Index,
    },
    SnapshotInstalled {
        node: NodeId,
        through: Index,
    },
    MembershipChanged {
        voters: Vec<NodeId>,
    },
    /// A safety violation. Always kept.
    Violation {
        invariant: String,
        detail: String,
    },
}

impl Moment {
    /// Rough importance, so the UI can decide what to show when space is tight.
    pub fn severity(&self) -> &'static str {
        match self {
            Moment::Violation { .. } => "violation",
            Moment::LogTruncated { .. } | Moment::Crashed { .. } | Moment::Fault { .. } => "fault",
            Moment::LeaderElected { .. } | Moment::MembershipChanged { .. } => "leadership",
            _ => "info",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub tick: Tick,
    pub moment: Moment,
}

#[derive(Clone, Debug, Default)]
pub struct Timeline {
    entries: VecDeque<Entry>,
    /// Violations, kept separately so they survive the ring buffer.
    violations: Vec<Entry>,
    /// The last commit index recorded per node, so `Committed` is only pushed
    /// when it actually moves and not on every heartbeat.
    last_commit: std::collections::BTreeMap<NodeId, Index>,
    dropped: u64,
}

impl Timeline {
    pub fn new() -> Self {
        Timeline::default()
    }

    pub fn push(&mut self, tick: Tick, moment: Moment) {
        if let Moment::Violation { .. } = &moment {
            self.violations.push(Entry {
                tick,
                moment: moment.clone(),
            });
        }
        self.entries.push_back(Entry { tick, moment });
        while self.entries.len() > CAPACITY {
            self.entries.pop_front();
            self.dropped += 1;
        }
    }

    /// Record a commit only when it advances, and only in steps, so a busy
    /// leader does not bury everything else.
    pub fn note_commit(&mut self, tick: Tick, node: NodeId, index: Index) {
        const EVERY: Index = 5;
        let last = self.last_commit.get(&node).copied().unwrap_or(0);
        if index > last && (index / EVERY > last / EVERY || index - last >= EVERY) {
            self.last_commit.insert(node, index);
            self.push(tick, Moment::Committed { node, index });
        } else if index > last {
            self.last_commit.insert(node, index);
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    pub fn violations(&self) -> &[Entry] {
        &self.violations
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_keeps_the_most_recent_entries() {
        let mut t = Timeline::new();
        for i in 0..(CAPACITY as u64 + 50) {
            t.push(i, Moment::Crashed { node: 0 });
        }
        assert_eq!(t.len(), CAPACITY);
        assert_eq!(t.dropped(), 50);
        assert_eq!(
            t.entries().next().unwrap().tick,
            50,
            "the oldest are dropped"
        );
    }

    #[test]
    fn violations_survive_the_ring_buffer() {
        let mut t = Timeline::new();
        t.push(
            1,
            Moment::Violation {
                invariant: "Election Safety".into(),
                detail: "two leaders".into(),
            },
        );
        for i in 0..(CAPACITY as u64 + 100) {
            t.push(i + 2, Moment::Crashed { node: 0 });
        }
        assert_eq!(
            t.violations().len(),
            1,
            "a violation must never scroll away"
        );
        assert_eq!(t.violations()[0].tick, 1);
    }

    #[test]
    fn commits_are_recorded_sparsely_but_never_go_backwards() {
        let mut t = Timeline::new();
        for i in 1..=40u64 {
            t.note_commit(i * 10, 0, i);
        }
        assert!(
            t.len() < 20,
            "every commit would bury everything else: {}",
            t.len()
        );
        assert!(t.len() > 3, "but some should be recorded: {}", t.len());
        let ticks: Vec<Tick> = t.entries().map(|e| e.tick).collect();
        assert!(
            ticks.windows(2).all(|w| w[0] <= w[1]),
            "entries must be in order"
        );
    }

    #[test]
    fn a_commit_that_does_not_advance_records_nothing() {
        let mut t = Timeline::new();
        t.note_commit(10, 0, 5);
        let before = t.len();
        t.note_commit(20, 0, 5);
        t.note_commit(30, 0, 3);
        assert_eq!(t.len(), before);
    }
}
