//! A Raft consensus node, implemented as a pure state machine.
//!
//! # The one rule this crate lives by
//!
//! A node never performs I/O, never reads a clock, and never generates
//! randomness from the environment. It receives an [`Input`] plus the current
//! virtual time and returns a list of [`Output`]s describing what it *wants*
//! done:
//!
//! ```text
//! fn step(&mut self, input: Input, now: Tick) -> Vec<Output>
//! ```
//!
//! Everything else in the project follows from that. Because a node is a pure
//! function of (state, input, time), the simulator can own the clock and the
//! network, replay any run from its seed, and run tens of thousands of
//! randomized fault scenarios in a single thread.
//!
//! Section references in the comments (§5.2, §5.4.1, ...) are to Ongaro &
//! Ousterhout, "In Search of an Understandable Consensus Algorithm" (extended
//! version). The rules that enforce a safety property are commented with *why*
//! they exist, not just what they do.

pub mod config;
pub mod log;
pub mod message;
pub mod node;
pub mod rand;
pub mod snapshot;

pub use config::ClusterConfig;
pub use log::{EntryPayload, Log, LogEntry};
pub use message::{
    AppendEntriesReq, AppendEntriesResp, ConflictHint, InstallSnapshotReq, InstallSnapshotResp,
    RaftMessage, RequestVoteReq, RequestVoteResp,
};
pub use node::{
    BugSwitches, ClientRequest, ClientResult, Input, Output, PersistOp, RaftConfig, RaftNode, Role,
};
pub use rand::Rng;
pub use snapshot::Snapshot;

use serde::{Deserialize, Serialize};

/// Identifies a node within a cluster. Small and `Ord` so it can key a
/// `BTreeMap` — never a `HashMap`, whose iteration order varies per process.
pub type NodeId = u32;

/// Raft term. Monotonically increasing; the logical clock of the algorithm.
pub type Term = u64;

/// 1-based log position. Index 0 is the "before the beginning" sentinel and
/// always has term 0, which lets the very first `AppendEntries` consistency
/// check succeed without a special case.
pub type Index = u64;

/// Virtual time, supplied by the simulator. Never derived from a wall clock.
pub type Tick = u64;

/// Identifies a logical client issuing requests.
pub type ClientId = u32;

/// A command handed to the replicated state machine. Opaque here on purpose:
/// the Raft core must not know what is being agreed upon.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Command(pub Vec<u8>);

impl core::fmt::Debug for Command {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Commands are usually UTF-8 in this project (JSON from the kvstore),
        // and readable traces are worth the branch.
        match core::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "Command({s:?})"),
            Err(_) => write!(f, "Command(<{} bytes>)", self.0.len()),
        }
    }
}

/// A position in the log: an (index, term) pair.
///
/// This exists as a struct rather than a loose tuple because the election
/// restriction (§5.4.1) compares term FIRST and index SECOND, and swapping them
/// is a real, silent safety bug. Comparisons go through [`LogId::at_least_as_up_to_date_as`]
/// so there is exactly one place to get it right.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogId {
    pub index: Index,
    pub term: Term,
}

impl LogId {
    pub const ZERO: LogId = LogId { index: 0, term: 0 };

    pub fn new(index: Index, term: Term) -> Self {
        LogId { index, term }
    }

    /// §5.4.1 (Election restriction): "Raft determines which of two logs is
    /// more up-to-date by comparing the index and term of the last entries in
    /// the logs. If the logs have last entries with different terms, then the
    /// log with the later term is more up-to-date. If the logs end with the
    /// same term, then whichever log is longer is more up-to-date."
    ///
    /// WHY IT MATTERS: this is what guarantees Leader Completeness. A voter
    /// refuses any candidate whose log could be missing a committed entry, so
    /// every leader's log necessarily contains all committed entries and no
    /// leader ever has to overwrite one. Comparing index before term would let
    /// a node with a long stale-term log win an election and truncate committed
    /// entries out of the cluster.
    pub fn at_least_as_up_to_date_as(&self, other: &LogId) -> bool {
        (self.term, self.index) >= (other.term, other.index)
    }
}
