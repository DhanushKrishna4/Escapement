//! Snapshots (§7).
//!
//! A log that only grows is a log that eventually cannot be replayed, stored or
//! shipped to a new follower. Compaction fixes that by discarding a prefix that
//! the state machine has already absorbed — but the prefix cannot simply be
//! deleted, because the leader still has to answer `prevLogIndex` /
//! `prevLogTerm` for the entry immediately before whatever it sends next. That
//! is what `last_included` remembers, and it is why the log has carried an
//! offset since the first commit of this project.

use serde::{Deserialize, Serialize};

use crate::{ClusterConfig, LogId};

/// The state machine's state as of `last_included`, plus what Raft needs to
/// keep talking about the log after the prefix is gone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The last entry this snapshot accounts for.
    ///
    /// Both halves matter. The index says where the log resumes; the term is
    /// what a leader answers with when a follower probes at exactly this index,
    /// and without it the consistency check could not be satisfied after
    /// compaction.
    pub last_included: LogId,
    /// The membership in force at `last_included`.
    ///
    /// A snapshot has to carry this. A node recovering from one has discarded
    /// every config entry below the boundary, so without it there would be
    /// nothing to say who the voters are — and it would fall back to whatever
    /// it was started with, silently resurrecting a membership the cluster left
    /// behind.
    pub config: ClusterConfig,
    /// Opaque state machine bytes. Raft must not know what is in here.
    pub data: Vec<u8>,
}

impl Snapshot {
    pub fn new(last_included: LogId, config: ClusterConfig, data: Vec<u8>) -> Self {
        Snapshot {
            last_included,
            config,
            data,
        }
    }

    pub fn index(&self) -> crate::Index {
        self.last_included.index
    }

    pub fn term(&self) -> crate::Term {
        self.last_included.term
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
