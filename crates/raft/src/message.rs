//! Raft RPCs.
//!
//! Every message carries the sender's term, because the very first thing any
//! receiver does is compare terms (§5.1). Requests and responses are separate
//! variants of one enum so that the network can treat them uniformly and the
//! `step` function can match on every (role, message) pair exhaustively.

use serde::{Deserialize, Serialize};

use crate::{log::LogEntry, Index, NodeId, Term};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote(RequestVoteReq),
    RequestVoteResp(RequestVoteResp),
    AppendEntries(AppendEntriesReq),
    AppendEntriesResp(AppendEntriesResp),
    InstallSnapshot(InstallSnapshotReq),
    InstallSnapshotResp(InstallSnapshotResp),
}

impl RaftMessage {
    pub fn term(&self) -> Term {
        match self {
            RaftMessage::RequestVote(m) => m.term,
            RaftMessage::RequestVoteResp(m) => m.term,
            RaftMessage::AppendEntries(m) => m.term,
            RaftMessage::AppendEntriesResp(m) => m.term,
            RaftMessage::InstallSnapshot(m) => m.term,
            RaftMessage::InstallSnapshotResp(m) => m.term,
        }
    }

    /// Short label for traces and the visualizer.
    pub fn kind(&self) -> &'static str {
        match self {
            RaftMessage::RequestVote(_) => "RequestVote",
            RaftMessage::RequestVoteResp(_) => "RequestVoteResp",
            RaftMessage::AppendEntries(_) => "AppendEntries",
            RaftMessage::AppendEntriesResp(_) => "AppendEntriesResp",
            RaftMessage::InstallSnapshot(_) => "InstallSnapshot",
            RaftMessage::InstallSnapshotResp(_) => "InstallSnapshotResp",
        }
    }

    /// Whether this is a request (something a peer must answer) as opposed to
    /// a response. Stale requests get a reply carrying our term; stale
    /// responses are simply dropped.
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            RaftMessage::RequestVote(_)
                | RaftMessage::AppendEntries(_)
                | RaftMessage::InstallSnapshot(_)
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteReq {
    pub term: Term,
    pub candidate_id: NodeId,
    /// Last entry in the candidate's log. The voter applies the election
    /// restriction (§5.4.1) to these two fields.
    pub last_log_index: Index,
    pub last_log_term: Term,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteResp {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesReq {
    pub term: Term,
    pub leader_id: NodeId,
    /// The entry immediately preceding `entries`. The follower accepts only if
    /// it holds an entry at this index with this term -- the Log Matching
    /// Property (§5.3) is maintained by induction on exactly this check.
    pub prev_log_index: Index,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: Index,
    /// Identifies a ReadIndex confirmation round (§6.4), or 0 for an ordinary
    /// append. Echoed back so the leader can tell which round an
    /// acknowledgement belongs to — a delayed response from an older round
    /// proves nothing about leadership *now*.
    pub read_round: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
    /// On success, the highest index the follower now agrees on.
    ///
    /// WHY IT IS IN THE RESPONSE: the paper has the leader infer this from the
    /// request it sent, but under duplication and reordering a leader can
    /// receive an old response after a newer one. Echoing the index makes the
    /// response self-describing, so the leader can apply it with a `max` and a
    /// stale duplicate becomes a no-op instead of a regression.
    pub match_index: Index,
    /// On failure, where the leader should probe next (§5.3 optimization).
    pub conflict: Option<ConflictHint>,
    /// The `read_round` this is answering.
    pub read_round: u64,
    /// The `prevLogIndex` this is a response to.
    ///
    /// WHY: without it, a leader cannot tell a duplicated rejection from a
    /// fresh one, and each copy walks `nextIndex` back another step — so a
    /// duplicating network turns the conflict-hint optimization back into a
    /// one-entry-at-a-time crawl. Echoing the probe makes rejections
    /// idempotent: only the one answering the outstanding probe counts.
    pub probed_index: Index,
}

/// A follower's explanation of *why* the consistency check failed, so the
/// leader can skip an entire conflicting term in one round trip rather than
/// walking `nextIndex` back one entry at a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictHint {
    /// The term of the conflicting entry, or `None` if the follower's log is
    /// simply too short to have an entry at `prevLogIndex` at all.
    pub term: Option<Term>,
    /// If `term` is set: the first index the follower holds for that term.
    /// If `term` is `None`: one past the end of the follower's log.
    pub first_index: Index,
}

/// §7. Sent when a follower has fallen so far behind that the entries it needs
/// have already been compacted away.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotReq {
    pub term: Term,
    pub leader_id: NodeId,
    /// The last entry the snapshot accounts for. The term is as important as
    /// the index: it is what lets the follower answer a later `prevLogIndex`
    /// probe at exactly this position.
    pub last_included_index: Index,
    pub last_included_term: Term,
    /// The membership in force at the snapshot boundary (§6 + §7). Without it a
    /// follower installing this would have no idea who the voters are.
    pub config: crate::ClusterConfig,
    /// Opaque state machine bytes.
    ///
    /// The paper chunks this with an offset and a done flag. Sent whole here:
    /// chunking is a transport concern with no bearing on the algorithm's
    /// correctness, and modelling it would add reassembly state to every
    /// follower without exercising anything Raft-specific. The network model
    /// still delays and drops these like any other message.
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotResp {
    pub term: Term,
    /// What the follower now holds through — normally the snapshot's index, but
    /// its own higher watermark if it was already ahead. Echoed for the same
    /// reason `AppendEntriesResp` echoes `match_index`: a delayed or duplicated
    /// response must be applicable with a `max` rather than an assignment.
    pub last_included_index: Index,
}
