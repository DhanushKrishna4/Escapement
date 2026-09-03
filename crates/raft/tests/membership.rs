//! Joint consensus (§6), driven directly against `step()`.
//!
//! Two rules carry the weight here and both are easy to get backwards:
//!
//! 1. A configuration takes effect when its entry is **appended**, not when it
//!    commits — and is un-applied if that entry is later truncated away.
//! 2. While joint, every decision needs a majority of C_old **and** a majority
//!    of C_new, independently.

use std::collections::BTreeSet;

use raft::{
    AppendEntriesReq, AppendEntriesResp, ClientResult, ClusterConfig, EntryPayload, Index, Input,
    LogEntry, NodeId, Output, RaftConfig, RaftMessage, RaftNode, RequestVoteResp, Role, Term, Tick,
};

fn node_with(id: NodeId, cluster: ClusterConfig) -> RaftNode {
    RaftNode::new(id, cluster, RaftConfig::default(), 0xC0DE, 0)
}

fn node(id: NodeId, n: usize) -> RaftNode {
    node_with(id, ClusterConfig::new(0..n as NodeId))
}

fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
    ids.iter().copied().collect()
}

fn entry(index: Index, term: Term, payload: EntryPayload) -> LogEntry {
    LogEntry {
        term,
        index,
        payload,
        client: None,
    }
}

fn config_entry(index: Index, term: Term, cfg: ClusterConfig) -> LogEntry {
    entry(index, term, EntryPayload::Config(cfg))
}

fn append(
    term: Term,
    leader: NodeId,
    prev_log_index: Index,
    prev_log_term: Term,
    entries: Vec<LogEntry>,
    leader_commit: Index,
) -> RaftMessage {
    RaftMessage::AppendEntries(AppendEntriesReq {
        term,
        leader_id: leader,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
        read_round: 0,
    })
}

fn recv(n: &mut RaftNode, now: Tick, from: NodeId, msg: RaftMessage) -> Vec<Output> {
    n.step(Input::Message { from, msg }, now)
}

fn ack(n: &mut RaftNode, now: Tick, from: NodeId, term: Term, match_index: Index) {
    recv(
        n,
        now,
        from,
        RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term,
            success: true,
            match_index,
            conflict: None,
            probed_index: match_index.saturating_sub(1),
            read_round: 0,
        }),
    );
}

/// Drive a node to leadership with votes from `voters`.
fn elect(n: &mut RaftNode, now: &mut Tick, voters: &[NodeId]) -> Term {
    *now = n.next_deadline();
    n.step(Input::Tick, *now);
    let term = n.current_term();
    for v in voters {
        recv(
            n,
            *now,
            *v,
            RaftMessage::RequestVoteResp(RequestVoteResp {
                term,
                vote_granted: true,
            }),
        );
    }
    assert_eq!(n.role(), Role::Leader, "expected the node to win");
    term
}

// ---------------------------------------------------------------------------
// Applied on append, not on commit
// ---------------------------------------------------------------------------

/// THE RULE THAT IS EASY TO GET BACKWARDS.
///
/// A follower receiving a configuration entry must obey it immediately, while
/// it is still uncommitted. If it waited for the commit, the very quorum needed
/// to commit the entry could be one it refuses to count.
#[test]
fn a_follower_obeys_an_uncommitted_configuration_entry() {
    let mut n = node(0, 3);
    assert_eq!(n.cluster().voters(), set(&[0, 1, 2]));

    let joint = ClusterConfig::joint([0, 1, 2], [0, 1, 2, 3, 4]);
    // leader_commit is 0: nothing here is committed.
    recv(
        &mut n,
        1,
        1,
        append(1, 1, 0, 0, vec![config_entry(1, 1, joint)], 0),
    );

    assert_eq!(n.commit_index(), 0, "nothing should have committed");
    assert!(
        n.is_joint(),
        "but the configuration must already be in force"
    );
    assert_eq!(n.cluster().voters(), set(&[0, 1, 2, 3, 4]));
    assert_eq!(n.config_index(), 1);
}

/// The other half of the same rule: a configuration adopted on append has to be
/// abandoned if that entry is truncated away.
#[test]
fn truncating_a_configuration_entry_reverts_the_configuration() {
    let mut n = node(0, 3);
    let joint = ClusterConfig::joint([0, 1, 2], [3, 4, 5]);
    recv(
        &mut n,
        1,
        1,
        append(1, 1, 0, 0, vec![config_entry(1, 1, joint)], 0),
    );
    assert!(n.is_joint());

    // A new leader overwrites index 1 with something else entirely.
    recv(
        &mut n,
        2,
        2,
        append(2, 2, 0, 0, vec![entry(1, 2, EntryPayload::Noop)], 0),
    );
    assert!(!n.is_joint(), "the joint configuration was truncated away");
    assert_eq!(
        n.cluster().voters(),
        set(&[0, 1, 2]),
        "it should have fallen back to the configuration below it"
    );
    assert_eq!(n.config_index(), 0);
}

#[test]
fn a_later_configuration_entry_wins() {
    let mut n = node(0, 3);
    let joint = ClusterConfig::joint([0, 1, 2], [0, 1, 2, 3]);
    let final_cfg = ClusterConfig::new([0, 1, 2, 3]);
    recv(
        &mut n,
        1,
        1,
        append(
            1,
            1,
            0,
            0,
            vec![config_entry(1, 1, joint), config_entry(2, 1, final_cfg)],
            0,
        ),
    );
    assert!(
        !n.is_joint(),
        "the last entry in the log is the one in force"
    );
    assert_eq!(n.cluster().voters(), set(&[0, 1, 2, 3]));
    assert_eq!(n.config_index(), 2);
}

#[test]
fn truncating_back_to_an_earlier_configuration_restores_it() {
    let mut n = node(0, 3);
    let joint = ClusterConfig::joint([0, 1, 2], [0, 1, 2, 3]);
    let final_cfg = ClusterConfig::new([0, 1, 2, 3]);
    recv(
        &mut n,
        1,
        1,
        append(
            1,
            1,
            0,
            0,
            vec![config_entry(1, 1, joint), config_entry(2, 1, final_cfg)],
            0,
        ),
    );
    assert!(!n.is_joint());

    // Index 2 is overwritten; the joint entry at index 1 survives.
    recv(
        &mut n,
        2,
        2,
        append(2, 2, 1, 1, vec![entry(2, 2, EntryPayload::Noop)], 0),
    );
    assert!(n.is_joint(), "the surviving joint entry is in force again");
    assert_eq!(n.config_index(), 1);
}

// ---------------------------------------------------------------------------
// The joint quorum rule
// ---------------------------------------------------------------------------

/// While joint, a majority of C_old alone must not be able to commit.
#[test]
fn a_joint_leader_needs_both_halves_to_commit() {
    // Moving {0,1,2} to {3,4,5}: the halves are disjoint, which is the sharpest
    // case.
    let mut leader = node(0, 3);
    let mut now = 0;
    let term = elect(&mut leader, &mut now, &[1, 2]);
    // Commit the election no-op so a change is allowed to start.
    ack(&mut leader, now, 1, term, 1);
    ack(&mut leader, now, 2, term, 1);
    assert_eq!(leader.commit_index(), 1);

    leader.step(Input::ChangeMembership(set(&[3, 4, 5])), now);
    assert!(leader.is_joint());
    let joint_at = leader.config_index();
    assert_eq!(joint_at, 2);

    // Both of C_old's other members acknowledge. That is all of C_old, and it
    // still is not enough.
    ack(&mut leader, now, 1, term, joint_at);
    ack(&mut leader, now, 2, term, joint_at);
    assert_eq!(
        leader.commit_index(),
        1,
        "a unanimous C_old must not commit a joint entry on its own"
    );

    // Add a majority of C_new and it commits.
    ack(&mut leader, now, 3, term, joint_at);
    ack(&mut leader, now, 4, term, joint_at);
    assert!(
        leader.commit_index() >= joint_at,
        "with both halves it should commit"
    );
}

#[test]
fn committing_the_joint_entry_makes_the_leader_append_c_new() {
    let mut leader = node(0, 3);
    let mut now = 0;
    let term = elect(&mut leader, &mut now, &[1, 2]);
    ack(&mut leader, now, 1, term, 1);
    leader.step(Input::ChangeMembership(set(&[0, 1, 2, 3, 4])), now);
    let joint_at = leader.config_index();

    // A quorum of both halves.
    for peer in [1, 2, 3, 4] {
        ack(&mut leader, now, peer, term, joint_at);
    }

    assert!(
        !leader.is_joint(),
        "once C_old,new commits the leader should move to C_new"
    );
    assert_eq!(leader.cluster().voters(), set(&[0, 1, 2, 3, 4]));
    assert!(
        leader.config_index() > joint_at,
        "C_new should be a later entry"
    );
    assert!(matches!(
        leader.log().get(leader.config_index()).unwrap().payload,
        EntryPayload::Config(_)
    ));
}

// ---------------------------------------------------------------------------
// One change at a time
// ---------------------------------------------------------------------------

#[test]
fn a_second_change_is_refused_while_one_is_in_flight() {
    let mut leader = node(0, 3);
    let mut now = 0;
    let term = elect(&mut leader, &mut now, &[1, 2]);
    ack(&mut leader, now, 1, term, 1);

    leader.step(Input::ChangeMembership(set(&[0, 1, 2, 3])), now);
    assert!(leader.is_joint());

    let outs = leader.step(Input::ChangeMembership(set(&[0, 1, 2, 4])), now);
    assert!(
        outs.iter().any(|o| matches!(
            o,
            Output::ClientResponse {
                result: ClientResult::ChangeInProgress,
                ..
            }
        )),
        "overlapping changes must be refused: {outs:?}"
    );
    assert_eq!(
        leader.cluster().new_voters(),
        Some(&set(&[0, 1, 2, 3])),
        "the in-flight change must be untouched"
    );
}

#[test]
fn a_follower_refuses_to_change_membership() {
    let mut n = node(0, 3);
    let outs = n.step(Input::ChangeMembership(set(&[0, 1])), 1);
    assert!(outs.iter().any(|o| matches!(
        o,
        Output::ClientResponse {
            result: ClientResult::NotLeader { .. },
            ..
        }
    )));
    assert_eq!(n.cluster().voters(), set(&[0, 1, 2]));
}

// ---------------------------------------------------------------------------
// A leader that removes itself
// ---------------------------------------------------------------------------

/// §6: a leader not in C_new must keep leading until C_new is committed — it is
/// the one replicating the entry that removes it — and step down immediately
/// afterwards.
#[test]
fn a_leader_removed_by_the_change_steps_down_once_c_new_commits() {
    let mut leader = node_with(0, ClusterConfig::new([0, 1, 2]));
    let mut now = 0;
    let term = elect(&mut leader, &mut now, &[1, 2]);
    ack(&mut leader, now, 1, term, 1);

    // Remove ourselves.
    leader.step(Input::ChangeMembership(set(&[1, 2])), now);
    assert!(leader.is_joint());
    let joint_at = leader.config_index();
    assert_eq!(
        leader.role(),
        Role::Leader,
        "still leading during the change"
    );

    // Commit C_old,new: needs a majority of {0,1,2} and of {1,2}.
    ack(&mut leader, now, 1, term, joint_at);
    ack(&mut leader, now, 2, term, joint_at);
    assert!(!leader.is_joint(), "should have moved to C_new");
    let c_new_at = leader.config_index();
    assert_eq!(
        leader.role(),
        Role::Leader,
        "it must keep leading until C_new itself commits"
    );

    // Commit C_new. We are no longer a voter, so only nodes 1 and 2 count.
    ack(&mut leader, now, 1, term, c_new_at);
    ack(&mut leader, now, 2, term, c_new_at);
    assert_eq!(
        leader.role(),
        Role::Follower,
        "a leader outside C_new steps down once C_new commits"
    );
}

#[test]
fn a_node_outside_the_configuration_does_not_campaign() {
    let mut n = node_with(0, ClusterConfig::new([1, 2, 3]));
    for _ in 0..5 {
        let deadline = n.next_deadline();
        n.step(Input::Tick, deadline);
    }
    assert_eq!(n.role(), Role::Follower, "a non-voter must never campaign");
    assert_eq!(n.current_term(), 0, "and must not inflate the term");
}

#[test]
fn a_node_added_by_a_configuration_entry_may_campaign() {
    // Node 3 starts outside the cluster and learns of its own addition from the
    // log, exactly as it would in a real change.
    let mut n = node_with(3, ClusterConfig::new([0, 1, 2]));
    assert!(!n.cluster().contains(3));

    let joint = ClusterConfig::joint([0, 1, 2], [0, 1, 2, 3]);
    recv(
        &mut n,
        1,
        0,
        append(1, 0, 0, 0, vec![config_entry(1, 1, joint)], 0),
    );
    assert!(n.cluster().contains(3), "it should now be a voter");

    let deadline = n.next_deadline();
    n.step(Input::Tick, deadline);
    assert_eq!(n.role(), Role::Candidate, "and may now campaign");
}

// ---------------------------------------------------------------------------
// Votes
// ---------------------------------------------------------------------------

#[test]
fn a_joint_candidate_needs_votes_from_both_halves() {
    let mut n = node_with(0, ClusterConfig::new([0, 1, 2]));
    // Adopt a joint configuration moving to a disjoint set.
    let joint = ClusterConfig::joint([0, 1, 2], [0, 3, 4]);
    recv(
        &mut n,
        1,
        1,
        append(1, 1, 0, 0, vec![config_entry(1, 1, joint)], 0),
    );
    assert!(n.is_joint());

    let now = n.next_deadline();
    n.step(Input::Tick, now);
    let term = n.current_term();
    assert_eq!(n.role(), Role::Candidate);

    // All of C_old votes for us. Not enough: we hold only 1 of 3 in C_new.
    for v in [1, 2] {
        recv(
            &mut n,
            now,
            v,
            RaftMessage::RequestVoteResp(RequestVoteResp {
                term,
                vote_granted: true,
            }),
        );
    }
    assert_eq!(
        n.role(),
        Role::Candidate,
        "a unanimous C_old is not a joint quorum"
    );

    // One vote from C_new tips it.
    recv(
        &mut n,
        now,
        3,
        RaftMessage::RequestVoteResp(RequestVoteResp {
            term,
            vote_granted: true,
        }),
    );
    assert_eq!(n.role(), Role::Leader);
}
