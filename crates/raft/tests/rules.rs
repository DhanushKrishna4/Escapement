//! Unit tests for individual Raft rules, driven directly against `step()`.
//!
//! These poke at one rule at a time, in isolation, with no simulator involved.
//! The value is diagnostic: when the fuzzer reports a safety violation, these
//! say which rule broke.

use raft::{
    AppendEntriesReq, AppendEntriesResp, ClusterConfig, EntryPayload, Index, Input, LogEntry,
    NodeId, Output, PersistOp, RaftConfig, RaftMessage, RaftNode, RequestVoteReq, RequestVoteResp,
    Role, Term, Tick,
};

/// A tick far enough past any leader contact that §6's disruption guard is not
/// in play. Tests about voting rules use this so they are testing the rule they
/// mean to.
const QUIET: Tick = 1_000;

fn node(id: NodeId, n: usize) -> RaftNode {
    RaftNode::new(
        id,
        ClusterConfig::new(0..n as NodeId),
        RaftConfig::default(),
        0xABCD,
        0,
    )
}

fn entry(index: Index, term: Term) -> LogEntry {
    LogEntry {
        term,
        index,
        payload: EntryPayload::Noop,
        client: None,
    }
}

fn recv(n: &mut RaftNode, now: Tick, from: NodeId, msg: RaftMessage) -> Vec<Output> {
    n.step(Input::Message { from, msg }, now)
}

fn append_entries(
    term: Term,
    leader_id: NodeId,
    prev_log_index: Index,
    prev_log_term: Term,
    entries: Vec<LogEntry>,
    leader_commit: Index,
) -> RaftMessage {
    RaftMessage::AppendEntries(AppendEntriesReq {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
        read_round: 0,
    })
}

fn request_vote(
    term: Term,
    candidate_id: NodeId,
    last_log_index: Index,
    last_log_term: Term,
) -> RaftMessage {
    RaftMessage::RequestVote(RequestVoteReq {
        term,
        candidate_id,
        last_log_index,
        last_log_term,
    })
}

fn vote_reply(outs: &[Output]) -> RequestVoteResp {
    outs.iter()
        .find_map(|o| match o {
            Output::Send {
                msg: RaftMessage::RequestVoteResp(r),
                ..
            } => Some(r.clone()),
            _ => None,
        })
        .expect("expected a RequestVoteResp")
}

fn append_reply(outs: &[Output]) -> AppendEntriesResp {
    outs.iter()
        .find_map(|o| match o {
            Output::Send {
                msg: RaftMessage::AppendEntriesResp(r),
                ..
            } => Some(r.clone()),
            _ => None,
        })
        .expect("expected an AppendEntriesResp")
}

fn sent_appends(outs: &[Output]) -> Vec<(NodeId, AppendEntriesReq)> {
    outs.iter()
        .filter_map(|o| match o {
            Output::Send {
                to,
                msg: RaftMessage::AppendEntries(r),
            } => Some((*to, r.clone())),
            _ => None,
        })
        .collect()
}

/// Drive a node from follower to leader of the next term, with `voters`
/// granting their votes.
fn elect(n: &mut RaftNode, now: &mut Tick, voters: &[NodeId]) -> Term {
    *now = n.next_deadline();
    n.step(Input::Tick, *now);
    assert_eq!(n.role(), Role::Candidate);
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
    assert_eq!(
        n.role(),
        Role::Leader,
        "expected the node to win the election"
    );
    term
}

// ---------------------------------------------------------------------------
// §5.1 -- terms
// ---------------------------------------------------------------------------

#[test]
fn a_higher_term_makes_any_node_a_follower() {
    let mut n = node(0, 3);
    let mut now = 0;
    elect(&mut n, &mut now, &[1]);
    let term = n.current_term();

    recv(&mut n, now, 2, append_entries(term + 5, 2, 0, 0, vec![], 0));
    assert_eq!(n.role(), Role::Follower);
    assert_eq!(n.current_term(), term + 5);
    assert_eq!(
        n.leader_id(),
        Some(2),
        "AppendEntries identifies the new leader"
    );
    assert_eq!(n.voted_for(), None, "a new term starts with no vote cast");
}

#[test]
fn a_higher_term_request_vote_does_not_name_a_leader() {
    let mut n = node(0, 3);
    recv(&mut n, 0, 1, request_vote(7, 1, 0, 0));
    assert_eq!(n.current_term(), 7);
    assert_eq!(
        n.leader_id(),
        None,
        "a candidate has not won anything yet, so there is no leader to point at"
    );
}

#[test]
fn a_stale_request_is_rejected_with_our_term() {
    let mut n = node(0, 3);
    let mut now = 0;
    elect(&mut n, &mut now, &[1]);
    let term = n.current_term();

    let outs = recv(&mut n, now + QUIET, 2, request_vote(term - 1, 2, 99, 99));
    let reply = vote_reply(&outs);
    assert!(!reply.vote_granted);
    assert_eq!(
        reply.term, term,
        "the reply must carry our term so the sender steps down"
    );
    assert_eq!(
        n.role(),
        Role::Leader,
        "a stale message must not disturb us"
    );
}

#[test]
fn a_stale_response_is_dropped_silently() {
    let mut n = node(0, 3);
    let mut now = 0;
    elect(&mut n, &mut now, &[1]);
    let term = n.current_term();

    let outs = recv(
        &mut n,
        now,
        2,
        RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term: term - 1,
            success: true,
            match_index: 99,
            conflict: None,
            probed_index: 98,
            read_round: 0,
        }),
    );
    assert!(
        outs.is_empty(),
        "a stale response should produce nothing: {outs:?}"
    );
    assert_eq!(
        n.commit_index(),
        0,
        "a stale response must not move the commit index"
    );
}

// ---------------------------------------------------------------------------
// §5.2 / §5.4.1 -- voting
// ---------------------------------------------------------------------------

#[test]
fn one_vote_per_term() {
    let mut n = node(0, 3);
    assert!(vote_reply(&recv(&mut n, 0, 1, request_vote(1, 1, 0, 0))).vote_granted);
    assert!(
        !vote_reply(&recv(&mut n, 0, 2, request_vote(1, 2, 0, 0))).vote_granted,
        "a second candidate in the same term must be refused"
    );
}

#[test]
fn re_granting_to_the_same_candidate_is_idempotent() {
    // A duplicated RequestVote must not cost a candidate a vote it already won.
    let mut n = node(0, 3);
    assert!(vote_reply(&recv(&mut n, 0, 1, request_vote(1, 1, 0, 0))).vote_granted);
    assert!(vote_reply(&recv(&mut n, 0, 1, request_vote(1, 1, 0, 0))).vote_granted);
}

#[test]
fn a_granted_vote_is_persisted_before_the_reply() {
    // Ordering matters: granting a vote, crashing, and forgetting it would let
    // this node vote twice in one term and elect two leaders.
    let mut n = node(0, 3);
    let outs = recv(&mut n, 0, 1, request_vote(1, 1, 0, 0));
    // Exactly one write, even though this step both advanced the term and cast
    // a vote: currentTerm and votedFor are one durable record.
    assert_eq!(
        outs.iter()
            .filter(|o| matches!(o, Output::Persist(PersistOp::HardState { .. })))
            .count(),
        1,
        "one hard-state write per step: {outs:?}"
    );
    let persist_at = outs
        .iter()
        .position(|o| matches!(o, Output::Persist(PersistOp::HardState { .. })))
        .expect("the vote must be persisted");
    let send_at = outs
        .iter()
        .position(|o| matches!(o, Output::Send { .. }))
        .expect("the reply must be sent");
    assert!(
        persist_at < send_at,
        "persist must be ordered before send: {outs:?}"
    );
    match &outs[persist_at] {
        Output::Persist(PersistOp::HardState {
            current_term,
            voted_for,
        }) => {
            assert_eq!(*current_term, 1);
            assert_eq!(*voted_for, Some(1));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

/// §5.4.1: compare the last entry's TERM first, and only then its INDEX.
#[test]
fn the_election_restriction_compares_term_before_index() {
    // Give the voter a two-entry log, both from term 1.
    let mut n = node(0, 3);
    recv(
        &mut n,
        0,
        1,
        append_entries(1, 1, 0, 0, vec![entry(1, 1), entry(2, 1)], 0),
    );
    assert_eq!(n.log().last_index(), 2);

    // Ask outside the minimum election timeout. Inside it, §6's disruption
    // guard makes a node that just heard from a leader ignore vote requests
    // entirely, which is a different rule than the one under test here.
    let outs = recv(&mut n, QUIET, 2, request_vote(2, 2, 1, 2));
    assert!(
        vote_reply(&outs).vote_granted,
        "a higher last-log term wins even with a shorter log"
    );
}

#[test]
fn a_candidate_with_a_stale_log_is_denied_however_long_it_is() {
    let mut n = node(0, 3);
    // Voter's log ends at (index 2, term 3).
    recv(
        &mut n,
        0,
        1,
        append_entries(3, 1, 0, 0, vec![entry(1, 3), entry(2, 3)], 0),
    );

    // Candidate's log is far longer but ends in an older term. Denying this is
    // exactly what guarantees Leader Completeness: its log could be missing a
    // committed entry.
    let outs = recv(&mut n, QUIET, 2, request_vote(4, 2, 100, 2));
    assert!(!vote_reply(&outs).vote_granted);
}

#[test]
fn an_equal_log_is_up_to_date_enough() {
    let mut n = node(0, 3);
    recv(
        &mut n,
        0,
        1,
        append_entries(2, 1, 0, 0, vec![entry(1, 2)], 0),
    );
    let outs = recv(&mut n, QUIET, 2, request_vote(3, 2, 1, 2));
    assert!(
        vote_reply(&outs).vote_granted,
        "an identical log is 'at least as up to date'"
    );
}

#[test]
fn a_split_vote_leaves_no_leader() {
    // Two candidates, three voters, each candidate takes one vote plus its own.
    let mut a = node(0, 3);
    let mut b = node(1, 3);
    let now = a.next_deadline();
    a.step(Input::Tick, now);
    let term = a.current_term();
    // b campaigns in the same term.
    b.step(Input::Tick, b.next_deadline().max(now));
    while b.current_term() < term {
        b.step(Input::Tick, b.next_deadline());
    }
    // Neither has a majority with a single self-vote.
    assert_eq!(a.role(), Role::Candidate);
    assert_eq!(b.role(), Role::Candidate);
    // A denial does not make anyone a leader.
    recv(
        &mut a,
        now,
        2,
        RaftMessage::RequestVoteResp(RequestVoteResp {
            term,
            vote_granted: false,
        }),
    );
    assert_eq!(a.role(), Role::Candidate);
}

#[test]
fn duplicated_vote_grants_cannot_manufacture_a_quorum() {
    // Five nodes: one voter answering twice must not add up to three votes.
    let mut n = node(0, 5);
    let mut now = n.next_deadline();
    n.step(Input::Tick, now);
    let term = n.current_term();
    for _ in 0..4 {
        recv(
            &mut n,
            now,
            1,
            RaftMessage::RequestVoteResp(RequestVoteResp {
                term,
                vote_granted: true,
            }),
        );
    }
    assert_eq!(
        n.role(),
        Role::Candidate,
        "one voter is not a majority of five"
    );
    now += 1;
    recv(
        &mut n,
        now,
        2,
        RaftMessage::RequestVoteResp(RequestVoteResp {
            term,
            vote_granted: true,
        }),
    );
    assert_eq!(
        n.role(),
        Role::Leader,
        "three distinct votes is a majority of five"
    );
}

// ---------------------------------------------------------------------------
// §5.3 -- log replication
// ---------------------------------------------------------------------------

#[test]
fn the_consistency_check_rejects_a_gap() {
    let mut n = node(0, 3);
    // Leader claims entry 4 follows entry 3, but we have nothing.
    let outs = recv(
        &mut n,
        0,
        1,
        append_entries(1, 1, 3, 1, vec![entry(4, 1)], 0),
    );
    let reply = append_reply(&outs);
    assert!(!reply.success);
    let hint = reply.conflict.expect("a rejection should explain itself");
    assert_eq!(hint.term, None, "our log is simply too short");
    assert_eq!(hint.first_index, 1);
    assert_eq!(n.log().last_index(), 0, "nothing should have been appended");
}

#[test]
fn a_conflicting_entry_is_truncated_and_replaced() {
    let mut n = node(0, 3);
    recv(
        &mut n,
        0,
        1,
        append_entries(2, 1, 0, 0, vec![entry(1, 1), entry(2, 1), entry(3, 2)], 0),
    );
    assert_eq!(n.log().last_index(), 3);

    // A new leader in term 3 overwrites from index 2 on.
    let outs = recv(
        &mut n,
        0,
        2,
        append_entries(3, 2, 1, 1, vec![entry(2, 3)], 0),
    );
    assert!(append_reply(&outs).success);
    assert_eq!(n.log().last_index(), 2);
    assert_eq!(n.log().term_at(2), Some(3));
    assert!(
        outs.iter()
            .any(|o| matches!(o, Output::Persist(PersistOp::TruncateFrom(2)))),
        "the truncation must reach the disk: {outs:?}"
    );
}

#[test]
fn a_duplicated_append_entries_does_not_truncate() {
    // THE ONE THAT BITES: a delayed duplicate arrives after the log has moved
    // on. Truncating unconditionally here would delete committed entries.
    let mut n = node(0, 3);
    let msg = append_entries(1, 1, 0, 0, vec![entry(1, 1), entry(2, 1)], 0);
    recv(&mut n, 0, 1, msg.clone());
    // The log then grows.
    recv(
        &mut n,
        1,
        1,
        append_entries(1, 1, 2, 1, vec![entry(3, 1), entry(4, 1)], 4),
    );
    assert_eq!(n.log().last_index(), 4);
    assert_eq!(n.commit_index(), 4);

    // Now the original message arrives again.
    let outs = recv(&mut n, 2, 1, msg);
    assert!(append_reply(&outs).success);
    assert_eq!(
        n.log().last_index(),
        4,
        "a duplicate must not shorten the log"
    );
    assert_eq!(
        n.commit_index(),
        4,
        "a duplicate must not lower the commit index"
    );
    assert!(
        !outs
            .iter()
            .any(|o| matches!(o, Output::Persist(PersistOp::TruncateFrom(_)))),
        "a duplicate must not truncate: {outs:?}"
    );
    assert_eq!(
        append_reply(&outs).match_index,
        2,
        "the reply describes what this message established, not the whole log"
    );
}

#[test]
fn a_follower_never_commits_past_what_it_holds() {
    let mut n = node(0, 3);
    // The leader says it has committed through index 10, but only sends us 2.
    let outs = recv(
        &mut n,
        0,
        1,
        append_entries(1, 1, 0, 0, vec![entry(1, 1), entry(2, 1)], 10),
    );
    assert!(append_reply(&outs).success);
    assert_eq!(
        n.commit_index(),
        2,
        "commitIndex = min(leaderCommit, index of last new entry)"
    );
}

#[test]
fn an_empty_heartbeat_still_advances_the_commit_index() {
    let mut n = node(0, 3);
    recv(
        &mut n,
        0,
        1,
        append_entries(1, 1, 0, 0, vec![entry(1, 1), entry(2, 1)], 0),
    );
    assert_eq!(n.commit_index(), 0);
    recv(&mut n, 1, 1, append_entries(1, 1, 2, 1, vec![], 2));
    assert_eq!(n.commit_index(), 2);
}

#[test]
fn the_conflict_hint_lets_the_leader_skip_a_whole_term() {
    // Leader has [t1, t1, t4, t4]; follower reports a run of term 2.
    let mut leader = node(0, 3);
    let mut now = 0;
    recv(
        &mut leader,
        now,
        1,
        append_entries(1, 1, 0, 0, vec![entry(1, 1), entry(2, 1)], 0),
    );
    let term = elect(&mut leader, &mut now, &[1]);
    assert_eq!(leader.log().last_index(), 3, "election appends a no-op");

    // The follower rejects, reporting term 2 starting at index 2. The leader
    // has no term-2 entry at all, so it should jump straight back to index 2 --
    // not walk back one index at a time.
    let outs = recv(
        &mut leader,
        now,
        2,
        RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term,
            success: false,
            match_index: 0,
            conflict: Some(raft::ConflictHint {
                term: Some(2),
                first_index: 2,
            }),
            probed_index: 2,
            read_round: 0,
        }),
    );
    let retries = sent_appends(&outs);
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].0, 2);
    assert_eq!(
        retries[0].1.prev_log_index, 1,
        "nextIndex should have dropped to 2, so prevLogIndex is 1"
    );
}

#[test]
fn a_conflicting_term_the_leader_also_has_resumes_after_its_last_entry() {
    // Leader's log ends [1@t1, 2@t2, 3@t2, 4@t2] and it probes at index 4.
    // The follower has term 1 where the leader has term 2, so it reports
    // term 1 starting at index 1. The leader holds term 1 only at index 1, so
    // it should resume at index 2 -- one round trip to skip three entries,
    // instead of three round trips.
    let mut leader = node(0, 3);
    let mut now = 0;
    recv(
        &mut leader,
        now,
        1,
        append_entries(
            2,
            1,
            0,
            0,
            vec![entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 2)],
            0,
        ),
    );
    let term = elect(&mut leader, &mut now, &[1]);
    assert_eq!(
        leader.next_index().get(&2),
        Some(&5),
        "probing just past the inherited log"
    );

    let outs = recv(
        &mut leader,
        now,
        2,
        RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term,
            success: false,
            match_index: 0,
            conflict: Some(raft::ConflictHint {
                term: Some(1),
                first_index: 1,
            }),
            probed_index: 4,
            read_round: 0,
        }),
    );
    let retries = sent_appends(&outs);
    assert_eq!(retries.len(), 1);
    assert_eq!(
        retries[0].1.prev_log_index, 1,
        "the leader's term-1 run ends at index 1, so everything up to there agrees"
    );
    assert_eq!(retries[0].1.prev_log_term, 1);
}

/// A duplicated rejection describes a guess the leader has already corrected.
/// Acting on it twice would walk `nextIndex` back a second time for the same
/// information, turning the conflict-hint optimization back into a crawl the
/// moment the network starts duplicating.
#[test]
fn a_duplicated_rejection_does_not_move_next_index_twice() {
    let mut leader = node(0, 3);
    let mut now = 0;
    recv(
        &mut leader,
        now,
        1,
        append_entries(
            2,
            1,
            0,
            0,
            vec![entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 2)],
            0,
        ),
    );
    let term = elect(&mut leader, &mut now, &[1]);
    assert_eq!(leader.next_index().get(&2), Some(&5));

    let rejection = RaftMessage::AppendEntriesResp(AppendEntriesResp {
        term,
        success: false,
        match_index: 0,
        conflict: Some(raft::ConflictHint {
            term: Some(1),
            first_index: 1,
        }),
        probed_index: 4,
        read_round: 0,
    });

    recv(&mut leader, now, 2, rejection.clone());
    let after_first = leader.next_index().get(&2).copied().unwrap();
    assert_eq!(
        after_first, 2,
        "the hint should have skipped back to index 2"
    );

    // The same rejection arrives again, delayed or duplicated by the network.
    let outs = recv(&mut leader, now, 2, rejection);
    assert_eq!(
        leader.next_index().get(&2).copied().unwrap(),
        after_first,
        "a duplicated rejection must not move nextIndex again"
    );
    assert!(
        sent_appends(&outs).is_empty(),
        "and must not trigger another probe: {outs:?}"
    );
}

#[test]
fn match_index_never_regresses_on_a_reordered_response() {
    let mut leader = node(0, 3);
    let mut now = 0;
    let term = elect(&mut leader, &mut now, &[1, 2]);
    // Push the log out to a few entries.
    for i in 0..3u64 {
        leader.step(
            Input::ClientRequest(raft::ClientRequest {
                client: 1,
                seq: i,
                read_only: false,
                command: RaftNode::command(format!("c{i}")),
            }),
            now,
        );
    }
    let last = leader.log().last_index();

    let ok = |m: Index| {
        RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term,
            success: true,
            match_index: m,
            conflict: None,
            probed_index: m.saturating_sub(1),
            read_round: 0,
        })
    };
    recv(&mut leader, now, 1, ok(last));
    assert_eq!(leader.match_index().get(&1), Some(&last));
    // An older response arrives late.
    recv(&mut leader, now, 1, ok(1));
    assert_eq!(
        leader.match_index().get(&1),
        Some(&last),
        "a stale response must not pull matchIndex backwards"
    );
}

// ---------------------------------------------------------------------------
// §5.4.2 -- the commit rule
// ---------------------------------------------------------------------------

/// The single most commonly botched rule in Raft.
///
/// A leader inherits an uncommitted entry from a previous term and sees it
/// replicated on a majority. It must NOT commit it by replica count: Figure 8
/// shows such an entry being overwritten afterwards. It may only commit once an
/// entry of its OWN term reaches a majority, which then commits the earlier one
/// indirectly.
#[test]
fn a_leader_may_not_commit_a_prior_term_entry_by_counting_replicas() {
    let mut leader = node(0, 3);
    let mut now = 0;

    // Inherit one uncommitted entry from term 1.
    recv(
        &mut leader,
        now,
        1,
        append_entries(1, 1, 0, 0, vec![entry(1, 1)], 0),
    );
    assert_eq!(leader.commit_index(), 0);

    // Win term 2. The election appends a no-op at index 2.
    let term = elect(&mut leader, &mut now, &[1]);
    assert_eq!(leader.log().last_index(), 2);
    assert_eq!(leader.log().term_at(1), Some(1));
    assert_eq!(leader.log().term_at(2), Some(term));

    // A follower acknowledges index 1 only. That is a majority (the follower
    // plus ourselves) for an entry from term 1.
    recv(
        &mut leader,
        now,
        1,
        RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term,
            success: true,
            match_index: 1,
            conflict: None,
            probed_index: 0,
            read_round: 0,
        }),
    );
    assert_eq!(
        leader.commit_index(),
        0,
        "committing a prior-term entry by replica count is the Figure 8 safety bug"
    );

    // Now the follower acknowledges the no-op, an entry of the current term.
    recv(
        &mut leader,
        now,
        1,
        RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term,
            success: true,
            match_index: 2,
            conflict: None,
            probed_index: 1,
            read_round: 0,
        }),
    );
    assert_eq!(
        leader.commit_index(),
        2,
        "a current-term entry on a majority commits, and carries the earlier entry with it"
    );
}

#[test]
fn a_leader_commits_its_own_term_entries_on_a_majority() {
    let mut leader = node(0, 5);
    let mut now = 0;
    let term = elect(&mut leader, &mut now, &[1, 2]);
    // Index 1 is the election no-op.
    assert_eq!(leader.commit_index(), 0);

    let ok = |m: Index| {
        RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term,
            success: true,
            match_index: m,
            conflict: None,
            probed_index: m.saturating_sub(1),
            read_round: 0,
        })
    };
    recv(&mut leader, now, 1, ok(1));
    assert_eq!(leader.commit_index(), 0, "two of five is not a majority");
    recv(&mut leader, now, 2, ok(1));
    assert_eq!(leader.commit_index(), 1, "three of five is");
}

#[test]
fn a_new_leader_appends_a_noop_of_its_own_term() {
    let mut n = node(0, 3);
    let mut now = 0;
    let term = elect(&mut n, &mut now, &[1]);
    let last = n.log().last_log_id();
    assert_eq!(last.index, 1);
    assert_eq!(last.term, term);
    assert!(matches!(
        n.log().get(1).unwrap().payload,
        EntryPayload::Noop
    ));
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

#[test]
fn a_candidate_concedes_to_a_leader_of_the_same_term() {
    let mut n = node(0, 3);
    let mut now = n.next_deadline();
    n.step(Input::Tick, now);
    assert_eq!(n.role(), Role::Candidate);
    let term = n.current_term();

    now += 1;
    let outs = recv(
        &mut n,
        now,
        1,
        append_entries(term, 1, 0, 0, vec![entry(1, term)], 0),
    );
    assert_eq!(n.role(), Role::Follower, "§5.2: someone else won this term");
    assert_eq!(n.leader_id(), Some(1));
    assert!(
        append_reply(&outs).success,
        "and the entries are still processed"
    );
    assert_eq!(n.log().last_index(), 1);
}

#[test]
fn a_leader_heartbeats_on_its_own_schedule() {
    let mut n = node(0, 3);
    let mut now = 0;
    let term = elect(&mut n, &mut now, &[1]);
    let deadline = n.next_deadline();
    assert!(
        deadline > now,
        "the heartbeat must be scheduled in the future"
    );

    // Before anyone has acknowledged, a "heartbeat" still carries the election
    // no-op: the leader keeps retrying an entry it has no acknowledgement for.
    let outs = n.step(Input::Tick, deadline);
    let sends = sent_appends(&outs);
    assert_eq!(sends.len(), 2, "one message per peer: {outs:?}");
    assert!(
        sends.iter().all(|(_, r)| r.entries.len() == 1),
        "an unacknowledged entry is retried, not dropped"
    );

    // Once both followers are caught up, heartbeats go empty.
    let last = n.log().last_index();
    for peer in [1, 2] {
        recv(
            &mut n,
            deadline,
            peer,
            RaftMessage::AppendEntriesResp(AppendEntriesResp {
                term,
                success: true,
                match_index: last,
                conflict: None,
                probed_index: last.saturating_sub(1),
                read_round: 0,
            }),
        );
    }
    let next = n.next_deadline();
    let outs = n.step(Input::Tick, next);
    let sends = sent_appends(&outs);
    assert_eq!(sends.len(), 2);
    assert!(
        sends.iter().all(|(_, r)| r.entries.is_empty()),
        "a caught-up follower gets an empty AppendEntries: {outs:?}"
    );
}

#[test]
fn a_follower_that_hears_from_the_leader_does_not_campaign() {
    let mut n = node(0, 3);
    let mut now = 0;
    let mut term = 1;
    // Heartbeat well inside the election timeout, repeatedly.
    for _ in 0..50 {
        now += 40;
        recv(&mut n, now, 1, append_entries(term, 1, 0, 0, vec![], 0));
        n.step(Input::Tick, now);
        term = n.current_term();
    }
    assert_eq!(n.role(), Role::Follower);
    assert_eq!(
        n.current_term(),
        1,
        "the term must not climb while a leader is alive"
    );
}

#[test]
fn a_follower_with_no_leader_eventually_campaigns() {
    let mut n = node(0, 3);
    let deadline = n.next_deadline();
    n.step(Input::Tick, deadline - 1);
    assert_eq!(n.role(), Role::Follower, "not yet");
    n.step(Input::Tick, deadline);
    assert_eq!(n.role(), Role::Candidate);
    assert_eq!(n.current_term(), 1);
    assert_eq!(n.voted_for(), Some(0), "a candidate votes for itself");
}

#[test]
fn election_timeouts_are_randomized_but_reproducible() {
    let spread: std::collections::BTreeSet<Tick> =
        (0..8).map(|id| node(id, 8).next_deadline()).collect();
    assert!(
        spread.len() > 1,
        "identical timeouts would never resolve a split vote"
    );

    let a: Vec<Tick> = (0..8).map(|id| node(id, 8).next_deadline()).collect();
    let b: Vec<Tick> = (0..8).map(|id| node(id, 8).next_deadline()).collect();
    assert_eq!(a, b, "same seed, same timeouts");
}

#[test]
fn a_client_request_to_a_follower_is_redirected_not_appended() {
    let mut n = node(0, 3);
    recv(&mut n, 0, 1, append_entries(1, 1, 0, 0, vec![], 0));
    let outs = n.step(
        Input::ClientRequest(raft::ClientRequest {
            client: 7,
            seq: 0,
            read_only: false,
            command: RaftNode::command("x"),
        }),
        1,
    );
    assert_eq!(
        n.log().last_index(),
        0,
        "a follower must not append client commands"
    );
    match outs.as_slice() {
        [Output::ClientResponse {
            client: 7,
            seq: 0,
            result,
        }] => {
            assert_eq!(*result, raft::ClientResult::NotLeader { leader: Some(1) });
        }
        other => panic!("unexpected outputs: {other:?}"),
    }
}
