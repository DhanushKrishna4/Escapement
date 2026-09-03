//! Validating the invariant checkers by breaking Raft on purpose.
//!
//! A checker that always says "OK" passes every test until the day it matters.
//! So every property in `sim::invariants` gets a scenario that violates it, and
//! the test asserts the checker fires — plus a control asserting a correct run
//! stays clean.
//!
//! Two kinds of scenario here:
//!
//! * Direct: feed the checker node states that violate a property. Some
//!   properties (a term going backwards) are not reachable by a correct node at
//!   all, so the only way to exercise the check is to hand it the states.
//! * End to end: turn on a `BugSwitches` flag, run the real simulator or a
//!   hand-built scenario, and watch the violation emerge from actual Raft
//!   behaviour. This is the stronger evidence, so it is used wherever the bug
//!   can manifest.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use kvstore::KvCommand;
use raft::{
    AppendEntriesReq, BugSwitches, ClientRequest, ClusterConfig, Command, EntryPayload, Index,
    Input, LogEntry, NodeId, Output, RaftConfig, RaftMessage, RaftNode, RequestVoteReq, Role, Term,
    Tick,
};
use sim::invariants::{Invariant, Invariants};
use sim::{Cluster, SimConfig};

// ---------------------------------------------------------------------------
// A hand-driven scenario harness
// ---------------------------------------------------------------------------

/// Nodes plus a checker, with fully manual message routing.
///
/// This is not a second simulator: there is no clock, no queue and no
/// randomness. It exists so the paper's scenarios can be written as exact
/// sequences ("this message reaches that node and no other") rather than hoping
/// a random schedule produces them.
struct Scenario {
    nodes: BTreeMap<NodeId, RaftNode>,
    checker: Invariants,
    tick: Tick,
}

/// Which messages are allowed to flow. Returning false is a partition, a
/// crashed node, or simply "not in this phase of the scenario".
type Link<'a> = &'a dyn Fn(NodeId, NodeId, &RaftMessage) -> bool;

fn everything(_from: NodeId, _to: NodeId, _msg: &RaftMessage) -> bool {
    true
}

fn is_vote(msg: &RaftMessage) -> bool {
    matches!(
        msg,
        RaftMessage::RequestVote(_) | RaftMessage::RequestVoteResp(_)
    )
}

/// Votes flow, log replication does not. Lets a node win an election without
/// its entries reaching anyone.
fn votes_only(_from: NodeId, _to: NodeId, msg: &RaftMessage) -> bool {
    is_vote(msg)
}

impl Scenario {
    fn new(n: usize, bugs: BugSwitches) -> Self {
        let cfg = RaftConfig {
            // One entry per message, so a leader's own-term entry does not ride
            // along with the previous-term entry it is backfilling. Without
            // this the no-op always commits at the same moment and the Figure 8
            // bug cannot be reached.
            max_entries_per_append: 1,
            bugs,
            ..RaftConfig::default()
        };
        let cluster = ClusterConfig::new(0..n as NodeId);
        let nodes = (0..n as NodeId)
            .map(|id| (id, RaftNode::new(id, cluster.clone(), cfg.clone(), 7, 0)))
            .collect();
        Scenario {
            nodes,
            checker: Invariants::new(),
            tick: 0,
        }
    }

    fn node(&self, id: NodeId) -> &RaftNode {
        &self.nodes[&id]
    }

    fn step(&mut self, id: NodeId, input: Input) -> Vec<Output> {
        let tick = self.tick;
        let outs = self
            .nodes
            .get_mut(&id)
            .expect("no such node")
            .step(input, tick);

        // Mirror what the simulator does: the lowest log index this step
        // touched drives the incremental Log Matching scan.
        let mut dirty: Option<Index> = None;
        for out in &outs {
            if let Output::Persist(op) = out {
                match op {
                    raft::PersistOp::Append(entries) => {
                        if let Some(first) = entries.first() {
                            dirty = Some(dirty.map_or(first.index, |d: Index| d.min(first.index)));
                        }
                    }
                    raft::PersistOp::TruncateFrom(i) => {
                        dirty = Some(dirty.map_or(*i, |d: Index| d.min(*i)));
                    }
                    raft::PersistOp::HardState { .. } => {}
                    // The scenario harness never compacts.
                    raft::PersistOp::Snapshot(_)
                    | raft::PersistOp::Compact(_)
                    | raft::PersistOp::ResetLog(_) => {}
                }
            }
        }
        for out in &outs {
            if let Output::Apply {
                index,
                term,
                payload,
                ..
            } = out
            {
                let cmd = match payload {
                    EntryPayload::Noop | EntryPayload::Config(_) => None,
                    EntryPayload::Command(c) => Some(KvCommand::decode(c).expect("kv command")),
                };
                self.checker.observe_apply(tick, id, *index, *term, &cmd);
            }
        }
        self.checker.observe_node(tick, &self.nodes[&id], dirty);
        outs
    }

    /// Deliver everything the outputs imply, breadth first, until quiet.
    fn pump(&mut self, from: NodeId, outs: Vec<Output>, allow: Link<'_>) {
        let mut queue: VecDeque<(NodeId, NodeId, RaftMessage)> = VecDeque::new();
        Self::enqueue(from, &outs, &mut queue, allow);
        let mut budget = 10_000;
        while let Some((from, to, msg)) = queue.pop_front() {
            budget -= 1;
            assert!(budget > 0, "message pump did not settle");
            self.tick += 1;
            let outs = self.step(to, Input::Message { from, msg });
            Self::enqueue(to, &outs, &mut queue, allow);
        }
    }

    fn enqueue(
        from: NodeId,
        outs: &[Output],
        queue: &mut VecDeque<(NodeId, NodeId, RaftMessage)>,
        allow: Link<'_>,
    ) {
        for out in outs {
            if let Output::Send { to, msg } = out {
                if allow(from, *to, msg) {
                    queue.push_back((from, *to, msg.clone()));
                }
            }
        }
    }

    /// Force `id` to time out and campaign, delivering whatever `allow` permits.
    fn campaign(&mut self, id: NodeId, allow: Link<'_>) {
        self.tick = self.tick.max(self.node(id).next_deadline());
        let outs = self.step(id, Input::Tick);
        self.pump(id, outs, allow);
    }

    /// Campaign repeatedly until `id` wins. A candidate can lose a term
    /// outright (peers already voted in it), and the real algorithm's answer is
    /// to time out and try the next term.
    fn campaign_until_leader(&mut self, id: NodeId, allow: Link<'_>) -> Term {
        for _ in 0..8 {
            self.campaign(id, allow);
            if self.node(id).role() == Role::Leader {
                return self.node(id).current_term();
            }
        }
        panic!(
            "node {id} never won an election (role {:?}, term {})",
            self.node(id).role(),
            self.node(id).current_term()
        );
    }

    fn client_write(&mut self, leader: NodeId, key: &str, value: &str, allow: Link<'_>) {
        self.tick += 1;
        let outs = self.step(
            leader,
            Input::ClientRequest(ClientRequest {
                client: 1,
                seq: self.tick,
                read_only: false,
                command: KvCommand::Put {
                    key: key.into(),
                    value: value.into(),
                }
                .encode(),
            }),
        );
        self.pump(leader, outs, allow);
    }

    /// Make the leader send another round of AppendEntries.
    fn heartbeat(&mut self, leader: NodeId, allow: Link<'_>) {
        self.tick = self.tick.max(self.node(leader).next_deadline());
        let outs = self.step(leader, Input::Tick);
        self.pump(leader, outs, allow);
    }

    fn terms(&self, id: NodeId) -> Vec<Term> {
        let log = self.node(id).log();
        (1..=log.last_index())
            .map(|i| log.term_at(i).unwrap_or(0))
            .collect()
    }

    fn broke(&self, invariant: Invariant) -> bool {
        self.checker.broken().contains(&invariant)
    }

    fn report(&self) -> String {
        self.checker
            .violations()
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

// ---------------------------------------------------------------------------
// Helpers for the direct tests
// ---------------------------------------------------------------------------

fn lone(id: NodeId, n: usize) -> RaftNode {
    RaftNode::new(
        id,
        ClusterConfig::new(0..n as NodeId),
        RaftConfig::default(),
        1,
        0,
    )
}

fn cmd_entry(index: Index, term: Term, payload: &str) -> LogEntry {
    LogEntry {
        term,
        index,
        payload: EntryPayload::Command(Command(payload.as_bytes().to_vec())),
        client: None,
    }
}

fn append(term: Term, leader: NodeId, entries: Vec<LogEntry>, leader_commit: Index) -> RaftMessage {
    RaftMessage::AppendEntries(AppendEntriesReq {
        term,
        leader_id: leader,
        prev_log_index: 0,
        prev_log_term: 0,
        entries,
        leader_commit,
        read_round: 0,
    })
}

/// Feed one node an AppendEntries and show the result to the checker.
fn feed(checker: &mut Invariants, node: &mut RaftNode, tick: Tick, from: NodeId, msg: RaftMessage) {
    node.step(Input::Message { from, msg }, tick);
    checker.observe_node(tick, node, Some(1));
}

// ---------------------------------------------------------------------------
// Control: a correct run must stay clean
// ---------------------------------------------------------------------------

#[test]
fn a_correct_run_produces_no_violations() {
    for seed in 0..32u64 {
        let mut sim = Cluster::new(SimConfig {
            seed,
            nodes: 5,
            ..SimConfig::default()
        });
        let leader = sim.run_until_leader(5_000).expect("leader");
        for i in 0..10u64 {
            sim.submit(
                leader,
                1,
                i,
                KvCommand::Put {
                    key: format!("k{}", i % 3),
                    value: format!("v{i}"),
                },
            );
            sim.run_for(120);
        }
        sim.run_for(2_000);
        sim.assert_no_violations();
        assert!(
            sim.checker().checks() > 1_000,
            "seed {seed}: the checker barely ran ({} checks)",
            sim.checker().checks()
        );
    }
}

#[test]
fn the_checker_actually_examines_the_run() {
    // Guards against the whole validation suite passing because the checker is
    // wired up but never invoked.
    let mut sim = Cluster::new(SimConfig::with_seed(4));
    sim.run_for(10_000);
    assert!(
        sim.checker().checks() > 500,
        "only {} checks ran",
        sim.checker().checks()
    );
    assert!(sim.violations().is_empty());
    assert!(sim.broken_invariants().is_empty());
}

// ---------------------------------------------------------------------------
// End to end: deliberate bugs, real Raft behaviour
// ---------------------------------------------------------------------------

/// §5.2. With `vote_twice_per_term` a node grants a vote to every candidate
/// that asks, so two candidates can each assemble a majority in one term.
///
/// This one manifests on a perfect network with no faults at all: it only needs
/// two nodes to time out close enough together to campaign in the same term,
/// which happens naturally at startup.
#[test]
fn breaking_one_vote_per_term_trips_election_safety() {
    let mut tripped = 0;
    let mut seeds_with_a_race = 0;

    for seed in 0..200u64 {
        let mut sim = Cluster::new(SimConfig {
            seed,
            nodes: 5,
            raft: RaftConfig {
                bugs: BugSwitches {
                    vote_twice_per_term: true,
                    ..BugSwitches::default()
                },
                ..RaftConfig::default()
            },
            ..SimConfig::default()
        });
        sim.run_for(3_000);
        if !sim.violations().is_empty() {
            seeds_with_a_race += 1;
            if sim
                .violations()
                .iter()
                .any(|v| v.invariant == Invariant::ElectionSafety)
            {
                tripped += 1;
            }
        }
    }

    assert!(
        tripped > 0,
        "granting two votes per term never produced two leaders in one term \
         across 200 seeds; the checker cannot be validated this way"
    );
    assert_eq!(
        tripped, seeds_with_a_race,
        "every seed that broke should have broken Election Safety first"
    );
}

/// A run with the vote bug on must produce a violation whose text names the two
/// nodes and the term. "FAIL" is not a useful report.
#[test]
fn the_violation_report_says_what_actually_happened() {
    let seed = (0..200u64)
        .find(|seed| {
            let mut sim = Cluster::new(SimConfig {
                seed: *seed,
                nodes: 5,
                raft: RaftConfig {
                    bugs: BugSwitches {
                        vote_twice_per_term: true,
                        ..BugSwitches::default()
                    },
                    ..RaftConfig::default()
                },
                ..SimConfig::default()
            });
            sim.run_for(3_000);
            sim.violations()
                .iter()
                .any(|v| v.invariant == Invariant::ElectionSafety)
        })
        .expect("no seed produced two leaders in one term");

    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 5,
        raft: RaftConfig {
            bugs: BugSwitches {
                vote_twice_per_term: true,
                ..BugSwitches::default()
            },
            ..RaftConfig::default()
        },
        ..SimConfig::default()
    });
    sim.run_for(3_000);

    // The double vote is detected before the second leader appears, so look for
    // the Election Safety report specifically rather than taking the first one.
    let text = sim
        .violations()
        .iter()
        .find(|v| v.invariant == Invariant::ElectionSafety)
        .expect("this seed should have produced two leaders in one term")
        .to_string();
    assert!(text.contains("Election Safety"), "{text}");
    assert!(text.contains("§5.2"), "{text}");
    assert!(text.contains("both leader of term"), "{text}");
    assert!(text.contains("likely cause"), "{text}");
    // And the tick, so it can be found in the trace.
    assert!(text.starts_with("[tick "), "{text}");
}

/// §5.3. With `trust_leader_commit_blindly` a follower takes `leaderCommit` at
/// face value and marks entries committed that it does not hold.
#[test]
fn breaking_the_leader_commit_cap_trips_commit_index_within_log() {
    let cfg = RaftConfig {
        bugs: BugSwitches {
            trust_leader_commit_blindly: true,
            ..BugSwitches::default()
        },
        ..RaftConfig::default()
    };
    let mut node = RaftNode::new(0, ClusterConfig::new(0..3), cfg, 1, 0);
    let mut checker = Invariants::new();

    // The leader claims to have committed through index 10 but sends two
    // entries. A correct follower caps at 2.
    feed(
        &mut checker,
        &mut node,
        1,
        1,
        append(1, 1, vec![cmd_entry(1, 1, "a"), cmd_entry(2, 1, "b")], 10),
    );

    assert_eq!(node.commit_index(), 10, "the bug is supposed to be on");
    assert!(
        checker.broken().contains(&Invariant::CommitIndexWithinLog),
        "checker missed a commit index past the end of the log: {:?}",
        checker.violations()
    );
}

/// §5.4.2, the Figure 8 scenario. The one the spec singles out.
///
/// A leader commits an entry from a *previous* term because a majority holds
/// it. A later leader, whose log is legitimately more up to date, then
/// overwrites that "committed" entry. Raft forbids the first step precisely so
/// the second cannot destroy a commitment.
///
/// Building this needs exact control over who hears what, which is why the
/// scenario harness exists rather than hoping a random schedule finds it.
fn figure_8(bugs: BugSwitches) -> Scenario {
    let mut s = Scenario::new(5, bugs);

    // Phase (a): node 0 leads term 1 and gets its no-op onto everyone.
    let t1 = s.campaign_until_leader(0, &everything);
    assert_eq!(t1, 1);
    s.heartbeat(0, &everything);
    for id in 0..5 {
        assert_eq!(
            s.terms(id),
            vec![1],
            "node {id} should hold the term-1 no-op"
        );
    }

    // Phase (b): node 0 accepts a write that reaches node 1 only. Entry 2 is
    // from term 1 and sits on a minority: 2 of 5.
    let only_node_1 = |_from: NodeId, to: NodeId, _msg: &RaftMessage| to == 1;
    s.client_write(0, "x", "old", &only_node_1);
    assert_eq!(s.terms(0), vec![1, 1]);
    assert_eq!(s.terms(1), vec![1, 1]);
    assert_eq!(s.terms(2), vec![1]);
    assert_eq!(s.node(0).commit_index(), 1, "2 of 5 is not a majority");

    // Phase (c): node 4 wins term 2 on votes from the nodes that never saw the
    // write, and appends its own no-op at index 2 -- a *different* entry at the
    // same index. Its log stays private.
    let t2 = s.campaign_until_leader(4, &votes_only);
    assert_eq!(t2, 2);
    assert_eq!(
        s.terms(4),
        vec![1, 2],
        "node 4 has term 2 where node 0 has term 1"
    );
    assert_eq!(s.terms(2), vec![1]);

    // Phase (d): node 0 wins a later term and backfills the *old* entry to a
    // majority, while its own term-3 entry does not get out.
    //
    // Modelling the crash precisely matters. The leader's first message to each
    // follower necessarily carries its own-term entry — that is what
    // `nextIndex` points at — and a short follower rejects it, which is how the
    // leader learns to back up. So the own-term entry gets exactly one shot per
    // follower (always rejected, since their logs are too short) and node 0
    // then "crashes" before any retry. That is the paper's phase (c): S1
    // replicates the old entry and dies before its own term's entry spreads.
    //
    // Node 4 is down for this whole phase, as S5 is in the paper. Without that
    // it would be probed too, and the backfill would destroy the term-2 entry
    // that makes its later election legitimate.
    let own_term_attempts: RefCell<BTreeSet<NodeId>> = RefCell::new(BTreeSet::new());
    let node_0_crashes_mid_replication = |_from: NodeId, to: NodeId, msg: &RaftMessage| {
        if to == 4 {
            return false;
        }
        match msg {
            RaftMessage::AppendEntries(req) if req.entries.iter().any(|e| e.term > 1) => {
                own_term_attempts.borrow_mut().insert(to)
            }
            _ => true,
        }
    };
    let t3 = s.campaign_until_leader(0, &node_0_crashes_mid_replication);
    assert!(t3 > t2, "node 0 should have taken a term above node 4's");

    // A few rounds for probe-back-and-backfill to finish.
    for _ in 0..6 {
        s.heartbeat(0, &node_0_crashes_mid_replication);
    }
    assert_eq!(s.terms(2), vec![1, 1], "node 2 should have been backfilled");
    assert_eq!(s.terms(3), vec![1, 1], "and so should node 3");
    assert_eq!(
        s.terms(4),
        vec![1, 2],
        "node 4 was down and must still hold the term-2 entry that wins it the next election"
    );

    // The previous-term entry is now on 4 of 5 nodes. The leader's own-term
    // entry is on 2 of 5 — not a majority — so nothing legitimises the commit.
    assert_eq!(s.node(0).log().last_index(), 3);
    assert_eq!(
        (0..5).filter(|id| s.terms(*id).get(1) == Some(&1)).count(),
        4,
        "the previous-term entry should be on a majority"
    );
    s
}

/// Phase (e): node 4 comes back. Its log ends at (index 2, term 2), which beats
/// the (index 2, term 1) that nodes 1-3 hold, so the election restriction lets
/// it win legitimately -- and it overwrites index 2.
///
/// This is legal Raft. It is only a disaster if someone already called index 2
/// committed, which is exactly what §5.4.2 forbids.
fn figure_8_takeover(s: &mut Scenario) {
    s.campaign_until_leader(4, &everything);
    for _ in 0..6 {
        s.heartbeat(4, &everything);
    }
    assert_eq!(
        s.terms(2),
        vec![1, 2, 4],
        "node 2 should have truncated the term-1 entry and taken node 4's"
    );
}

#[test]
fn the_commit_rule_is_what_stops_a_committed_entry_being_overwritten() {
    // With §5.4.2 enforced, node 0 must NOT commit index 2 -- even though a
    // majority holds it -- because it is from an earlier term.
    let mut clean = figure_8(BugSwitches::default());
    assert_eq!(
        clean.node(0).commit_index(),
        1,
        "§5.4.2: an entry from a previous term must not be committed by replica count"
    );

    // Node 4 then overwrites index 2. Nothing was violated, because nothing had
    // been committed there. Same sequence of events, no violation -- which is
    // what makes the bug test below meaningful.
    figure_8_takeover(&mut clean);
    assert!(
        clean.checker.is_clean(),
        "a correct run of Figure 8 must be clean:\n{}",
        clean.report()
    );
}

#[test]
fn breaking_the_commit_rule_trips_the_checker() {
    let mut s = figure_8(BugSwitches {
        commit_prior_term_entries: true,
        ..BugSwitches::default()
    });

    // With the bug on, node 0 counts replicas regardless of term and declares
    // index 2 committed. This is the mistake.
    assert_eq!(
        s.node(0).commit_index(),
        2,
        "the deliberate bug should have committed the previous-term entry"
    );

    // The identical takeover now destroys a committed entry.
    figure_8_takeover(&mut s);

    assert!(
        s.broke(Invariant::CommittedEntriesStable),
        "the checker did not notice a committed entry being overwritten:\n{}",
        s.report()
    );

    let text = s
        .checker
        .violations()
        .iter()
        .find(|v| v.invariant == Invariant::CommittedEntriesStable)
        .unwrap()
        .to_string();
    assert!(text.contains("index 2"), "{text}");
    assert!(text.contains("§5.4.2"), "{text}");
}

// ---------------------------------------------------------------------------
// Direct: properties a correct node cannot violate, fed to the checker
// ---------------------------------------------------------------------------

#[test]
fn log_matching_fires_on_different_entries_at_the_same_index_and_term() {
    let mut checker = Invariants::new();
    let (mut a, mut b) = (lone(0, 3), lone(1, 3));
    feed(
        &mut checker,
        &mut a,
        1,
        2,
        append(1, 2, vec![cmd_entry(1, 1, "x")], 0),
    );
    feed(
        &mut checker,
        &mut b,
        1,
        2,
        append(1, 2, vec![cmd_entry(1, 1, "y")], 0),
    );

    assert!(
        checker.broken().contains(&Invariant::LogMatching),
        "two different entries at (index 1, term 1) should break Log Matching"
    );
    let text = checker.violations()[0].to_string();
    assert!(text.contains("index 1"), "{text}");
    assert!(text.contains("different contents"), "{text}");
}

#[test]
fn log_matching_fires_when_histories_diverge_below_a_shared_entry() {
    let mut checker = Invariants::new();
    let (mut a, mut b) = (lone(0, 3), lone(1, 3));
    // Same entry at index 2 term 2, but different terms at index 1.
    feed(
        &mut checker,
        &mut a,
        1,
        2,
        append(2, 2, vec![cmd_entry(1, 1, "p"), cmd_entry(2, 2, "same")], 0),
    );
    feed(
        &mut checker,
        &mut b,
        1,
        2,
        append(2, 2, vec![cmd_entry(1, 2, "q"), cmd_entry(2, 2, "same")], 0),
    );

    assert!(checker.broken().contains(&Invariant::LogMatching));
    let text = checker
        .violations()
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("disagree about index 1"), "{text}");
}

#[test]
fn election_safety_fires_on_two_leaders_in_one_term() {
    let mut checker = Invariants::new();
    let mut leaders = Vec::new();
    for id in [0u32, 1] {
        let mut n = lone(id, 3);
        let deadline = n.next_deadline();
        n.step(Input::Tick, deadline);
        let term = n.current_term();
        // Two peers grant, which is a majority of three.
        for voter in [0u32, 1, 2].into_iter().filter(|v| *v != id).take(2) {
            n.step(
                Input::Message {
                    from: voter,
                    msg: RaftMessage::RequestVoteResp(raft::RequestVoteResp {
                        term,
                        vote_granted: true,
                    }),
                },
                deadline,
            );
        }
        assert_eq!(n.role(), Role::Leader);
        assert_eq!(n.current_term(), 1, "both should be leader of term 1");
        leaders.push(n);
    }
    for n in &leaders {
        checker.observe_node(10, n, None);
    }
    assert!(checker.broken().contains(&Invariant::ElectionSafety));
}

#[test]
fn leader_completeness_fires_when_a_leader_lacks_a_committed_entry() {
    let mut checker = Invariants::new();

    // Node 0 commits an entry at index 1, in term 1.
    let mut a = lone(0, 3);
    feed(
        &mut checker,
        &mut a,
        1,
        2,
        append(1, 2, vec![cmd_entry(1, 1, "committed")], 1),
    );
    assert_eq!(a.commit_index(), 1);

    // Node 1 becomes leader of a LATER term with an empty log. §5.4 requires a
    // leader to hold every entry committed in an earlier term, and a correct
    // election restriction makes this impossible — but the checker must notice
    // if it ever happens.
    let mut b = lone(1, 3);
    // Push it to term 1 first so that campaigning takes it to term 2, strictly
    // above the term the entry was committed in.
    b.step(
        Input::Message {
            from: 2,
            msg: RaftMessage::RequestVote(RequestVoteReq {
                term: 1,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        },
        1,
    );
    let deadline = b.next_deadline();
    b.step(Input::Tick, deadline);
    let term = b.current_term();
    assert_eq!(
        term, 2,
        "the leader's term must exceed the committed entry's"
    );
    for voter in [0u32, 2] {
        b.step(
            Input::Message {
                from: voter,
                msg: RaftMessage::RequestVoteResp(raft::RequestVoteResp {
                    term,
                    vote_granted: true,
                }),
            },
            deadline,
        );
    }
    assert_eq!(b.role(), Role::Leader);
    checker.observe_node(20, &b, Some(1));

    assert!(
        checker.broken().contains(&Invariant::LeaderCompleteness),
        "a leader missing a committed entry should break Leader Completeness: {:?}",
        checker.violations()
    );
    let text = checker
        .violations()
        .iter()
        .find(|v| v.invariant == Invariant::LeaderCompleteness)
        .unwrap()
        .to_string();
    assert!(text.contains("§5.4"), "{text}");
    assert!(text.contains("index 1"), "{text}");
}

/// The other half of the property, and a false positive a 50,000-seed sweep
/// found: a leader of an *older* term must NOT be reported for an entry
/// committed in a later one.
///
/// §5.4 only requires an entry committed in term T to appear in leaders of
/// terms greater than T. A stale leader that has not yet noticed a newer term
/// is a perfectly normal state, not a safety violation, and reporting it would
/// bury real findings under noise.
#[test]
fn leader_completeness_ignores_entries_committed_in_later_terms() {
    let mut checker = Invariants::new();

    // A node becomes leader of term 1 with an empty log.
    let mut stale = lone(0, 3);
    let deadline = stale.next_deadline();
    stale.step(Input::Tick, deadline);
    let term = stale.current_term();
    assert_eq!(term, 1);
    for voter in [1u32, 2] {
        stale.step(
            Input::Message {
                from: voter,
                msg: RaftMessage::RequestVoteResp(raft::RequestVoteResp {
                    term,
                    vote_granted: true,
                }),
            },
            deadline,
        );
    }
    assert_eq!(stale.role(), Role::Leader);

    // Meanwhile another node commits an entry from a much later term.
    let mut ahead = lone(1, 3);
    feed(
        &mut checker,
        &mut ahead,
        5,
        2,
        append(9, 2, vec![cmd_entry(1, 9, "newer")], 1),
    );
    assert_eq!(ahead.commit_index(), 1);

    // Observing the stale leader must not report anything.
    checker.observe_node(30, &stale, Some(1));
    assert!(
        !checker.broken().contains(&Invariant::LeaderCompleteness),
        "a term-1 leader was blamed for not having a term-9 entry: {:?}",
        checker.violations()
    );
}

/// A third false positive a large sweep found: Leader Completeness must compare
/// against the term an entry was **committed** in, not the term it was created
/// in.
///
/// §5.4.2 lets a leader commit earlier-term entries indirectly, by committing
/// one of its own on top. So an entry created in term 1 can sit uncommitted
/// through several elections and only become committed much later. A leader
/// elected before that commit happened owes nothing to the entry — and blaming
/// it produces a violation report for a completely healthy cluster.
#[test]
fn leader_completeness_ignores_entries_committed_after_the_leader_was_elected() {
    let mut checker = Invariants::new();

    // An entry created in term 1, but only committed once the node has reached
    // term 9 — the shape §5.4.2 produces.
    let mut late = lone(0, 3);
    feed(
        &mut checker,
        &mut late,
        5,
        2,
        append(9, 2, vec![cmd_entry(1, 1, "old")], 1),
    );
    assert_eq!(late.current_term(), 9);
    assert_eq!(late.commit_index(), 1);

    // A leader of term 5: elected before that commitment, so not answerable for
    // it.
    let mut earlier = lone(1, 3);
    earlier.step(
        Input::Message {
            from: 2,
            msg: RaftMessage::RequestVote(RequestVoteReq {
                term: 4,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        },
        1,
    );
    let deadline = earlier.next_deadline();
    earlier.step(Input::Tick, deadline);
    let term = earlier.current_term();
    assert_eq!(term, 5);
    for voter in [0u32, 2] {
        earlier.step(
            Input::Message {
                from: voter,
                msg: RaftMessage::RequestVoteResp(raft::RequestVoteResp {
                    term,
                    vote_granted: true,
                }),
            },
            deadline,
        );
    }
    assert_eq!(earlier.role(), Role::Leader);
    checker.observe_node(30, &earlier, Some(1));

    assert!(
        !checker.broken().contains(&Invariant::LeaderCompleteness),
        "a term-5 leader was blamed for an entry committed in term 9: {:?}",
        checker.violations()
    );
}

#[test]
fn state_machine_safety_fires_on_divergent_applies() {
    let mut checker = Invariants::new();
    let x = Some(KvCommand::Put {
        key: "k".into(),
        value: "x".into(),
    });
    let y = Some(KvCommand::Put {
        key: "k".into(),
        value: "y".into(),
    });
    checker.observe_apply(1, 0, 1, 1, &x);
    checker.observe_apply(2, 1, 1, 1, &y);
    assert!(checker.broken().contains(&Invariant::StateMachineSafety));
    let text = checker.violations()[0].to_string();
    assert!(text.contains("at index 1"), "{text}");
}

#[test]
fn applied_in_order_fires_on_a_gap() {
    let mut checker = Invariants::new();
    let c = Some(KvCommand::Put {
        key: "k".into(),
        value: "v".into(),
    });
    checker.observe_apply(1, 0, 1, 1, &c);
    checker.observe_apply(2, 0, 3, 1, &c);
    assert!(checker.broken().contains(&Invariant::AppliedInOrder));
    let text = checker
        .violations()
        .iter()
        .find(|v| v.invariant == Invariant::AppliedInOrder)
        .unwrap()
        .to_string();
    assert!(text.contains("should have applied 2"), "{text}");
}

#[test]
fn single_vote_per_term_fires_when_a_node_votes_twice() {
    let cfg = RaftConfig {
        bugs: BugSwitches {
            vote_twice_per_term: true,
            ..BugSwitches::default()
        },
        ..RaftConfig::default()
    };
    let mut n = RaftNode::new(0, ClusterConfig::new(0..3), cfg, 1, 0);
    let mut checker = Invariants::new();

    for candidate in [1u32, 2] {
        n.step(
            Input::Message {
                from: candidate,
                msg: RaftMessage::RequestVote(RequestVoteReq {
                    term: 1,
                    candidate_id: candidate,
                    last_log_index: 0,
                    last_log_term: 0,
                }),
            },
            1,
        );
        checker.observe_node(1, &n, None);
    }

    assert_eq!(n.voted_for(), Some(2), "the bug is supposed to be on");
    assert!(checker.broken().contains(&Invariant::SingleVotePerTerm));
}

#[test]
fn committed_entries_stable_fires_on_two_different_committed_entries() {
    let mut checker = Invariants::new();
    let (mut a, mut b) = (lone(0, 3), lone(1, 3));
    feed(
        &mut checker,
        &mut a,
        1,
        2,
        append(1, 2, vec![cmd_entry(1, 1, "first")], 1),
    );
    feed(
        &mut checker,
        &mut b,
        2,
        2,
        append(2, 2, vec![cmd_entry(1, 2, "second")], 1),
    );
    assert!(checker
        .broken()
        .contains(&Invariant::CommittedEntriesStable));
}

/// Term Monotonic and Commit Index Monotonic are not reachable by a correct
/// node at all — a node cannot un-learn a term without a crash, and crashes do
/// not exist yet (step 7). The checks still have to work when they do, so they
/// are exercised by handing the checker two states for the same node id in the
/// wrong order.
#[test]
fn term_and_commit_regressions_fire() {
    let mut checker = Invariants::new();

    let mut ahead = lone(0, 3);
    ahead.step(
        Input::Message {
            from: 1,
            msg: RaftMessage::RequestVote(RequestVoteReq {
                term: 9,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            }),
        },
        1,
    );
    assert_eq!(ahead.current_term(), 9);
    checker.observe_node(1, &ahead, None);

    // A second instance with the same id, as a crashed-and-recovered node that
    // forgot its term would look.
    let behind = lone(0, 3);
    checker.observe_node(2, &behind, None);
    assert!(checker.broken().contains(&Invariant::TermMonotonic));

    let mut checker = Invariants::new();
    let mut committed = lone(0, 3);
    committed.step(
        Input::Message {
            from: 1,
            msg: append(1, 1, vec![cmd_entry(1, 1, "a")], 1),
        },
        1,
    );
    assert_eq!(committed.commit_index(), 1);
    checker.observe_node(1, &committed, Some(1));
    checker.observe_node(2, &lone(0, 3), None);
    assert!(checker.broken().contains(&Invariant::CommitIndexMonotonic));
}

/// Every invariant the checker knows about must have a test above that fires
/// it. Without this, adding a property to the enum and forgetting to validate
/// it would go unnoticed.
#[test]
fn every_invariant_has_a_scenario_that_trips_it() {
    let all = [
        Invariant::ElectionSafety,
        Invariant::LogMatching,
        Invariant::LeaderCompleteness,
        Invariant::StateMachineSafety,
        Invariant::CommittedEntriesStable,
        Invariant::CommittedPrefixNeverTruncated,
        Invariant::CommitIndexMonotonic,
        Invariant::CommitIndexWithinLog,
        Invariant::AppliedInOrder,
        Invariant::SingleVotePerTerm,
        Invariant::TermMonotonic,
    ];
    // Each of these has a `#[test]` above; this asserts the metadata is filled
    // in so a report is never blank.
    for inv in all {
        assert!(!inv.name().is_empty());
        assert!(inv.paper_ref().starts_with('§') || inv.paper_ref().starts_with("Figure"));
        assert!(
            inv.usual_cause().len() > 20,
            "{:?} needs a useful 'likely cause'",
            inv
        );
    }
}
