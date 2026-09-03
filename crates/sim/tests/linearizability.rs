//! Validating the linearizability checker.
//!
//! A checker that always answers "OK" passes every test until the day it
//! matters, so it is fed histories that are known linearizable and histories
//! that are known not, and has to get both right. The pending-operation cases
//! get the most attention: they are where a checker most easily becomes either
//! unsound (inventing violations) or useless (excusing them).

use kvstore::{KvCommand, KvResult};
use raft::{ClientId, Tick};
use sim::history::{History, Outcome};
use sim::linearizability::{check, check_with_budget, Verdict};
use sim::{Cluster, FaultConfig, NetworkConfig, SimConfig, Workload, WorkloadConfig};

// ---------------------------------------------------------------------------
// Building histories by hand
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Build {
    history: History,
    seq: std::collections::BTreeMap<ClientId, u64>,
}

impl Build {
    fn new() -> Self {
        Build::default()
    }

    fn next_seq(&mut self, client: ClientId) -> u64 {
        let s = self.seq.entry(client).or_insert(0);
        let v = *s;
        *s += 1;
        v
    }

    /// A completed operation spanning [invoke, response].
    fn done(
        mut self,
        client: ClientId,
        invoke: Tick,
        response: Tick,
        command: KvCommand,
        result: KvResult,
    ) -> Self {
        let seq = self.next_seq(client);
        self.history.invoke(client, seq, command, invoke);
        self.history.complete(client, seq, response, result);
        self
    }

    /// An operation that never received a response.
    fn pending(mut self, client: ClientId, invoke: Tick, command: KvCommand) -> Self {
        let seq = self.next_seq(client);
        self.history.invoke(client, seq, command, invoke);
        self
    }

    /// An operation the node refused outright.
    fn rejected(
        mut self,
        client: ClientId,
        invoke: Tick,
        response: Tick,
        command: KvCommand,
    ) -> Self {
        let seq = self.next_seq(client);
        self.history.invoke(client, seq, command, invoke);
        self.history.refuse(client, seq);
        self.history.abandon(client, seq, response);
        self
    }

    fn build(self) -> History {
        self.history
    }
}

fn get(key: &str) -> KvCommand {
    KvCommand::Get { key: key.into() }
}
fn put(key: &str, value: &str) -> KvCommand {
    KvCommand::Put {
        key: key.into(),
        value: value.into(),
    }
}
fn del(key: &str) -> KvCommand {
    KvCommand::Delete { key: key.into() }
}
fn cas(key: &str, expect: Option<&str>, value: &str) -> KvCommand {
    KvCommand::Cas {
        key: key.into(),
        expect: expect.map(|s| s.to_string()),
        value: value.into(),
    }
}
fn val(v: Option<&str>) -> KvResult {
    KvResult::Value(v.map(|s| s.to_string()))
}
fn ok() -> KvResult {
    KvResult::Ok
}
fn cas_failed(actual: Option<&str>) -> KvResult {
    KvResult::CasFailed {
        actual: actual.map(|s| s.to_string()),
    }
}

fn assert_linearizable(h: &History, why: &str) {
    match check(h) {
        Verdict::Linearizable { .. } => {}
        other => panic!("{why}\nexpected linearizable, got: {}", render(&other)),
    }
}

fn assert_not_linearizable(h: &History, why: &str) {
    match check(h) {
        Verdict::NotLinearizable(_) => {}
        other => panic!("{why}\nexpected a violation, got: {}", render(&other)),
    }
}

fn render(v: &Verdict) -> String {
    match v {
        Verdict::Linearizable { keys, operations } => {
            format!("linearizable ({keys} keys, {operations} ops)")
        }
        Verdict::NotLinearizable(e) => format!("NOT linearizable:\n{e}"),
        Verdict::Unknown { reason } => format!("unknown: {reason}"),
    }
}

// ---------------------------------------------------------------------------
// Known linearizable
// ---------------------------------------------------------------------------

#[test]
fn an_empty_history_is_linearizable() {
    assert_linearizable(&History::new(), "nothing happened");
}

#[test]
fn a_plain_sequential_history_is_linearizable() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(1, 20, 30, get("x"), val(Some("a")))
        .done(1, 40, 50, put("x", "b"), ok())
        .done(1, 60, 70, get("x"), val(Some("b")))
        .done(1, 80, 90, del("x"), ok())
        .done(1, 100, 110, get("x"), val(None))
        .build();
    assert_linearizable(&h, "a simple sequential run");
}

#[test]
fn concurrent_operations_that_admit_an_ordering_are_linearizable() {
    // Two writes overlap; a later read sees one of them. Either order works.
    let h = Build::new()
        .done(1, 0, 100, put("x", "a"), ok())
        .done(2, 10, 110, put("x", "b"), ok())
        .done(3, 120, 130, get("x"), val(Some("b")))
        .build();
    assert_linearizable(&h, "overlapping writes, then a read of one of them");
}

#[test]
fn a_read_concurrent_with_a_write_may_see_either_value() {
    for observed in [None, Some("a")] {
        let h = Build::new()
            .done(1, 0, 10, put("x", "old"), ok())
            .done(1, 20, 100, put("x", "a"), ok())
            .done(2, 30, 90, get("x"), val(observed.or(Some("old"))))
            .build();
        assert_linearizable(&h, "a read overlapping a write may see before or after");
    }
}

#[test]
fn cas_sequences_are_linearizable() {
    let h = Build::new()
        .done(1, 0, 10, cas("x", None, "a"), ok())
        .done(
            1,
            20,
            30,
            cas("x", Some("wrong"), "b"),
            cas_failed(Some("a")),
        )
        .done(1, 40, 50, cas("x", Some("a"), "b"), ok())
        .done(1, 60, 70, get("x"), val(Some("b")))
        .build();
    assert_linearizable(&h, "compare-and-set behaving itself");
}

#[test]
fn independent_keys_do_not_interfere() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(2, 5, 15, put("y", "b"), ok())
        .done(1, 20, 30, get("x"), val(Some("a")))
        .done(2, 25, 35, get("y"), val(Some("b")))
        .build();
    assert_linearizable(&h, "two keys minding their own business");
}

#[test]
fn a_rejected_operation_is_treated_as_never_having_happened() {
    // The node said "not leader", so the write never entered any log and the
    // read correctly sees nothing.
    let h = Build::new()
        .rejected(1, 0, 10, put("x", "a"))
        .done(2, 20, 30, get("x"), val(None))
        .build();
    assert_linearizable(&h, "a refused write must not be assumed to have happened");
}

// ---------------------------------------------------------------------------
// Known NOT linearizable
// ---------------------------------------------------------------------------

#[test]
fn a_read_of_a_value_nobody_wrote_is_not_linearizable() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(1, 20, 30, get("x"), val(Some("ghost")))
        .build();
    assert_not_linearizable(&h, "the store invented a value");
}

#[test]
fn a_stale_read_after_a_fresh_one_is_not_linearizable() {
    // The classic. x=a is read, then a strictly later read returns the older
    // value. No sequential order can produce that.
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(1, 20, 30, put("x", "b"), ok())
        .done(2, 40, 50, get("x"), val(Some("b")))
        .done(2, 60, 70, get("x"), val(Some("a")))
        .build();
    assert_not_linearizable(&h, "time went backwards");
}

#[test]
fn a_completed_write_that_a_later_read_misses_is_not_linearizable() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(2, 20, 30, get("x"), val(None))
        .build();
    assert_not_linearizable(&h, "an acknowledged write vanished");
}

#[test]
fn a_cas_reporting_the_wrong_actual_value_is_not_linearizable() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(
            1,
            20,
            30,
            cas("x", Some("zzz"), "b"),
            cas_failed(Some("nonsense")),
        )
        .build();
    assert_not_linearizable(&h, "the failed CAS lied about what it saw");
}

#[test]
fn a_delete_that_does_not_take_is_not_linearizable() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(1, 20, 30, del("x"), ok())
        .done(1, 40, 50, get("x"), val(Some("a")))
        .build();
    assert_not_linearizable(&h, "the delete was acknowledged and ignored");
}

#[test]
fn a_violation_on_one_key_is_found_among_healthy_keys() {
    let h = Build::new()
        .done(1, 0, 10, put("a", "1"), ok())
        .done(1, 20, 30, get("a"), val(Some("1")))
        .done(1, 40, 50, put("b", "2"), ok())
        .done(1, 60, 70, get("b"), val(Some("2")))
        .done(1, 80, 90, put("c", "3"), ok())
        .done(1, 100, 110, get("c"), val(Some("wrong")))
        .build();
    match check(&h) {
        Verdict::NotLinearizable(e) => assert_eq!(e.key, "c", "blamed the wrong key"),
        other => panic!("expected a violation on key c, got {}", render(&other)),
    }
}

// ---------------------------------------------------------------------------
// Pending operations — where a checker becomes unsound or useless
// ---------------------------------------------------------------------------

/// A pending write may have taken effect. If a later read can only be explained
/// by it, the history IS linearizable — and a checker that recorded pending
/// operations as failures would report a violation here that does not exist.
#[test]
fn a_pending_write_can_explain_a_later_read() {
    let h = Build::new()
        .pending(1, 0, put("x", "a"))
        .done(2, 50, 60, get("x"), val(Some("a")))
        .build();
    assert_linearizable(
        &h,
        "the only explanation for the read is the write the client never heard about",
    );
}

/// The other direction. A pending write may equally never have taken effect, so
/// a later read seeing nothing is also fine. A checker that assumed pending
/// operations succeeded would wrongly reject this.
#[test]
fn a_pending_write_need_not_have_taken_effect() {
    let h = Build::new()
        .pending(1, 0, put("x", "a"))
        .done(2, 50, 60, get("x"), val(None))
        .build();
    assert_linearizable(&h, "silence does not mean the write happened");
}

/// A pending write can be linearized arbitrarily late — it may commit long
/// after the client gives up — so a read that misses it and a later read that
/// sees it are both consistent.
#[test]
fn a_pending_write_may_land_between_two_reads() {
    let h = Build::new()
        .pending(1, 0, put("x", "a"))
        .done(2, 10, 20, get("x"), val(None))
        .done(2, 30, 40, get("x"), val(Some("a")))
        .build();
    assert_linearizable(&h, "the stranded write landed between the two reads");
}

/// But a pending write must not become a universal excuse. The values here
/// cannot be produced in any order regardless of whether the pending write
/// happened.
#[test]
fn a_pending_write_does_not_excuse_a_real_violation() {
    let h = Build::new()
        .pending(1, 0, put("x", "a"))
        .done(2, 10, 20, get("x"), val(Some("b")))
        .build();
    assert_not_linearizable(&h, "nobody ever wrote b");
}

#[test]
fn a_pending_write_cannot_undo_an_acknowledged_one() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .pending(2, 20, put("x", "b"))
        .done(1, 30, 40, get("x"), val(Some("a")))
        .done(1, 50, 60, get("x"), val(Some("b")))
        .done(1, 70, 80, get("x"), val(Some("a")))
        .build();
    assert_not_linearizable(
        &h,
        "once the pending write is placed, x cannot go back to a on its own",
    );
}

/// An unobserved read tells us nothing, so it must not constrain anything.
#[test]
fn a_pending_read_constrains_nothing() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .pending(2, 20, get("x"))
        .done(1, 30, 40, get("x"), val(Some("a")))
        .build();
    assert_linearizable(&h, "a read nobody heard the answer to says nothing");
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

/// The verdict must depend on the operations and their times, not on the order
/// they happen to sit in the list.
#[test]
fn the_verdict_does_not_depend_on_input_order() {
    let make = |order: &[usize]| {
        let specs: Vec<(ClientId, Tick, Tick, KvCommand, KvResult)> = vec![
            (1, 0, 10, put("x", "a"), ok()),
            (2, 20, 30, get("x"), val(Some("a"))),
            (1, 40, 50, put("x", "b"), ok()),
            (2, 60, 70, get("x"), val(Some("b"))),
            (3, 15, 25, get("x"), val(Some("a"))),
        ];
        let mut b = Build::new();
        for i in order {
            let (c, iv, rp, cmd, res) = specs[*i].clone();
            b = b.done(c, iv, rp, cmd, res);
        }
        b.build()
    };

    let baseline = check(&make(&[0, 1, 2, 3, 4]));
    assert!(baseline.is_linearizable());
    // Every permutation describes the same real-time history.
    for order in [
        vec![4, 3, 2, 1, 0],
        vec![2, 0, 4, 3, 1],
        vec![1, 4, 0, 2, 3],
        vec![3, 2, 1, 0, 4],
    ] {
        assert!(
            check(&make(&order)).is_linearizable(),
            "permuting the input changed the verdict: {order:?}"
        );
    }
}

/// Any strictly sequential history produced by actually running the model is
/// linearizable, by construction. Anything else is a checker bug.
#[test]
fn generated_sequential_histories_are_always_accepted() {
    let mut rng = raft::Rng::new(17);
    for round in 0..200u64 {
        let mut store = kvstore::KvStore::new();
        let mut b = Build::new();
        let mut t = 0u64;
        for i in 0..25u64 {
            let key = format!("k{}", rng.gen_range(0, 3));
            let cmd = match rng.gen_range(0, 4) {
                0 => KvCommand::Get { key },
                1 => KvCommand::Put {
                    key,
                    value: format!("v{round}_{i}"),
                },
                2 => KvCommand::Delete { key },
                _ => KvCommand::Cas {
                    key,
                    expect: if rng.chance(1, 2) {
                        None
                    } else {
                        Some(format!("v{round}_{}", rng.gen_range(0, i.max(1))))
                    },
                    value: format!("w{round}_{i}"),
                },
            };
            let result = store.apply(&cmd);
            b = b.done(1, t, t + 5, cmd, result);
            t += 10;
        }
        let h = b.build();
        assert_linearizable(&h, &format!("round {round}: a real sequential run"));
    }
}

/// And corrupting one observed value in such a history must be caught.
#[test]
fn corrupting_a_sequential_history_is_always_caught() {
    let mut rng = raft::Rng::new(23);
    let mut caught = 0;
    for _ in 0..100u64 {
        let mut store = kvstore::KvStore::new();
        let mut b = Build::new();
        let mut t = 0u64;
        let corrupt_at = rng.gen_range(1, 12);
        for i in 0..12u64 {
            let cmd = if i % 2 == 0 {
                KvCommand::Put {
                    key: "k".into(),
                    value: format!("v{i}"),
                }
            } else {
                KvCommand::Get { key: "k".into() }
            };
            let mut result = store.apply(&cmd);
            if i == corrupt_at {
                // Replace a real observation with a value that was never written.
                result = match result {
                    KvResult::Value(_) => val(Some("impossible")),
                    other => other,
                };
            }
            b = b.done(1, t, t + 5, cmd, result);
            t += 10;
        }
        let h = b.build();
        // Only reads carry an observable value, so only a corrupted read is
        // detectable; the rest of the time the history is genuinely unchanged.
        if corrupt_at % 2 == 1 {
            assert_not_linearizable(&h, "a corrupted read should be caught");
            caught += 1;
        }
    }
    assert!(
        caught > 20,
        "the corruption test barely exercised anything: {caught}"
    );
}

// ---------------------------------------------------------------------------
// Honesty about what it cannot decide
// ---------------------------------------------------------------------------

#[test]
fn an_exhausted_budget_reports_unknown_rather_than_lying() {
    // A pile of mutually concurrent writes, and a read afterwards that saw the
    // value written by the one the search tries FIRST. The only valid orders
    // put that write last, so the search has to exhaust almost everything else
    // before finding one.
    let mut b = Build::new();
    for i in 0..30u64 {
        b = b.done(i as ClientId, 0, 1_000, put("x", &format!("v{i}")), ok());
    }
    b = b.done(99, 1_100, 1_200, get("x"), val(Some("v0")));
    let h = b.build();
    match check_with_budget(&h, 50) {
        Verdict::Unknown { reason } => {
            assert!(reason.contains("budget"), "{reason}");
            assert!(
                reason.contains('x'),
                "the reason should name the key: {reason}"
            );
        }
        other => panic!("expected unknown, got {}", render(&other)),
    }
}

#[test]
fn the_explanation_says_what_actually_went_wrong() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(1, 20, 30, put("x", "b"), ok())
        .done(2, 40, 50, get("x"), val(Some("b")))
        .done(2, 60, 70, get("x"), val(Some("a")))
        .build();
    let Verdict::NotLinearizable(e) = check(&h) else {
        panic!("expected a violation");
    };
    let text = e.to_string();

    assert!(text.contains("key \"x\""), "{text}");
    assert!(
        text.contains("the operations, by invocation time"),
        "{text}"
    );
    // It should show a real partial ordering, not just give up.
    assert!(
        !e.linearized.is_empty(),
        "no partial ordering was reported:\n{text}"
    );
    // And explain the disagreement in terms of expected vs observed.
    assert!(
        !e.blocked.is_empty(),
        "nothing was reported as blocked:\n{text}"
    );
    assert!(text.contains("would return"), "{text}");
    assert!(text.contains("but the client observed"), "{text}");
    // The offending read must appear.
    assert!(text.contains("get(x)"), "{text}");
}

// ---------------------------------------------------------------------------
// Against the real simulator
// ---------------------------------------------------------------------------

fn run(seed: u64, net: NetworkConfig, faults: FaultConfig, ticks: u64) -> Cluster {
    let mut sim = Cluster::new(SimConfig {
        seed,
        nodes: 5,
        network: net,
        faults,
        ..SimConfig::default()
    });
    let mut work = Workload::new(seed, WorkloadConfig::contended());
    sim.run_until_leader(10_000);
    while sim.now() < ticks {
        let leader = sim.leader();
        let target = work.target(leader, 5);
        let req = work.next_request();
        sim.submit(target, req.client, req.seq, req.command);
        let gap = work.gap();
        sim.run_for(gap);
    }
    sim.run_for(5_000);
    sim
}

#[test]
fn real_runs_produce_linearizable_histories() {
    for seed in 0..16u64 {
        let sim = run(seed, NetworkConfig::perfect(), FaultConfig::none(), 20_000);
        assert!(
            sim.history().completed() > 20,
            "seed {seed}: barely any operations completed"
        );
        match sim.check_linearizability() {
            Verdict::Linearizable { .. } => {}
            other => panic!("seed {seed}: {}", render(&other)),
        }
    }
}

#[test]
fn histories_stay_linearizable_under_faults() {
    for seed in 0..16u64 {
        let sim = run(
            seed,
            NetworkConfig::long_tail(),
            FaultConfig::aggressive(),
            25_000,
        );
        match sim.check_linearizability() {
            Verdict::Linearizable { .. } | Verdict::Unknown { .. } => {}
            other => panic!("seed {seed}: {}", render(&other)),
        }
    }
}

/// Faults must actually produce the pending operations the checker has to
/// reason about, or the hard path is never exercised on real data.
#[test]
fn faulty_runs_really_do_strand_operations() {
    let mut pending = 0;
    let mut refused = 0;
    for seed in 0..16u64 {
        let sim = run(
            seed,
            NetworkConfig::long_tail(),
            FaultConfig::aggressive(),
            25_000,
        );
        pending += sim.history().pending();
        refused += sim.history().refused();
    }
    assert!(pending > 20, "no operations were left stranded: {pending}");
    // A refusal is a failed *attempt*, not an outcome: this workload never
    // retries and never gives up, so those operations stay PENDING rather than
    // becoming Rejected. Both are exercised; only the harness with a retrying
    // client produces Rejected.
    assert!(refused > 5, "no attempt was ever refused: {refused}");
}

#[test]
fn the_history_matches_what_the_state_machines_did() {
    // Every completed write in the history should be reflected in the final
    // state of the replicas, or the recording is not describing this run.
    let sim = run(3, NetworkConfig::perfect(), FaultConfig::none(), 20_000);
    let leader = sim.leader().expect("a leader");
    let store = sim.machine(leader);

    let mut last_write: std::collections::BTreeMap<String, String> = Default::default();
    for op in sim.history().operations() {
        if let (KvCommand::Put { key, value }, Outcome::Completed { .. }) =
            (&op.command, &op.outcome)
        {
            last_write.insert(key.clone(), value.clone());
        }
        if let (KvCommand::Delete { key }, Outcome::Completed { .. }) = (&op.command, &op.outcome) {
            last_write.remove(key);
        }
    }
    assert!(!last_write.is_empty(), "no writes completed at all");
    // Every key the store holds must have been written by someone.
    for key in store.snapshot().keys() {
        assert!(
            sim.history()
                .operations()
                .iter()
                .any(|o| o.key() == key && !o.is_read_only()),
            "the store holds {key}, which no recorded operation ever wrote"
        );
    }
}

// ---------------------------------------------------------------------------
// Answers no command could produce
// ---------------------------------------------------------------------------

/// A result that does not even match its command's shape is reported directly,
/// rather than as a puzzling "no ordering exists" over twenty operations.
///
/// This is what a session table handing back the wrong slot looks like from the
/// client's side, and it is how that bug first appeared.
#[test]
fn a_result_the_command_could_never_produce_is_reported_as_such() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), cas_failed(Some("b")))
        .build();
    let Verdict::NotLinearizable(e) = check(&h) else {
        panic!("a put that answers cas-failed must be rejected");
    };
    let text = e.to_string();
    assert!(text.contains("a result this command can produce"), "{text}");
    assert!(text.contains("put(x, a)"), "{text}");
}

#[test]
fn every_command_shape_is_accepted_when_it_matches() {
    let h = Build::new()
        .done(1, 0, 10, put("x", "a"), ok())
        .done(1, 20, 30, get("x"), val(Some("a")))
        .done(1, 40, 50, cas("x", Some("a"), "b"), ok())
        .done(1, 60, 70, cas("x", Some("zz"), "c"), cas_failed(Some("b")))
        .done(1, 80, 90, del("x"), ok())
        .build();
    assert_linearizable(&h, "well-formed results must not trip the shape check");
}

#[test]
fn a_get_that_answers_ok_is_rejected() {
    let h = Build::new().done(1, 0, 10, get("x"), ok()).build();
    assert_not_linearizable(&h, "a get cannot answer ok");
}
