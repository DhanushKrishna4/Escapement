//! Is this history linearizable?
//!
//! Given a set of concurrent client operations, decide whether there exists a
//! sequential order — consistent with real-time precedence — that a correct
//! single-threaded key/value store could have produced.
//!
//! # The approach
//!
//! Wing & Gong's search with Lowe's optimizations:
//!
//! * **P-compositionality.** Operations on different keys cannot affect each
//!   other, so the history splits into independent per-key sub-histories and
//!   each is checked alone. The search is exponential in what it is handed, so
//!   this is not a nice-to-have — it is the difference between finishing and
//!   not.
//! * **Linearize the earliest candidate, recurse, backtrack.** An operation may
//!   go next only if nothing still un-linearized had to finish before it
//!   started.
//! * **Memoize on (state, remaining set).** Two different orders that reach the
//!   same state with the same work left are the same subproblem; without this
//!   the search revisits the same branches exponentially.
//!
//! # Pending operations
//!
//! The subtle part. An operation with no response may or may not have taken
//! effect:
//!
//! * A pending **read** observed nothing, so it constrains nothing. Dropped.
//! * A pending **write** is *optional*: the search may linearize it (a later
//!   read might need it to explain what it saw) or leave it out (it never
//!   happened). Its response time is unbounded, so nothing forces it to come
//!   before anything.
//! * A **rejected** operation — the node said "not leader" — definitely never
//!   entered any log. Dropped.
//!
//! Treating a pending write as failed invents violations; treating it as
//! definitely-succeeded hides them. It has to be genuinely optional.
//!
//! # Honesty
//!
//! The problem is NP-hard. When the search budget runs out, or a sub-history is
//! longer than the cap, the answer is [`Verdict::Unknown`] with a reason —
//! never a cheerful "linearizable".

use std::collections::BTreeSet;
use std::fmt;

use kvstore::{KvCommand, KvResult};
use raft::Tick;

use crate::history::{show, History, OpId, Operation, Outcome};

/// Most operations one key's sub-history may have before the search gives up.
/// The remaining-set is a bitmask, so this is also the width of that mask.
pub const MAX_OPS_PER_KEY: usize = 128;

/// Default cap on search steps, across all keys.
pub const DEFAULT_BUDGET: u64 = 2_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Linearizable {
        keys: usize,
        operations: usize,
    },
    NotLinearizable(Box<Explanation>),
    /// The search could not decide. Says why, and never pretends otherwise.
    Unknown {
        reason: String,
    },
}

impl Verdict {
    pub fn is_linearizable(&self) -> bool {
        matches!(self, Verdict::Linearizable { .. })
    }
    pub fn is_violation(&self) -> bool {
        matches!(self, Verdict::NotLinearizable(_))
    }
    pub fn is_unknown(&self) -> bool {
        matches!(self, Verdict::Unknown { .. })
    }
}

/// Why no valid ordering exists.
///
/// "FAIL" is close to useless when a fuzzer hands you one of these at 3am, so
/// this carries the sub-history, the longest prefix that *did* work, the state
/// it reached, and what each remaining candidate would have had to return.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Explanation {
    pub key: String,
    pub operations: Vec<String>,
    pub linearized: Vec<String>,
    pub state_after: Option<String>,
    pub blocked: Vec<Blocked>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blocked {
    pub operation: String,
    pub expected: String,
    pub observed: String,
}

impl fmt::Display for Explanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "no sequential ordering of the operations on key \"{}\" can explain what clients saw.",
            self.key
        )?;
        writeln!(f, "\n  the operations, by invocation time:")?;
        for op in &self.operations {
            writeln!(f, "    {op}")?;
        }
        writeln!(
            f,
            "\n  the longest ordering that works ({} of {} operations):",
            self.linearized.len(),
            self.operations.len()
        )?;
        if self.linearized.is_empty() {
            writeln!(
                f,
                "    (none — the very first operation is already impossible)"
            )?;
        }
        for op in &self.linearized {
            writeln!(f, "    {op}")?;
        }
        writeln!(
            f,
            "  leaving the value as {}",
            match &self.state_after {
                Some(v) => v.as_str(),
                None => "nil",
            }
        )?;
        writeln!(
            f,
            "\n  from there every operation that could legally come next disagrees:"
        )?;
        for b in &self.blocked {
            writeln!(
                f,
                "    {}\n      would return {}, but the client observed {}",
                b.operation, b.expected, b.observed
            )?;
        }
        Ok(())
    }
}

/// The value of a single key. The whole model state, because the history has
/// already been split per key.
type State = Option<String>;

/// Apply one command to a single key's value.
///
/// Deliberately independent of `KvStore` so the search can keep its state in an
/// `Option<String>` rather than a map — the state is cloned and compared on
/// every memo lookup, and that cost dominates. `agrees_with_the_real_store`
/// below pins the two implementations together so they cannot drift.
fn apply(state: &mut State, command: &KvCommand) -> KvResult {
    match command {
        KvCommand::Get { .. } => KvResult::Value(state.clone()),
        KvCommand::Put { value, .. } => {
            *state = Some(value.clone());
            KvResult::Ok
        }
        KvCommand::Delete { .. } => {
            *state = None;
            KvResult::Ok
        }
        KvCommand::Cas { expect, value, .. } => {
            if state == expect {
                *state = Some(value.clone());
                KvResult::Ok
            } else {
                KvResult::CasFailed {
                    actual: state.clone(),
                }
            }
        }
    }
}

/// One operation as the search sees it.
#[derive(Clone, Debug)]
struct Entry {
    id: OpId,
    invoke: Tick,
    /// `None` means it never responded, so nothing has to happen after it.
    response: Option<Tick>,
    command: KvCommand,
    /// `None` for a pending write: there is no observed result to match.
    observed: Option<KvResult>,
    /// A pending write may simply never have taken effect.
    optional: bool,
    describe: String,
}

/// Could this command ever have produced this result, in any state?
///
/// A cheap shape check, run before the search. When it fails, the store did not
/// merely order things badly — it handed back an answer belonging to a
/// different command, and saying so directly is far more useful than a
/// twenty-operation "no ordering exists" report. This is exactly how a session
/// table returning a stale slot's response shows up: a `put` that answers
/// `cas-failed`.
fn result_is_possible(command: &KvCommand, result: &KvResult) -> bool {
    matches!(
        (command, result),
        (KvCommand::Get { .. }, KvResult::Value(_))
            | (
                KvCommand::Put { .. } | KvCommand::Delete { .. },
                KvResult::Ok
            )
            | (
                KvCommand::Cas { .. },
                KvResult::Ok | KvResult::CasFailed { .. }
            )
    )
}

/// Check a whole history.
pub fn check(history: &History) -> Verdict {
    check_with_budget(history, DEFAULT_BUDGET)
}

pub fn check_with_budget(history: &History, budget: u64) -> Verdict {
    let by_key = history.by_key();
    let mut keys_checked = 0usize;
    let mut ops_checked = 0usize;
    let mut unknown: Option<String> = None;
    let mut remaining_budget = budget;

    for (key, ops) in by_key {
        // Shape check first: an answer that no state could have produced is a
        // violation on its own, and pointing at it beats a search report.
        for op in &ops {
            if let Outcome::Completed { result, .. } = &op.outcome {
                if !result_is_possible(&op.command, result) {
                    return Verdict::NotLinearizable(Box::new(Explanation {
                        key: key.to_string(),
                        operations: ops.iter().map(|o| o.describe()).collect(),
                        linearized: Vec::new(),
                        state_after: None,
                        blocked: vec![Blocked {
                            operation: op.describe(),
                            expected: "a result this command can produce".to_string(),
                            observed: show(result),
                        }],
                    }));
                }
            }
        }

        let entries = prepare(&ops);
        if entries.is_empty() {
            continue;
        }

        // Over the cap, check a prefix instead. Linearizability is
        // prefix-closed, so a violation in a prefix is a real violation of the
        // whole; but a clean prefix says nothing about the rest, which is why
        // the verdict below is Unknown rather than Linearizable.
        let truncated = entries.len() > MAX_OPS_PER_KEY;
        let window = if truncated {
            &entries[..MAX_OPS_PER_KEY]
        } else {
            &entries[..]
        };

        let mut search = Search::new(window, remaining_budget);
        match search.run() {
            Outcome2::Ok => {
                remaining_budget = remaining_budget.saturating_sub(search.steps);
                keys_checked += 1;
                ops_checked += window.len();
                if truncated && unknown.is_none() {
                    unknown = Some(format!(
                        "key \"{key}\" has {} operations, over the cap of {MAX_OPS_PER_KEY}; \
                         only the first {MAX_OPS_PER_KEY} were checked and they are consistent",
                        entries.len()
                    ));
                }
            }
            Outcome2::Violation => {
                return Verdict::NotLinearizable(Box::new(search.explain(key)));
            }
            Outcome2::OutOfBudget => {
                return Verdict::Unknown {
                    reason: format!(
                        "search budget exhausted on key \"{key}\" after {} steps \
                         ({} operations); could not determine",
                        search.steps,
                        window.len()
                    ),
                };
            }
        }
    }

    match unknown {
        Some(reason) => Verdict::Unknown { reason },
        None => Verdict::Linearizable {
            keys: keys_checked,
            operations: ops_checked,
        },
    }
}

/// Turn a key's operations into search entries, dropping the ones that
/// constrain nothing.
fn prepare(ops: &[&Operation]) -> Vec<Entry> {
    let mut entries: Vec<Entry> = ops
        .iter()
        .filter_map(|op| match &op.outcome {
            // Never entered any log.
            Outcome::Rejected { .. } => None,
            // An unobserved read constrains nothing at all.
            Outcome::Pending if op.is_read_only() => None,
            Outcome::Pending => Some(Entry {
                id: op.id,
                invoke: op.invoked,
                response: None,
                command: op.command.clone(),
                observed: None,
                optional: true,
                describe: op.describe(),
            }),
            Outcome::Completed { at, result } => Some(Entry {
                id: op.id,
                invoke: op.invoked,
                response: Some(*at),
                command: op.command.clone(),
                observed: Some(result.clone()),
                optional: false,
                describe: op.describe(),
            }),
        })
        .collect();
    // Stable order by invocation, then id, so the search and its reports are
    // deterministic regardless of how the history was assembled.
    entries.sort_by_key(|e| (e.invoke, e.id));
    entries
}

enum Outcome2 {
    Ok,
    Violation,
    OutOfBudget,
}

struct Search<'a> {
    entries: &'a [Entry],
    /// States already known to be dead ends, keyed by (value, remaining set).
    ///
    /// `BTreeSet`, not `HashSet`: this file is in the deterministic core and a
    /// randomly-seeded hasher has no business anywhere near it.
    seen: BTreeSet<(State, u128)>,
    steps: u64,
    budget: u64,
    /// The deepest the search ever got, kept for the explanation.
    best_depth: usize,
    best_order: Vec<usize>,
    best_state: State,
    order: Vec<usize>,
}

impl<'a> Search<'a> {
    fn new(entries: &'a [Entry], budget: u64) -> Self {
        Search {
            entries,
            seen: BTreeSet::new(),
            steps: 0,
            budget,
            best_depth: 0,
            best_order: Vec::new(),
            best_state: None,
            order: Vec::new(),
        }
    }

    fn run(&mut self) -> Outcome2 {
        let all: u128 = if self.entries.len() == 128 {
            u128::MAX
        } else {
            (1u128 << self.entries.len()) - 1
        };
        match self.step(None, all) {
            Some(true) => Outcome2::Ok,
            Some(false) => Outcome2::Violation,
            None => Outcome2::OutOfBudget,
        }
    }

    /// `None` means the budget ran out.
    fn step(&mut self, state: State, remaining: u128) -> Option<bool> {
        self.steps += 1;
        if self.steps > self.budget {
            return None;
        }

        // Done when every *required* operation has been placed. Optional
        // entries still outstanding simply never took effect.
        let required_left = self
            .entries
            .iter()
            .enumerate()
            .any(|(i, e)| remaining & (1 << i) != 0 && !e.optional);
        if !required_left {
            return Some(true);
        }

        if !self.seen.insert((state.clone(), remaining)) {
            return Some(false);
        }

        if self.order.len() > self.best_depth {
            self.best_depth = self.order.len();
            self.best_order = self.order.clone();
            self.best_state = state.clone();
        }

        // An operation may go next only if nothing still outstanding had to
        // finish before it started. A pending operation never responded, so it
        // never has to precede anything.
        let earliest_response = self
            .entries
            .iter()
            .enumerate()
            .filter(|(i, _)| remaining & (1 << i) != 0)
            .filter_map(|(_, e)| e.response)
            .min();

        for (i, entry) in self.entries.iter().enumerate() {
            if remaining & (1 << i) == 0 {
                continue;
            }
            if let Some(limit) = earliest_response {
                if entry.invoke > limit {
                    continue;
                }
            }

            let mut next = state.clone();
            let produced = apply(&mut next, &entry.command);
            if let Some(observed) = &entry.observed {
                if &produced != observed {
                    continue;
                }
            }

            self.order.push(i);
            match self.step(next, remaining & !(1 << i)) {
                Some(true) => {
                    self.order.pop();
                    return Some(true);
                }
                Some(false) => {
                    self.order.pop();
                }
                None => {
                    self.order.pop();
                    return None;
                }
            }
        }

        Some(false)
    }

    fn explain(&self, key: &str) -> Explanation {
        // Replay the best prefix to recover the state it reached, then report
        // what every legal next operation would have had to return.
        let mut state: State = None;
        let mut remaining: u128 = if self.entries.len() == 128 {
            u128::MAX
        } else {
            (1u128 << self.entries.len()) - 1
        };
        let mut linearized = Vec::new();
        for i in &self.best_order {
            apply(&mut state, &self.entries[*i].command);
            remaining &= !(1u128 << *i);
            linearized.push(self.entries[*i].describe.clone());
        }

        let earliest_response = self
            .entries
            .iter()
            .enumerate()
            .filter(|(i, _)| remaining & (1 << i) != 0)
            .filter_map(|(_, e)| e.response)
            .min();

        let mut blocked = Vec::new();
        for (i, entry) in self.entries.iter().enumerate() {
            if remaining & (1 << i) == 0 {
                continue;
            }
            if let Some(limit) = earliest_response {
                if entry.invoke > limit {
                    continue;
                }
            }
            let Some(observed) = &entry.observed else {
                continue;
            };
            let mut probe = state.clone();
            let produced = apply(&mut probe, &entry.command);
            blocked.push(Blocked {
                operation: entry.describe.clone(),
                expected: show(&produced),
                observed: show(observed),
            });
        }

        Explanation {
            key: key.to_string(),
            operations: self.entries.iter().map(|e| e.describe.clone()).collect(),
            linearized,
            state_after: state,
            blocked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvstore::KvStore;

    /// The single-key model must agree with the real store, or the checker is
    /// grading against the wrong answer key.
    #[test]
    fn agrees_with_the_real_store() {
        let mut rng = raft::Rng::new(4);
        let mut model: State = None;
        let mut real = KvStore::new();
        for i in 0..5_000u64 {
            let cmd = match rng.gen_range(0, 4) {
                0 => KvCommand::Get { key: "k".into() },
                1 => KvCommand::Put {
                    key: "k".into(),
                    value: format!("v{i}"),
                },
                2 => KvCommand::Delete { key: "k".into() },
                _ => KvCommand::Cas {
                    key: "k".into(),
                    expect: if rng.chance(1, 2) {
                        None
                    } else {
                        Some(format!("v{}", rng.gen_range(0, i.max(1))))
                    },
                    value: format!("w{i}"),
                },
            };
            assert_eq!(
                apply(&mut model, &cmd),
                real.apply(&cmd),
                "diverged at step {i}"
            );
            assert_eq!(model.as_ref(), real.get("k"), "state diverged at step {i}");
        }
    }
}
