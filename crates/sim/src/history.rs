//! Recording what clients asked for and what they were told.
//!
//! The whole value of this file is in one distinction. An operation that never
//! received a response is **not** a failed operation. It may have committed and
//! been applied; the client simply never found out. A checker told "that one
//! failed" will report violations that are not there, and a checker told to
//! ignore it will miss violations that are. Both mistakes make the
//! linearizability check worthless, so the ambiguity is recorded explicitly and
//! carried all the way into the search.

use std::collections::BTreeMap;

use kvstore::{KvCommand, KvResult};
use raft::{ClientId, Tick};
use serde::{Deserialize, Serialize};

pub type OpId = u32;

/// What became of an operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// The client got an answer. It definitely took effect, with this result.
    Completed { at: Tick, result: KvResult },
    /// The node refused it outright — it was not the leader, so the command
    /// never entered any log. This one really did not happen, and the
    /// distinction from `Pending` matters: a refusal is information, silence is
    /// not.
    Rejected { at: Tick },
    /// No response ever arrived.
    ///
    /// The operation may or may not have taken effect, and if it did, it may
    /// have taken effect at any point after it was invoked — possibly long
    /// after the client gave up. The checker must consider both possibilities.
    Pending,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub id: OpId,
    /// The client that issued it. Called `process` in the literature.
    pub process: ClientId,
    pub seq: u64,
    pub command: KvCommand,
    pub invoked: Tick,
    pub outcome: Outcome,
    /// How many times the client sent this request.
    ///
    /// A retry is not a new operation. The logical operation spans from the
    /// FIRST attempt to whenever the client finally hears — that is the
    /// interval linearizability cares about.
    pub attempts: u32,
    /// How many attempts came back explicitly refused ("not leader").
    pub refusals: u32,
}

impl Operation {
    pub fn key(&self) -> &str {
        self.command.key()
    }

    pub fn is_read_only(&self) -> bool {
        self.command.is_read_only()
    }

    /// When the client learned the answer, or `None` while it is still in
    /// flight. A pending operation has no response time — not an infinitely
    /// late one and not the moment it was invoked.
    pub fn responded(&self) -> Option<Tick> {
        match self.outcome {
            Outcome::Completed { at, .. } | Outcome::Rejected { at } => Some(at),
            Outcome::Pending => None,
        }
    }

    pub fn result(&self) -> Option<&KvResult> {
        match &self.outcome {
            Outcome::Completed { result, .. } => Some(result),
            _ => None,
        }
    }

    /// A short description for reports.
    pub fn describe(&self) -> String {
        let op = match &self.command {
            KvCommand::Get { key } => format!("get({key})"),
            KvCommand::Put { key, value } => format!("put({key}, {value})"),
            KvCommand::Delete { key } => format!("del({key})"),
            KvCommand::Cas { key, expect, value } => {
                format!("cas({key}, {expect:?} -> {value})")
            }
        };
        match &self.outcome {
            Outcome::Completed { at, result } => {
                format!(
                    "[{}..{}] c{} {op} = {}",
                    self.invoked,
                    at,
                    self.process,
                    show(result)
                )
            }
            Outcome::Rejected { at } => {
                format!(
                    "[{}..{}] c{} {op} = rejected",
                    self.invoked, at, self.process
                )
            }
            Outcome::Pending => {
                format!("[{}..?] c{} {op} = NO RESPONSE", self.invoked, self.process)
            }
        }
    }
}

pub fn show(result: &KvResult) -> String {
    match result {
        KvResult::Value(Some(v)) => v.to_string(),
        KvResult::Value(None) => "nil".to_string(),
        KvResult::Ok => "ok".to_string(),
        KvResult::CasFailed { actual } => match actual {
            Some(v) => format!("cas-failed(actual {v})"),
            None => "cas-failed(actual nil)".to_string(),
        },
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct History {
    ops: Vec<Operation>,
    /// Where each in-flight request lives, so a response can find it.
    index: BTreeMap<(ClientId, u64), usize>,
}

impl History {
    pub fn new() -> Self {
        History::default()
    }

    /// Record an attempt at a request.
    ///
    /// A repeat of a (client, seq) that is still in flight is a *retry*, not a
    /// new operation: the invocation time stays at the first attempt and only
    /// the attempt count moves. Recording retries as separate operations would
    /// invent concurrency that never existed and make histories look
    /// linearizable that are not.
    pub fn invoke(&mut self, process: ClientId, seq: u64, command: KvCommand, at: Tick) -> OpId {
        if let Some(i) = self.index.get(&(process, seq)).copied() {
            if self.ops[i].outcome == Outcome::Pending {
                self.ops[i].attempts += 1;
                return self.ops[i].id;
            }
        }
        let id = self.ops.len() as OpId;
        self.index.insert((process, seq), self.ops.len());
        self.ops.push(Operation {
            id,
            process,
            seq,
            command,
            invoked: at,
            outcome: Outcome::Pending,
            attempts: 1,
            refusals: 0,
        });
        id
    }

    /// Record a response. Ignored if this request already has one — a second
    /// leader re-applying the same entry must not overwrite what the client was
    /// originally told.
    pub fn complete(&mut self, process: ClientId, seq: u64, at: Tick, result: KvResult) {
        if let Some(op) = self.lookup(process, seq) {
            if op.outcome == Outcome::Pending {
                op.outcome = Outcome::Completed { at, result };
            }
        }
    }

    /// One attempt was refused outright ("not leader").
    ///
    /// This does NOT close the operation: the client is expected to try
    /// somewhere else with the same sequence number, and the operation is not
    /// over until it succeeds or the client gives up.
    pub fn refuse(&mut self, process: ClientId, seq: u64) {
        if let Some(op) = self.lookup(process, seq) {
            if op.outcome == Outcome::Pending {
                op.refusals += 1;
            }
        }
    }

    /// The client stopped trying.
    ///
    /// If *every* attempt was explicitly refused, the command never entered any
    /// log and definitely did not happen — which is worth knowing, because it
    /// removes the operation from the checker's search entirely. If any attempt
    /// simply went unanswered, it may have committed anyway, and the operation
    /// stays PENDING.
    pub fn abandon(&mut self, process: ClientId, seq: u64, at: Tick) {
        if let Some(op) = self.lookup(process, seq) {
            if op.outcome == Outcome::Pending && op.refusals >= op.attempts {
                op.outcome = Outcome::Rejected { at };
            }
        }
    }

    pub fn outcome_of(&self, process: ClientId, seq: u64) -> Option<&Outcome> {
        let i = *self.index.get(&(process, seq))?;
        self.ops.get(i).map(|o| &o.outcome)
    }

    pub fn is_answered(&self, process: ClientId, seq: u64) -> bool {
        !matches!(self.outcome_of(process, seq), Some(Outcome::Pending) | None)
    }

    fn lookup(&mut self, process: ClientId, seq: u64) -> Option<&mut Operation> {
        let i = *self.index.get(&(process, seq))?;
        self.ops.get_mut(i)
    }

    pub fn operations(&self) -> &[Operation] {
        &self.ops
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn completed(&self) -> usize {
        self.ops
            .iter()
            .filter(|o| matches!(o.outcome, Outcome::Completed { .. }))
            .count()
    }

    pub fn pending(&self) -> usize {
        self.ops
            .iter()
            .filter(|o| o.outcome == Outcome::Pending)
            .count()
    }

    /// Operations that had at least one attempt explicitly refused.
    ///
    /// Distinct from `rejected`: a refusal is a failed attempt, and the
    /// operation is only *rejected* once the client gives up having been
    /// refused every time.
    pub fn refused(&self) -> usize {
        self.ops.iter().filter(|o| o.refusals > 0).count()
    }

    pub fn rejected(&self) -> usize {
        self.ops
            .iter()
            .filter(|o| matches!(o.outcome, Outcome::Rejected { .. }))
            .count()
    }

    /// Split into independent per-key sub-histories.
    ///
    /// This is P-compositionality: a history over a set of independent objects
    /// is linearizable exactly when each object's sub-history is. Every command
    /// here touches exactly one key, so the split is sound — and it is the
    /// difference between a check that finishes and one that does not, because
    /// the search is exponential in the size of what it is given.
    pub fn by_key(&self) -> BTreeMap<&str, Vec<&Operation>> {
        let mut out: BTreeMap<&str, Vec<&Operation>> = BTreeMap::new();
        for op in &self.ops {
            out.entry(op.key()).or_default().push(op);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &str, value: &str) -> KvCommand {
        KvCommand::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    #[test]
    fn an_operation_starts_pending() {
        let mut h = History::new();
        h.invoke(1, 0, put("a", "1"), 10);
        assert_eq!(h.pending(), 1);
        assert_eq!(h.completed(), 0);
        assert_eq!(h.operations()[0].responded(), None);
    }

    #[test]
    fn completing_records_the_result_and_time() {
        let mut h = History::new();
        h.invoke(1, 0, put("a", "1"), 10);
        h.complete(1, 0, 25, KvResult::Ok);
        assert_eq!(h.pending(), 0);
        assert_eq!(h.operations()[0].responded(), Some(25));
        assert_eq!(h.operations()[0].result(), Some(&KvResult::Ok));
    }

    #[test]
    fn a_second_response_does_not_overwrite_the_first() {
        // A later leader re-applying the same entry must not change what the
        // client was told it saw.
        let mut h = History::new();
        h.invoke(1, 0, KvCommand::Get { key: "a".into() }, 10);
        h.complete(1, 0, 20, KvResult::Value(Some("first".into())));
        h.complete(1, 0, 30, KvResult::Value(Some("second".into())));
        assert_eq!(
            h.operations()[0].result(),
            Some(&KvResult::Value(Some("first".into())))
        );
    }

    #[test]
    fn a_rejection_is_not_the_same_as_silence() {
        let mut h = History::new();
        h.invoke(1, 0, put("a", "1"), 10);
        h.refuse(1, 0);
        h.abandon(1, 0, 12);
        h.invoke(1, 1, put("a", "2"), 20);
        assert_eq!(h.rejected(), 1);
        assert_eq!(h.pending(), 1);
    }

    #[test]
    fn a_retry_extends_the_operation_rather_than_creating_one() {
        let mut h = History::new();
        h.invoke(1, 0, put("a", "1"), 10);
        h.invoke(1, 0, put("a", "1"), 50);
        h.invoke(1, 0, put("a", "1"), 90);
        assert_eq!(h.len(), 1, "a retry is the same logical operation");
        assert_eq!(
            h.operations()[0].invoked,
            10,
            "invoked at the FIRST attempt"
        );
        assert_eq!(h.operations()[0].attempts, 3);

        h.complete(1, 0, 120, KvResult::Ok);
        assert_eq!(h.operations()[0].responded(), Some(120));
    }

    #[test]
    fn abandoning_after_a_silent_attempt_stays_pending() {
        let mut h = History::new();
        h.invoke(1, 0, put("a", "1"), 10);
        h.refuse(1, 0);
        // A second attempt went unanswered: it may have committed after all.
        h.invoke(1, 0, put("a", "1"), 50);
        h.abandon(1, 0, 90);
        assert_eq!(h.pending(), 1, "silence is not a refusal");
        assert_eq!(h.rejected(), 0);
    }

    #[test]
    fn a_new_sequence_number_after_completion_is_a_new_operation() {
        let mut h = History::new();
        h.invoke(1, 0, put("a", "1"), 10);
        h.complete(1, 0, 20, KvResult::Ok);
        h.invoke(1, 1, put("a", "2"), 30);
        assert_eq!(h.len(), 2);
    }

    #[test]
    fn responses_to_unknown_requests_are_ignored() {
        let mut h = History::new();
        h.complete(9, 9, 5, KvResult::Ok);
        assert!(h.is_empty());
    }

    #[test]
    fn keys_split_into_independent_sub_histories() {
        let mut h = History::new();
        h.invoke(1, 0, put("a", "1"), 1);
        h.invoke(1, 1, put("b", "2"), 2);
        h.invoke(2, 0, put("a", "3"), 3);
        let by_key = h.by_key();
        assert_eq!(by_key.len(), 2);
        assert_eq!(by_key["a"].len(), 2);
        assert_eq!(by_key["b"].len(), 1);
    }
}
