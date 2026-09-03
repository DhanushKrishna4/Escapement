//! The replicated state machine: a key/value store.
//!
//! Deliberately boring. Its only jobs are to be a *deterministic* function of
//! the sequence of commands applied to it, and to be simple enough that when
//! the linearizability checker says a history is impossible, the store is not
//! the thing in doubt.
//!
//! Determinism notes:
//! * `BTreeMap`, not `HashMap` -- iteration order feeds `snapshot()`, which
//!   feeds equality checks between replicas.
//! * Commands are encoded with `serde_json`, which emits struct fields in
//!   declaration order and `BTreeMap` keys in sorted order, so the same command
//!   always produces the same bytes.

use std::collections::BTreeMap;

use raft::{ClientId, Command};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Compare-and-set. Present because it makes histories much more
    /// interesting to check: it is a read and a write in one linearization
    /// point.
    Cas {
        key: String,
        expect: Option<String>,
        value: String,
    },
}

impl KvCommand {
    /// The key this command touches. The linearizability checker uses it to
    /// decompose a history into independent per-key sub-histories, which is the
    /// difference between a check that finishes and one that does not.
    pub fn key(&self) -> &str {
        match self {
            KvCommand::Get { key }
            | KvCommand::Put { key, .. }
            | KvCommand::Delete { key }
            | KvCommand::Cas { key, .. } => key,
        }
    }

    pub fn is_read_only(&self) -> bool {
        matches!(self, KvCommand::Get { .. })
    }

    pub fn encode(&self) -> Command {
        Command(serde_json::to_vec(self).expect("kv command is always serializable"))
    }

    pub fn decode(cmd: &Command) -> Result<KvCommand, serde_json::Error> {
        serde_json::from_slice(&cmd.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvResult {
    Value(Option<String>),
    Ok,
    /// A `Cas` whose `expect` did not match; carries what was actually there.
    CasFailed {
        actual: Option<String>,
    },
}

/// What a client was last told.
///
/// Kept in the state machine rather than on the leader, which is the whole
/// point: it is replicated, so a retry that lands on a *different* leader is
/// still recognised as a duplicate. A leader-local table would forget
/// everything the moment leadership moved, which is exactly when clients retry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Session {
    last_seq: u64,
    last_result: KvResult,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvStore {
    data: BTreeMap<String, String>,
    /// Per-client deduplication state (§8).
    sessions: BTreeMap<ClientId, Session>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore::default()
    }

    /// Apply a command on behalf of a client, deduplicating retries (§8).
    ///
    /// A client that never hears back retries with the *same* sequence number.
    /// Without this, the retry is a second, indistinguishable entry in the log
    /// and the command is applied twice — so "exactly once" is a lie, a
    /// compare-and-set can succeed twice, and the linearizability checker will
    /// eventually catch it.
    ///
    /// Only the most recent response per client is kept, which is what the
    /// paper describes — and it is why §8 assumes a client has **one
    /// outstanding request at a time**. With several in flight, an old retry
    /// can arrive after a newer request has claimed the slot, and the cached
    /// answer then belongs to a different command entirely. `ClientDriver`
    /// enforces the one-at-a-time rule; concurrency comes from having several
    /// clients.
    ///
    /// A duplicate whose sequence number is *older* than the slot is still
    /// never re-applied. Its response is not deliverable either — the client
    /// was answered long ago, and the simulator drops a second answer for a
    /// request it has already closed.
    pub fn apply_for(&mut self, client: Option<(ClientId, u64)>, cmd: &KvCommand) -> KvResult {
        let Some((client, seq)) = client else {
            return self.apply(cmd);
        };
        if let Some(session) = self.sessions.get(&client) {
            if seq <= session.last_seq {
                return session.last_result.clone();
            }
        }
        let result = self.apply(cmd);
        // Reads do not need a session slot, and keeping them out means a
        // client's writes are not evicted by its own reads.
        if !cmd.is_read_only() {
            self.sessions.insert(
                client,
                Session {
                    last_seq: seq,
                    last_result: result.clone(),
                },
            );
        }
        result
    }

    /// The highest sequence number applied for a client, if any.
    pub fn last_seq(&self, client: ClientId) -> Option<u64> {
        self.sessions.get(&client).map(|s| s.last_seq)
    }

    pub fn apply(&mut self, cmd: &KvCommand) -> KvResult {
        match cmd {
            KvCommand::Get { key } => KvResult::Value(self.data.get(key).cloned()),
            KvCommand::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                KvResult::Ok
            }
            KvCommand::Delete { key } => {
                self.data.remove(key);
                KvResult::Ok
            }
            KvCommand::Cas { key, expect, value } => {
                let actual = self.data.get(key).cloned();
                if &actual == expect {
                    self.data.insert(key.clone(), value.clone());
                    KvResult::Ok
                } else {
                    KvResult::CasFailed { actual }
                }
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Ordered view of the whole store, for comparing replicas.
    pub fn snapshot(&self) -> &BTreeMap<String, String> {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get() {
        let mut kv = KvStore::new();
        assert_eq!(
            kv.apply(&KvCommand::Get { key: "a".into() }),
            KvResult::Value(None)
        );
        assert_eq!(
            kv.apply(&KvCommand::Put {
                key: "a".into(),
                value: "1".into()
            }),
            KvResult::Ok
        );
        assert_eq!(
            kv.apply(&KvCommand::Get { key: "a".into() }),
            KvResult::Value(Some("1".into()))
        );
    }

    #[test]
    fn cas_reports_the_actual_value_on_failure() {
        let mut kv = KvStore::new();
        kv.apply(&KvCommand::Put {
            key: "a".into(),
            value: "1".into(),
        });
        assert_eq!(
            kv.apply(&KvCommand::Cas {
                key: "a".into(),
                expect: Some("2".into()),
                value: "3".into()
            }),
            KvResult::CasFailed {
                actual: Some("1".into())
            }
        );
        assert_eq!(
            kv.apply(&KvCommand::Cas {
                key: "a".into(),
                expect: Some("1".into()),
                value: "3".into()
            }),
            KvResult::Ok
        );
        assert_eq!(kv.get("a"), Some(&"3".to_string()));
    }

    #[test]
    fn encoding_round_trips_and_is_stable() {
        let cmd = KvCommand::Put {
            key: "k".into(),
            value: "v".into(),
        };
        let a = cmd.encode();
        let b = cmd.encode();
        assert_eq!(a, b, "encoding must be byte-stable across calls");
        assert_eq!(KvCommand::decode(&a).unwrap(), cmd);
    }

    #[test]
    fn a_retried_request_is_not_applied_twice() {
        let mut kv = KvStore::new();
        let cas = KvCommand::Cas {
            key: "a".into(),
            expect: None,
            value: "1".into(),
        };
        // First attempt succeeds.
        assert_eq!(kv.apply_for(Some((7, 0)), &cas), KvResult::Ok);
        // The client never heard back and retries with the same sequence
        // number. Applied twice, this compare-and-set would fail the second
        // time and the client would be told something that never happened.
        assert_eq!(kv.apply_for(Some((7, 0)), &cas), KvResult::Ok);
        assert_eq!(kv.get("a"), Some(&"1".to_string()));
    }

    #[test]
    fn a_new_sequence_number_is_applied_normally() {
        let mut kv = KvStore::new();
        kv.apply_for(
            Some((7, 0)),
            &KvCommand::Put {
                key: "a".into(),
                value: "1".into(),
            },
        );
        kv.apply_for(
            Some((7, 1)),
            &KvCommand::Put {
                key: "a".into(),
                value: "2".into(),
            },
        );
        assert_eq!(kv.get("a"), Some(&"2".to_string()));
        assert_eq!(kv.last_seq(7), Some(1));
    }

    #[test]
    fn clients_are_deduplicated_independently() {
        let mut kv = KvStore::new();
        let put = |v: &str| KvCommand::Put {
            key: "a".into(),
            value: v.into(),
        };
        kv.apply_for(Some((1, 0)), &put("from-1"));
        kv.apply_for(Some((2, 0)), &put("from-2"));
        assert_eq!(kv.get("a"), Some(&"from-2".to_string()));
        assert_eq!(kv.last_seq(1), Some(0));
        assert_eq!(kv.last_seq(2), Some(0));
    }

    #[test]
    fn an_older_sequence_number_is_never_re_applied() {
        let mut kv = KvStore::new();
        let put = |v: &str| KvCommand::Put {
            key: "a".into(),
            value: v.into(),
        };
        kv.apply_for(Some((1, 5)), &put("five"));
        kv.apply_for(Some((1, 3)), &put("three"));
        assert_eq!(
            kv.get("a"),
            Some(&"five".to_string()),
            "a stale retry must not overwrite a newer write"
        );
    }

    #[test]
    fn reads_do_not_consume_a_session_slot() {
        let mut kv = KvStore::new();
        kv.apply_for(
            Some((1, 0)),
            &KvCommand::Put {
                key: "a".into(),
                value: "1".into(),
            },
        );
        kv.apply_for(Some((1, 1)), &KvCommand::Get { key: "a".into() });
        // The write's dedup entry must survive its own client's reads.
        assert_eq!(kv.last_seq(1), Some(0));
        assert_eq!(
            kv.apply_for(
                Some((1, 0)),
                &KvCommand::Put {
                    key: "a".into(),
                    value: "z".into()
                }
            ),
            KvResult::Ok
        );
        assert_eq!(
            kv.get("a"),
            Some(&"1".to_string()),
            "the retry was deduplicated"
        );
    }

    #[test]
    fn commands_without_a_client_are_never_deduplicated() {
        let mut kv = KvStore::new();
        let put = KvCommand::Put {
            key: "a".into(),
            value: "1".into(),
        };
        assert_eq!(kv.apply_for(None, &put), KvResult::Ok);
        assert_eq!(kv.apply_for(None, &put), KvResult::Ok);
        assert!(kv.last_seq(0).is_none());
    }

    #[test]
    fn replaying_the_same_commands_gives_the_same_state() {
        let cmds = vec![
            KvCommand::Put {
                key: "b".into(),
                value: "2".into(),
            },
            KvCommand::Put {
                key: "a".into(),
                value: "1".into(),
            },
            KvCommand::Delete { key: "b".into() },
            KvCommand::Cas {
                key: "a".into(),
                expect: Some("1".into()),
                value: "9".into(),
            },
        ];
        let mut x = KvStore::new();
        let mut y = KvStore::new();
        for c in &cmds {
            x.apply(c);
            y.apply(c);
        }
        assert_eq!(x, y);
        assert_eq!(x.snapshot().keys().collect::<Vec<_>>(), vec!["a"]);
    }
}
