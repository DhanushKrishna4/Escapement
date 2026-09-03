//! Client request generators.
//!
//! Lives here rather than in the fuzzer so that tests, the fuzzer and the
//! browser UI all drive the cluster the same way. Everything comes from a
//! seeded PRNG on its own stream, so a workload is as reproducible as the run
//! it drives.

use kvstore::KvCommand;
use raft::{ClientId, NodeId, Rng, Tick};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadConfig {
    /// Distinct logical clients issuing requests concurrently.
    pub clients: u32,
    /// Key space. Small on purpose: contention is what makes a history
    /// interesting to check, and per-key decomposition is what makes checking
    /// it affordable (step 9).
    pub keys: u32,
    /// Relative weights for the operation mix.
    pub weight_put: u32,
    pub weight_get: u32,
    pub weight_cas: u32,
    pub weight_delete: u32,
    /// Chance per mille of aiming at some node other than the current leader,
    /// modelling a client with a stale leader hint.
    pub stale_target_permille: u32,
    /// Ticks between requests, sampled uniformly.
    pub gap_min: Tick,
    pub gap_max: Tick,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            clients: 3,
            keys: 4,
            weight_put: 5,
            weight_get: 3,
            weight_cas: 2,
            weight_delete: 1,
            stale_target_permille: 150,
            gap_min: 80,
            gap_max: 300,
        }
    }
}

impl WorkloadConfig {
    /// Writes only, one client. The simplest thing that still replicates.
    pub fn writes_only() -> Self {
        WorkloadConfig {
            clients: 1,
            weight_get: 0,
            weight_cas: 0,
            weight_delete: 0,
            ..WorkloadConfig::default()
        }
    }

    /// Heavy contention on very few keys.
    pub fn contended() -> Self {
        WorkloadConfig {
            clients: 5,
            keys: 2,
            weight_cas: 5,
            ..WorkloadConfig::default()
        }
    }

    fn total_weight(&self) -> u32 {
        self.weight_put + self.weight_get + self.weight_cas + self.weight_delete
    }
}

#[derive(Clone, Debug)]
pub struct Workload {
    cfg: WorkloadConfig,
    rng: Rng,
    /// Per-client monotonic sequence numbers. A retry reuses its number, which
    /// is what deduplication will key on in step 12.
    next_seq: Vec<u64>,
    issued: u64,
}

/// One generated request, before it has been aimed at a node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub client: ClientId,
    pub seq: u64,
    pub command: KvCommand,
}

impl Workload {
    pub fn new(seed: u64, cfg: WorkloadConfig) -> Self {
        assert!(
            cfg.clients > 0 && cfg.keys > 0,
            "a workload needs clients and keys"
        );
        assert!(
            cfg.total_weight() > 0,
            "a workload needs at least one operation kind"
        );
        let clients = cfg.clients as usize;
        Workload {
            cfg,
            // Its own derived stream, so adding a draw in the simulator does
            // not change what the clients ask for.
            rng: Rng::derive(seed, 0x574f_524b_4c44_0000),
            next_seq: vec![0; clients],
            issued: 0,
        }
    }

    pub fn issued(&self) -> u64 {
        self.issued
    }

    pub fn config(&self) -> &WorkloadConfig {
        &self.cfg
    }

    pub fn next_request(&mut self) -> Request {
        let client = self.rng.gen_range(0, self.cfg.clients as u64) as ClientId;
        let seq = self.next_seq[client as usize];
        self.next_seq[client as usize] += 1;
        self.issued += 1;

        let key = format!("k{}", self.rng.gen_range(0, self.cfg.keys as u64));
        let value = format!("v{}", self.issued);

        let roll = self.rng.gen_range(0, self.cfg.total_weight() as u64) as u32;
        let command = if roll < self.cfg.weight_put {
            KvCommand::Put { key, value }
        } else if roll < self.cfg.weight_put + self.cfg.weight_get {
            KvCommand::Get { key }
        } else if roll < self.cfg.weight_put + self.cfg.weight_get + self.cfg.weight_cas {
            // Half the time expect nothing, half the time expect a value that
            // may or may not be there. Both outcomes are worth generating: a
            // CAS is a read and a write at one linearization point.
            let expect = if self.rng.chance(1, 2) {
                None
            } else {
                Some(format!("v{}", self.rng.gen_range(0, self.issued.max(1))))
            };
            KvCommand::Cas { key, expect, value }
        } else {
            KvCommand::Delete { key }
        };

        Request {
            client,
            seq,
            command,
        }
    }

    /// Which node to send to. Usually the leader, sometimes not.
    pub fn target(&mut self, leader: Option<NodeId>, nodes: usize) -> NodeId {
        let stale = self.cfg.stale_target_permille > 0
            && self.rng.chance(self.cfg.stale_target_permille as u64, 1000);
        match leader {
            Some(id) if !stale => id,
            _ => self.rng.gen_range(0, nodes as u64) as NodeId,
        }
    }

    /// Undo the most recent sequence number handed out for a client.
    ///
    /// Used when a generated request is not actually sent, so a client's
    /// numbering has no holes in it.
    pub fn rewind(&mut self, client: ClientId) {
        let slot = &mut self.next_seq[client as usize];
        *slot = slot.saturating_sub(1);
    }

    pub fn gap(&mut self) -> Tick {
        if self.cfg.gap_max > self.cfg.gap_min {
            self.rng.gen_range(self.cfg.gap_min, self.cfg.gap_max)
        } else {
            self.cfg.gap_min
        }
    }
}

/// A client that keeps trying.
///
/// Retries are what make deduplication testable: a client that never hears back
/// resends with the **same** sequence number, so the command can reach the log
/// twice. Without that, the session table in the state machine never does
/// anything and "exactly once" is an untested claim.
pub struct ClientDriver {
    work: Workload,
    in_flight: Vec<InFlight>,
    /// Ticks to wait for an answer before resending.
    pub retry_after: Tick,
    /// Attempts before the client gives up.
    pub max_attempts: u32,
    retries: u64,
    abandoned: u64,
}

struct InFlight {
    client: ClientId,
    seq: u64,
    command: KvCommand,
    attempts: u32,
    resend_at: Tick,
}

impl ClientDriver {
    pub fn new(seed: u64, cfg: WorkloadConfig, retry_after: Tick, max_attempts: u32) -> Self {
        ClientDriver {
            work: Workload::new(seed, cfg),
            in_flight: Vec::new(),
            retry_after,
            max_attempts,
            retries: 0,
            abandoned: 0,
        }
    }

    /// Retries sent, and requests the client stopped trying.
    pub fn retries(&self) -> u64 {
        self.retries
    }
    pub fn abandoned(&self) -> u64 {
        self.abandoned
    }
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }
    pub fn issued(&self) -> u64 {
        self.work.issued()
    }

    /// Issue one new request, resend anything overdue, and drop anything the
    /// client has given up on. Returns how long to run before the next call.
    ///
    /// **One outstanding request per client.** §8's session table keeps a
    /// single slot per client — the latest sequence number and its response —
    /// so a client that has several requests in flight can have an old retry
    /// arrive after a newer request has already claimed the slot, and be handed
    /// the wrong command's answer. The fuzzer found exactly that: a `put` that
    /// returned `cas-failed`. Concurrency here comes from having several
    /// clients, which is the model the paper describes.
    pub fn step(&mut self, sim: &mut crate::Cluster) -> Tick {
        let now = sim.now();
        let nodes = sim.node_ids().len();

        // Clear out anything that has been answered, and resend the rest.
        let mut still_waiting = Vec::new();
        let mut resend = Vec::new();
        let mut give_up = Vec::new();
        for mut f in self.in_flight.drain(..) {
            if sim.history().is_answered(f.client, f.seq) {
                continue;
            }
            if now >= f.resend_at {
                if f.attempts >= self.max_attempts {
                    give_up.push((f.client, f.seq));
                    continue;
                }
                f.attempts += 1;
                f.resend_at = now + self.retry_after;
                resend.push((f.client, f.seq, f.command.clone()));
            }
            still_waiting.push(f);
        }
        self.in_flight = still_waiting;

        for (client, seq) in give_up {
            self.abandoned += 1;
            sim.abandon_request(client, seq);
        }
        for (client, seq, command) in resend {
            self.retries += 1;
            // Aim somewhere else this time: a stale leader hint is the usual
            // reason a client is retrying at all.
            let target = self.work.target(sim.leader(), nodes);
            sim.submit(target, client, seq, command);
        }

        // Only start a new request for a client that is not already waiting on
        // one.
        let req = self.work.next_request();
        if !self.in_flight.iter().any(|f| f.client == req.client) {
            let target = self.work.target(sim.leader(), nodes);
            sim.submit(target, req.client, req.seq, req.command.clone());
            self.in_flight.push(InFlight {
                client: req.client,
                seq: req.seq,
                command: req.command,
                attempts: 1,
                resend_at: now + self.retry_after,
            });
        } else {
            // Hand the sequence number back so the client's numbering stays
            // gapless -- a gap would look like a request that vanished.
            self.work.rewind(req.client);
        }

        self.work.gap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_generates_the_same_requests() {
        let gen = || {
            let mut w = Workload::new(9, WorkloadConfig::default());
            (0..200).map(|_| w.next_request()).collect::<Vec<_>>()
        };
        assert_eq!(gen(), gen());
    }

    #[test]
    fn sequence_numbers_are_per_client_and_monotonic() {
        let mut w = Workload::new(1, WorkloadConfig::default());
        let mut expected = [0u64; 3];
        for _ in 0..300 {
            let r = w.next_request();
            assert_eq!(r.seq, expected[r.client as usize]);
            expected[r.client as usize] += 1;
        }
    }

    #[test]
    fn the_mix_respects_its_weights() {
        let mut w = Workload::new(2, WorkloadConfig::writes_only());
        for _ in 0..200 {
            assert!(matches!(w.next_request().command, KvCommand::Put { .. }));
        }
    }

    #[test]
    fn requests_stay_inside_the_key_space() {
        let mut w = Workload::new(
            3,
            WorkloadConfig {
                keys: 3,
                ..Default::default()
            },
        );
        for _ in 0..500 {
            let key = w.next_request().command.key().to_string();
            assert!(
                ["k0", "k1", "k2"].contains(&key.as_str()),
                "unexpected key {key}"
            );
        }
    }

    #[test]
    fn targeting_usually_follows_the_leader_but_not_always() {
        let mut w = Workload::new(4, WorkloadConfig::default());
        let mut on_leader = 0;
        for _ in 0..1000 {
            if w.target(Some(2), 5) == 2 {
                on_leader += 1;
            }
        }
        assert!(
            on_leader > 750,
            "too few requests reached the leader: {on_leader}"
        );
        assert!(
            on_leader < 990,
            "a stale hint should sometimes miss: {on_leader}"
        );
    }

    #[test]
    fn with_no_leader_it_still_picks_a_node() {
        let mut w = Workload::new(5, WorkloadConfig::default());
        for _ in 0..200 {
            assert!(w.target(None, 5) < 5);
        }
    }
}
