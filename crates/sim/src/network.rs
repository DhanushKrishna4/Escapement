//! The network model.
//!
//! Every message a node sends passes through here, and this is where the
//! interesting faults live. All of it is driven by the simulator's seeded PRNG,
//! so a lossy, reordering, duplicating network is still perfectly reproducible.
//!
//! Probabilities are integers per mille, never floats: floating point is
//! deterministic within a platform but not guaranteed across them, and these
//! values decide behaviour.
//!
//! Partitions — including asymmetric ones — are step 6 and land on top of this.

use std::collections::BTreeMap;

use raft::{NodeId, Rng, Tick};

use crate::faults::Partitions;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LatencyModel {
    /// Every message takes exactly this long. Useful as a control: with fixed
    /// latency, messages on a link cannot reorder on their own.
    Fixed(Tick),
    /// Uniform in `[min, max)`. Reordering falls out of this for free.
    Uniform { min: Tick, max: Tick },
    /// Mostly fast, occasionally terrible.
    ///
    /// This is where the interesting bugs live. A uniform distribution rarely
    /// produces the "message arrives after everyone gave up on it" case that
    /// breaks naive implementations, because its worst case is only a little
    /// worse than its average. A long tail produces it constantly.
    LongTail {
        min: Tick,
        max: Tick,
        /// Chance per mille that a message takes the tail path.
        tail_permille: u32,
        /// How much worse the tail is.
        tail_multiplier: u32,
    },
}

impl LatencyModel {
    fn sample(&self, rng: &mut Rng) -> Tick {
        match *self {
            LatencyModel::Fixed(d) => d,
            LatencyModel::Uniform { min, max } => {
                if max > min {
                    rng.gen_range(min, max)
                } else {
                    min
                }
            }
            LatencyModel::LongTail {
                min,
                max,
                tail_permille,
                tail_multiplier,
            } => {
                let base = if max > min {
                    rng.gen_range(min, max)
                } else {
                    min
                };
                if tail_permille > 0 && rng.chance(tail_permille as u64, 1000) {
                    base.saturating_mul(tail_multiplier as Tick)
                } else {
                    base
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub latency: LatencyModel,
    /// Chance per mille that a message is silently lost.
    pub drop_permille: u32,
    /// Chance per mille that a message is delivered twice.
    ///
    /// Raft has to be idempotent under duplicates: a repeated AppendEntries
    /// must not truncate a log that has moved on, and a repeated vote must not
    /// be counted twice.
    pub duplicate_permille: u32,
    /// Chance per mille that a message deliberately overtakes an earlier one on
    /// the same link.
    ///
    /// Variable latency already reorders messages, but only by chance. This
    /// forces the case so it shows up even on a link that would otherwise be
    /// well behaved.
    pub reorder_permille: u32,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig::perfect()
    }
}

impl NetworkConfig {
    /// No faults at all. 10 ticks against a 50-tick heartbeat and a 150-300
    /// tick election timeout: stable, and slow enough to see messages in flight
    /// in the visualizer.
    pub fn perfect() -> Self {
        NetworkConfig {
            latency: LatencyModel::Fixed(10),
            drop_permille: 0,
            duplicate_permille: 0,
            reorder_permille: 0,
        }
    }

    /// A network that misbehaves in every way, but mildly enough that the
    /// cluster still makes steady progress.
    pub fn flaky() -> Self {
        NetworkConfig {
            latency: LatencyModel::Uniform { min: 5, max: 25 },
            drop_permille: 50,
            duplicate_permille: 20,
            reorder_permille: 50,
        }
    }

    /// Mostly fast with rare very slow messages. The schedule that finds
    /// ordering bugs.
    pub fn long_tail() -> Self {
        NetworkConfig {
            latency: LatencyModel::LongTail {
                min: 5,
                max: 20,
                tail_permille: 30,
                tail_multiplier: 20,
            },
            drop_permille: 20,
            duplicate_permille: 20,
            reorder_permille: 30,
        }
    }

    /// Bad enough to threaten liveness: a quarter of messages vanish and the
    /// tail is brutal. Safety must still hold.
    pub fn hostile() -> Self {
        NetworkConfig {
            latency: LatencyModel::LongTail {
                min: 5,
                max: 40,
                tail_permille: 100,
                tail_multiplier: 15,
            },
            drop_permille: 250,
            duplicate_permille: 80,
            reorder_permille: 100,
        }
    }

    pub fn is_perfect(&self) -> bool {
        self.drop_permille == 0
            && self.duplicate_permille == 0
            && self.reorder_permille == 0
            && matches!(self.latency, LatencyModel::Fixed(_))
    }
}

/// What the network decided to do with one message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Routed {
    /// Blocked by a partition. Indistinguishable from a drop to the sender, but
    /// tracked separately so a trace shows *why* a message vanished.
    Partitioned,
    /// Swallowed. The sender is never told.
    Dropped,
    Once(Tick),
    /// Delivered twice, at two independently sampled times.
    Twice(Tick, Tick),
}

/// Coverage counters. A fuzz run that reports "0 violations" is only meaningful
/// alongside evidence that the faults actually fired.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkStats {
    pub sent: u64,
    pub delivered: u64,
    pub dropped: u64,
    pub partitioned: u64,
    pub duplicated: u64,
    pub reordered: u64,
    pub total_delay: u64,
    pub max_delay: Tick,
}

impl NetworkStats {
    pub fn mean_delay(&self) -> Tick {
        self.total_delay.checked_div(self.delivered).unwrap_or(0)
    }
}

#[derive(Clone, Debug)]
pub struct Network {
    cfg: NetworkConfig,
    partitions: Partitions,
    stats: NetworkStats,
    /// Furthest-out delivery already scheduled on each directed link, so an
    /// explicit reorder knows what it has to overtake. `BTreeMap` because this
    /// influences delivery times, and a `HashMap` would make the run depend on
    /// a per-process hash seed.
    last_delivery: BTreeMap<(NodeId, NodeId), Tick>,
}

impl Network {
    pub fn new(cfg: NetworkConfig) -> Self {
        Network {
            cfg,
            partitions: Partitions::new(),
            stats: NetworkStats::default(),
            last_delivery: BTreeMap::new(),
        }
    }

    pub fn config(&self) -> &NetworkConfig {
        &self.cfg
    }

    pub fn stats(&self) -> &NetworkStats {
        &self.stats
    }

    pub fn partitions(&self) -> &Partitions {
        &self.partitions
    }

    pub fn partitions_mut(&mut self) -> &mut Partitions {
        &mut self.partitions
    }

    /// Decide what happens to a message sent at `now`.
    ///
    /// Every random draw is guarded by its knob being non-zero, so a fault that
    /// is turned off consumes no randomness and leaves the rest of the run
    /// byte-identical. That makes "same seed, one knob changed" a meaningful
    /// comparison rather than a completely different universe.
    pub fn route(&mut self, now: Tick, from: NodeId, to: NodeId, rng: &mut Rng) -> Routed {
        self.stats.sent += 1;

        // Partitions come first, and consume no randomness: a link that is down
        // is down, not down with some probability. Checking it before the drop
        // roll also means partitioning a link does not shift the random stream
        // for every other message.
        if !self.partitions.reachable(from, to) {
            self.stats.partitioned += 1;
            return Routed::Partitioned;
        }

        if self.cfg.drop_permille > 0 && rng.chance(self.cfg.drop_permille as u64, 1000) {
            self.stats.dropped += 1;
            return Routed::Dropped;
        }

        let first = self.schedule(now, from, to, rng);

        if self.cfg.duplicate_permille > 0 && rng.chance(self.cfg.duplicate_permille as u64, 1000) {
            let second = self.schedule(now, from, to, rng);
            self.stats.duplicated += 1;
            self.stats.delivered += 2;
            return Routed::Twice(first, second);
        }

        self.stats.delivered += 1;
        Routed::Once(first)
    }

    fn schedule(&mut self, now: Tick, from: NodeId, to: NodeId, rng: &mut Rng) -> Tick {
        // At least one tick, always: a message delivered in the instant it was
        // sent would let a node observe its own effects with no elapsed time
        // and would hide real ordering bugs.
        let delay = self.cfg.latency.sample(rng).max(1);
        let mut at = now + delay;

        if self.cfg.reorder_permille > 0 && rng.chance(self.cfg.reorder_permille as u64, 1000) {
            if let Some(prev) = self.last_delivery.get(&(from, to)).copied() {
                // Only counts as a reorder if this message would otherwise have
                // arrived after the one it is overtaking.
                if prev > now + 1 && at >= prev {
                    at = (prev - 1).max(now + 1);
                    self.stats.reordered += 1;
                }
            }
        }

        let furthest = self.last_delivery.entry((from, to)).or_insert(0);
        *furthest = (*furthest).max(at);

        self.stats.total_delay += at - now;
        self.stats.max_delay = self.stats.max_delay.max(at - now);
        at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rng() -> Rng {
        Rng::new(12345)
    }

    #[test]
    fn a_perfect_network_delivers_everything_on_time() {
        let mut net = Network::new(NetworkConfig::perfect());
        let mut r = rng();
        for tick in 0..1000 {
            assert_eq!(net.route(tick, 0, 1, &mut r), Routed::Once(tick + 10));
        }
        assert_eq!(net.stats().dropped, 0);
        assert_eq!(net.stats().duplicated, 0);
        assert_eq!(net.stats().delivered, 1000);
    }

    #[test]
    fn a_perfect_network_draws_no_randomness() {
        // A knob at zero must be a true no-op, or turning one fault on would
        // shift every other draw and produce a completely different run.
        let mut net = Network::new(NetworkConfig::perfect());
        let mut used = rng();
        for tick in 0..500 {
            net.route(tick, 0, 1, &mut used);
        }
        let mut untouched = rng();
        assert_eq!(used.next_u64(), untouched.next_u64());
    }

    #[test]
    fn drops_happen_at_roughly_the_configured_rate() {
        let cfg = NetworkConfig {
            drop_permille: 100,
            ..NetworkConfig::perfect()
        };
        let mut net = Network::new(cfg);
        let mut r = rng();
        for tick in 0..10_000 {
            net.route(tick, 0, 1, &mut r);
        }
        let dropped = net.stats().dropped;
        assert!(
            (800..1200).contains(&dropped),
            "expected ~1000 drops in 10000, got {dropped}"
        );
    }

    #[test]
    fn duplicates_deliver_the_message_twice() {
        let cfg = NetworkConfig {
            duplicate_permille: 1000,
            ..NetworkConfig::perfect()
        };
        let mut net = Network::new(cfg);
        let mut r = rng();
        assert!(matches!(net.route(0, 0, 1, &mut r), Routed::Twice(_, _)));
        assert_eq!(net.stats().duplicated, 1);
        assert_eq!(net.stats().delivered, 2);
    }

    #[test]
    fn explicit_reordering_makes_a_message_overtake() {
        let cfg = NetworkConfig {
            latency: LatencyModel::Fixed(100),
            reorder_permille: 1000,
            ..NetworkConfig::perfect()
        };
        let mut net = Network::new(cfg);
        let mut r = rng();
        let first = net.route(0, 0, 1, &mut r);
        let second = net.route(0, 0, 1, &mut r);
        match (first, second) {
            (Routed::Once(a), Routed::Once(b)) => {
                assert!(
                    b < a,
                    "the second message should have overtaken: {b} vs {a}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(net.stats().reordered, 1);
    }

    #[test]
    fn fixed_latency_alone_never_reorders() {
        let mut net = Network::new(NetworkConfig::perfect());
        let mut r = rng();
        let mut last = 0;
        for tick in 0..500 {
            match net.route(tick, 0, 1, &mut r) {
                Routed::Once(at) => {
                    assert!(at >= last);
                    last = at;
                }
                other => panic!("unexpected: {other:?}"),
            }
        }
    }

    #[test]
    fn the_long_tail_actually_has_a_tail() {
        let cfg = NetworkConfig {
            latency: LatencyModel::LongTail {
                min: 5,
                max: 20,
                tail_permille: 100,
                tail_multiplier: 20,
            },
            ..NetworkConfig::perfect()
        };
        let mut net = Network::new(cfg);
        let mut r = rng();
        for tick in 0..10_000 {
            net.route(tick, 0, 1, &mut r);
        }
        let stats = net.stats();
        assert!(
            stats.max_delay >= 100,
            "no tail: max delay {}",
            stats.max_delay
        );
        assert!(
            stats.mean_delay() < 40,
            "the tail should be rare, but the mean is {}",
            stats.mean_delay()
        );
    }

    #[test]
    fn routing_is_reproducible() {
        let run = || {
            let mut net = Network::new(NetworkConfig::hostile());
            let mut r = Rng::new(99);
            (0..2000)
                .map(|tick| net.route(tick, tick as u32 % 3, 4, &mut r))
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }
}
