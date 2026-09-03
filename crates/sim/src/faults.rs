//! Partitions and the schedules that drive them.
//!
//! Reachability is modelled as a set of *directed* links that are down, not as
//! a set of groups. That costs nothing and buys the asymmetric case: A cannot
//! reach B, but B can still reach A.
//!
//! Asymmetric partitions are worth testing hard because they break assumptions
//! that symmetric ones never touch. A node that can send but not receive keeps
//! timing out and campaigning, incrementing the term every time, and every one
//! of those RequestVotes reaches a healthy cluster and deposes a perfectly good
//! leader. Safety survives that; liveness does not, and the difference is worth
//! seeing.
//!
//! Crashes, restarts and pauses are step 7 and slot into [`Fault`] alongside
//! these.

use std::collections::BTreeSet;

use raft::{NodeId, Rng, Tick};
use serde::{Deserialize, Serialize};

/// Something the simulator does to the cluster at a point in time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    /// Split the cluster in two. Neither side can reach the other.
    Partition { a: Vec<NodeId>, b: Vec<NodeId> },
    /// One-way cut: `from` can no longer reach `to`, but `to` still reaches
    /// `from`. The nastier case, and the one naive implementations miss.
    AsymmetricCut { from: NodeId, to: NodeId },
    /// Cut a single node off in both directions.
    Isolate { node: NodeId },
    /// Restore full connectivity.
    Heal,
    /// Kill a node. Everything volatile is lost; only what reached the disk
    /// survives. Messages sent to it while down are discarded.
    Crash { node: NodeId },
    /// Bring a crashed node back, rebuilt from its durable state alone.
    Restart { node: NodeId },
    /// Freeze a node for `ticks`, then let it resume with stale state.
    ///
    /// Models a stop-the-world GC pause or a descheduled process. Messages that
    /// arrive while it is frozen are held and delivered on resume, the way a
    /// kernel socket buffer would, so the node wakes to a backlog and expired
    /// timers at once. Good at catching anything that assumed time did not move
    /// between two steps.
    Pause { node: NodeId, ticks: Tick },
}

impl Fault {
    pub fn label(&self) -> &'static str {
        match self {
            Fault::Partition { .. } => "partition",
            Fault::AsymmetricCut { .. } => "asymmetric cut",
            Fault::Isolate { .. } => "isolate",
            Fault::Heal => "heal",
            Fault::Crash { .. } => "crash",
            Fault::Restart { .. } => "restart",
            Fault::Pause { .. } => "pause",
        }
    }

    /// Does this fault disturb the network's link state? Crash, restart and
    /// pause are node-level and leave links alone.
    pub fn is_network_fault(&self) -> bool {
        matches!(
            self,
            Fault::Partition { .. }
                | Fault::AsymmetricCut { .. }
                | Fault::Isolate { .. }
                | Fault::Heal
        )
    }
}

/// Which directed links are down.
///
/// `BTreeSet`, not `HashSet`: this is consulted for every message and therefore
/// decides the run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Partitions {
    blocked: BTreeSet<(NodeId, NodeId)>,
}

impl Partitions {
    pub fn new() -> Self {
        Partitions::default()
    }

    /// Can `from` deliver to `to`?
    ///
    /// A node can always reach itself; a node that could not talk to itself
    /// would be modelling a crash, which is a different fault.
    pub fn reachable(&self, from: NodeId, to: NodeId) -> bool {
        from == to || !self.blocked.contains(&(from, to))
    }

    pub fn is_healed(&self) -> bool {
        self.blocked.is_empty()
    }

    pub fn blocked_links(&self) -> &BTreeSet<(NodeId, NodeId)> {
        &self.blocked
    }

    pub fn heal(&mut self) {
        self.blocked.clear();
    }

    /// Block traffic in both directions between the two groups.
    pub fn split(&mut self, a: &[NodeId], b: &[NodeId]) {
        for x in a {
            for y in b {
                self.blocked.insert((*x, *y));
                self.blocked.insert((*y, *x));
            }
        }
    }

    /// Block one direction only.
    pub fn cut(&mut self, from: NodeId, to: NodeId) {
        if from != to {
            self.blocked.insert((from, to));
        }
    }

    pub fn isolate(&mut self, node: NodeId, all: &[NodeId]) {
        for other in all {
            if *other != node {
                self.blocked.insert((node, *other));
                self.blocked.insert((*other, node));
            }
        }
    }

    pub fn apply(&mut self, fault: &Fault, all: &[NodeId]) {
        match fault {
            Fault::Partition { a, b } => self.split(a, b),
            Fault::AsymmetricCut { from, to } => self.cut(*from, *to),
            Fault::Isolate { node } => self.isolate(*node, all),
            Fault::Heal => self.heal(),
            // Node-level faults; the links are untouched.
            Fault::Crash { .. } | Fault::Restart { .. } | Fault::Pause { .. } => {}
        }
    }
}

/// Disk behaviour.
///
/// # What is deliberately NOT modelled
///
/// Silent write loss — an acknowledged persist that never reached the platter —
/// is excluded on purpose. Raft's correctness *assumes* that an acknowledged
/// write is durable; if fsync lies, the algorithm can genuinely violate safety,
/// and that is a documented property of Raft rather than a bug in this
/// implementation. Injecting it would fill the fuzzer's output with real
/// violations that no code change could fix, and a fuzzer with a noise floor is
/// a fuzzer nobody reads.
///
/// # What is modelled
///
/// The torn step: some prefix of a step's persists reaches the disk, then the
/// process dies before any message goes out. This is a legitimate crash point
/// that Raft must survive, and it also covers the fail-stop disk error (the
/// write fails, the process dies).
///
/// It is the only fault that lets a *crash* cause log divergence. Normally a
/// leader appends and broadcasts in the same step, so by the time it can die the
/// messages are already in the network; a torn step is what separates the two.
/// It is also the sharpest test of the persist-before-send ordering contract: a
/// vote that did not reach the disk must not have reached the wire either.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskConfig {
    /// Chance per mille that a step which writes to disk is torn.
    pub torn_step_permille: u32,
    /// How long before a torn node is restarted, sampled uniformly. Models a
    /// supervisor bringing the process back.
    pub restart_min: Tick,
    pub restart_max: Tick,
}

impl Default for DiskConfig {
    fn default() -> Self {
        DiskConfig::reliable()
    }
}

impl DiskConfig {
    pub fn reliable() -> Self {
        DiskConfig {
            torn_step_permille: 0,
            restart_min: 300,
            restart_max: 1_500,
        }
    }

    pub fn flaky() -> Self {
        DiskConfig {
            torn_step_permille: 5,
            restart_min: 300,
            restart_max: 1_500,
        }
    }

    pub fn restart_delay(&self, rng: &mut Rng) -> Tick {
        sample(self.restart_min, self.restart_max, rng)
    }
}

/// How often the simulator disturbs the cluster, and how.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultConfig {
    /// Off by default. A run with no faults is still a useful control.
    pub enabled: bool,
    /// Ticks of connectivity between disturbances, sampled uniformly.
    pub healthy_min: Tick,
    pub healthy_max: Tick,
    /// How long a disturbance lasts before healing, sampled uniformly.
    ///
    /// This wants to straddle the election timeout: shorter and nothing has
    /// time to happen, much longer and the cluster just sits idle.
    pub outage_min: Tick,
    pub outage_max: Tick,
    /// Relative weights for what kind of disturbance to inject.
    pub weight_partition: u32,
    pub weight_asymmetric: u32,
    pub weight_isolate: u32,
    /// Kill a node and bring it back after the outage.
    pub weight_crash: u32,
    /// Freeze a node for the outage span.
    pub weight_pause: u32,
}

impl Default for FaultConfig {
    fn default() -> Self {
        FaultConfig::none()
    }
}

impl FaultConfig {
    pub fn none() -> Self {
        FaultConfig {
            enabled: false,
            healthy_min: 0,
            healthy_max: 1,
            outage_min: 0,
            outage_max: 1,
            weight_partition: 0,
            weight_asymmetric: 0,
            weight_isolate: 0,
            weight_crash: 0,
            weight_pause: 0,
        }
    }

    /// Occasional trouble with long healthy stretches, so the cluster has time
    /// to settle and commit between disturbances.
    pub fn occasional() -> Self {
        FaultConfig {
            enabled: true,
            healthy_min: 1_500,
            healthy_max: 4_000,
            outage_min: 400,
            outage_max: 1_500,
            weight_partition: 3,
            weight_asymmetric: 1,
            weight_isolate: 1,
            weight_crash: 2,
            weight_pause: 1,
        }
    }

    /// Constant churn: outages roughly as long as the healthy stretches.
    pub fn aggressive() -> Self {
        FaultConfig {
            enabled: true,
            healthy_min: 400,
            healthy_max: 1_200,
            outage_min: 400,
            outage_max: 1_600,
            weight_partition: 3,
            weight_asymmetric: 2,
            weight_isolate: 2,
            weight_crash: 3,
            weight_pause: 2,
        }
    }

    /// Only one-way cuts. The case most likely to expose a wrong assumption.
    pub fn asymmetric_only() -> Self {
        FaultConfig {
            enabled: true,
            healthy_min: 600,
            healthy_max: 1_500,
            outage_min: 600,
            outage_max: 2_000,
            weight_partition: 0,
            weight_asymmetric: 1,
            weight_isolate: 0,
            weight_crash: 0,
            weight_pause: 0,
        }
    }

    /// Nothing but node death and revival. No network faults at all, so any
    /// divergence it produces is attributable to recovery alone.
    pub fn crash_only() -> Self {
        FaultConfig {
            enabled: true,
            healthy_min: 500,
            healthy_max: 2_000,
            outage_min: 300,
            outage_max: 1_500,
            weight_partition: 0,
            weight_asymmetric: 0,
            weight_isolate: 0,
            weight_crash: 4,
            weight_pause: 1,
        }
    }

    fn total_weight(&self) -> u32 {
        self.weight_partition
            + self.weight_asymmetric
            + self.weight_isolate
            + self.weight_crash
            + self.weight_pause
    }

    /// How long to stay connected before the next disturbance.
    pub fn healthy_span(&self, rng: &mut Rng) -> Tick {
        sample(self.healthy_min, self.healthy_max, rng)
    }

    /// How long a disturbance lasts.
    pub fn outage_span(&self, rng: &mut Rng) -> Tick {
        sample(self.outage_min, self.outage_max, rng)
    }

    /// Pick the next disturbance.
    ///
    /// `None` when this config is disabled, when every weight is zero, or when
    /// there are too few nodes to disturb. Checking `enabled` here as well as
    /// at the call site is deliberate: a disabled config that still hands out
    /// faults is exactly the kind of footgun that makes a "clean" control run
    /// quietly not be one.
    pub fn next_fault(&self, nodes: &[NodeId], rng: &mut Rng) -> Option<Fault> {
        let total = self.total_weight();
        if !self.enabled || total == 0 || nodes.len() < 2 {
            return None;
        }
        let roll = rng.gen_range(0, total as u64) as u32;
        if roll < self.weight_partition {
            let (a, b) = random_split(nodes, rng);
            return Some(Fault::Partition { a, b });
        }
        let roll = roll - self.weight_partition;
        if roll < self.weight_asymmetric {
            let (from, to) = random_pair(nodes, rng);
            return Some(Fault::AsymmetricCut { from, to });
        }
        let roll = roll - self.weight_asymmetric;
        if roll < self.weight_isolate {
            let node = nodes[rng.gen_range(0, nodes.len() as u64) as usize];
            return Some(Fault::Isolate { node });
        }
        let roll = roll - self.weight_isolate;
        let node = nodes[rng.gen_range(0, nodes.len() as u64) as usize];
        if roll < self.weight_crash {
            return Some(Fault::Crash { node });
        }
        let ticks = self.outage_span(rng);
        Some(Fault::Pause { node, ticks })
    }
}

fn sample(min: Tick, max: Tick, rng: &mut Rng) -> Tick {
    if max > min {
        rng.gen_range(min, max)
    } else {
        min
    }
}

/// Assign each node to one side or the other, keeping both sides non-empty.
fn random_split(nodes: &[NodeId], rng: &mut Rng) -> (Vec<NodeId>, Vec<NodeId>) {
    // Bounded rather than `loop`: the retry only fails when every node lands on
    // the same side, which is rare, but an unbounded loop in code that must be
    // deterministic is a bad habit.
    for _ in 0..8 {
        let mut a = Vec::new();
        let mut b = Vec::new();
        for id in nodes {
            if rng.chance(1, 2) {
                a.push(*id);
            } else {
                b.push(*id);
            }
        }
        if !a.is_empty() && !b.is_empty() {
            return (a, b);
        }
    }
    let mid = nodes.len() / 2;
    (nodes[..mid.max(1)].to_vec(), nodes[mid.max(1)..].to_vec())
}

/// Two distinct nodes, in order.
fn random_pair(nodes: &[NodeId], rng: &mut Rng) -> (NodeId, NodeId) {
    let n = nodes.len() as u64;
    let i = rng.gen_range(0, n) as usize;
    let mut j = rng.gen_range(0, n - 1) as usize;
    if j >= i {
        j += 1;
    }
    (nodes[i], nodes[j])
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODES: [NodeId; 5] = [0, 1, 2, 3, 4];

    #[test]
    fn a_healed_network_reaches_everywhere() {
        let p = Partitions::new();
        for a in NODES {
            for b in NODES {
                assert!(p.reachable(a, b));
            }
        }
    }

    #[test]
    fn a_split_blocks_both_directions() {
        let mut p = Partitions::new();
        p.split(&[0, 1], &[2, 3, 4]);
        assert!(!p.reachable(0, 2));
        assert!(!p.reachable(2, 0));
        assert!(p.reachable(0, 1), "within a group traffic still flows");
        assert!(p.reachable(2, 3));
    }

    #[test]
    fn an_asymmetric_cut_blocks_only_one_direction() {
        let mut p = Partitions::new();
        p.cut(0, 1);
        assert!(!p.reachable(0, 1));
        assert!(p.reachable(1, 0), "the reverse direction must survive");
    }

    #[test]
    fn isolation_cuts_a_node_off_entirely() {
        let mut p = Partitions::new();
        p.isolate(2, &NODES);
        for other in NODES.iter().filter(|n| **n != 2) {
            assert!(!p.reachable(2, *other));
            assert!(!p.reachable(*other, 2));
        }
        assert!(p.reachable(0, 1), "other links are untouched");
        assert!(p.reachable(2, 2), "a node can always reach itself");
    }

    #[test]
    fn healing_restores_everything() {
        let mut p = Partitions::new();
        p.split(&[0], &[1, 2, 3, 4]);
        p.cut(3, 4);
        assert!(!p.is_healed());
        p.heal();
        assert!(p.is_healed());
        assert!(p.reachable(0, 1) && p.reachable(3, 4));
    }

    #[test]
    fn generated_splits_never_leave_a_side_empty() {
        let mut rng = Rng::new(7);
        for _ in 0..2_000 {
            let (a, b) = random_split(&NODES, &mut rng);
            assert!(!a.is_empty() && !b.is_empty());
            assert_eq!(a.len() + b.len(), NODES.len());
        }
    }

    #[test]
    fn generated_pairs_are_distinct_and_cover_every_ordered_pair() {
        let mut rng = Rng::new(7);
        let mut seen = BTreeSet::new();
        for _ in 0..5_000 {
            let (from, to) = random_pair(&NODES, &mut rng);
            assert_ne!(from, to);
            seen.insert((from, to));
        }
        assert_eq!(seen.len(), 20, "all 5x4 ordered pairs should appear");
    }

    #[test]
    fn schedules_are_reproducible() {
        let generate = || {
            let cfg = FaultConfig::aggressive();
            let mut rng = Rng::new(42);
            (0..200)
                .map(|_| (cfg.healthy_span(&mut rng), cfg.next_fault(&NODES, &mut rng)))
                .collect::<Vec<_>>()
        };
        assert_eq!(generate(), generate());
    }

    #[test]
    fn asymmetric_only_produces_only_one_way_cuts() {
        let cfg = FaultConfig::asymmetric_only();
        let mut rng = Rng::new(3);
        for _ in 0..200 {
            match cfg.next_fault(&NODES, &mut rng) {
                Some(Fault::AsymmetricCut { .. }) => {}
                other => panic!("unexpected: {other:?}"),
            }
        }
    }

    #[test]
    fn a_disabled_schedule_produces_nothing() {
        let cfg = FaultConfig::none();
        let mut rng = Rng::new(1);
        assert_eq!(cfg.next_fault(&NODES, &mut rng), None);
    }
}
