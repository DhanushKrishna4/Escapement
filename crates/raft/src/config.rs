//! Cluster membership, including joint consensus (§6).
//!
//! A configuration is either a plain voter set, or a *joint* configuration
//! holding both the old and the new set at once. The joint form is the whole
//! point of §6: while it is in force, **every decision needs a majority of both
//! sets independently**. That is what makes it impossible for C_old and C_new
//! to elect different leaders during the changeover, which is exactly the
//! failure the naive "just swap the set" approach allows.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// The set (or sets) of nodes that may vote and whose acknowledgements count
/// toward commitment.
///
/// `BTreeSet`, not `HashSet`: iteration order feeds message send order, which
/// feeds the event queue, which decides the entire run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// The current voters. During a joint configuration this is C_old.
    old: BTreeSet<NodeId>,
    /// C_new, present only while a change is in flight.
    new: Option<BTreeSet<NodeId>>,
}

impl ClusterConfig {
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Self {
        let old: BTreeSet<NodeId> = voters.into_iter().collect();
        assert!(!old.is_empty(), "a cluster needs at least one voter");
        ClusterConfig { old, new: None }
    }

    /// C_old,new: the transitional configuration.
    pub fn joint(
        old: impl IntoIterator<Item = NodeId>,
        new: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        let old: BTreeSet<NodeId> = old.into_iter().collect();
        let new: BTreeSet<NodeId> = new.into_iter().collect();
        assert!(
            !old.is_empty() && !new.is_empty(),
            "neither half may be empty"
        );
        ClusterConfig {
            old,
            new: Some(new),
        }
    }

    pub fn is_joint(&self) -> bool {
        self.new.is_some()
    }

    /// The outgoing set. Equal to the whole configuration when not joint.
    pub fn old_voters(&self) -> &BTreeSet<NodeId> {
        &self.old
    }

    /// The incoming set, if a change is in flight.
    pub fn new_voters(&self) -> Option<&BTreeSet<NodeId>> {
        self.new.as_ref()
    }

    /// The configuration this one is transitioning to, as a plain config.
    pub fn to_new(&self) -> Option<ClusterConfig> {
        self.new.as_ref().map(|n| ClusterConfig {
            old: n.clone(),
            new: None,
        })
    }

    /// Everyone in either set.
    ///
    /// A node joining in C_new votes and is replicated to from the moment the
    /// joint entry is appended, and a node leaving keeps voting until C_new
    /// commits. Both are required: during the changeover the two sets are
    /// jointly responsible for every decision.
    pub fn voters(&self) -> BTreeSet<NodeId> {
        match &self.new {
            None => self.old.clone(),
            Some(new) => self.old.union(new).copied().collect(),
        }
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.old.contains(&id) || self.new.as_ref().is_some_and(|n| n.contains(&id))
    }

    pub fn len(&self) -> usize {
        self.voters().len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    /// Does the set of nodes satisfying `has` form a quorum?
    ///
    /// THE RULE THAT MAKES §6 SAFE. A joint configuration needs a majority of
    /// C_old *and* a majority of C_new, checked independently. Requiring only a
    /// majority of the union — or of either one — would let the two halves
    /// choose different leaders in the same term while both believed they had
    /// won.
    ///
    /// Nodes outside the configuration are ignored rather than counted, which
    /// is what stops a removed server from helping elect anyone.
    pub fn is_quorum_by(&self, has: impl Fn(NodeId) -> bool) -> bool {
        fn majority(set: &BTreeSet<NodeId>, has: &impl Fn(NodeId) -> bool) -> bool {
            set.iter().filter(|id| has(**id)).count() * 2 > set.len()
        }
        majority(&self.old, &has) && self.new.as_ref().is_none_or(|n| majority(n, &has))
    }

    /// Does `granted` contain a quorum?
    pub fn is_quorum(&self, granted: &BTreeSet<NodeId>) -> bool {
        self.is_quorum_by(|id| granted.contains(&id))
    }

    /// A compact description for traces and reports.
    pub fn describe(&self) -> String {
        match &self.new {
            None => format!("{:?}", self.old),
            Some(new) => format!("{:?} -> {:?}", self.old, new),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn simple_quorums() {
        let cfg = ClusterConfig::new([0, 1, 2]);
        assert!(!cfg.is_quorum(&set(&[0])));
        assert!(cfg.is_quorum(&set(&[0, 1])));
        assert!(cfg.is_quorum(&set(&[0, 1, 2])));
    }

    #[test]
    fn a_single_node_is_its_own_majority() {
        assert!(ClusterConfig::new([0]).is_quorum(&set(&[0])));
    }

    #[test]
    fn votes_from_outside_the_configuration_do_not_count() {
        let cfg = ClusterConfig::new([0, 1, 2]);
        assert!(!cfg.is_quorum(&set(&[0, 7, 8])));
    }

    /// The heart of §6: a joint configuration needs both halves, separately.
    #[test]
    fn a_joint_configuration_needs_a_majority_of_each_half() {
        // Moving from {0,1,2} to {2,3,4}: the halves overlap only at node 2.
        let cfg = ClusterConfig::joint([0, 1, 2], [2, 3, 4]);

        // A majority of C_old alone is not enough.
        assert!(!cfg.is_quorum(&set(&[0, 1])));
        // Nor a majority of C_new alone.
        assert!(!cfg.is_quorum(&set(&[3, 4])));
        // Both halves, separately, is what works.
        assert!(cfg.is_quorum(&set(&[0, 1, 3, 4])));
        assert!(cfg.is_quorum(&set(&[0, 2, 3])));
    }

    #[test]
    fn a_lopsided_union_majority_is_not_a_joint_quorum() {
        // Growing {0,1,2} to {0,1,2,3,4,5,6}. Five of the nine distinct voters
        // is a union majority, but if they are all from C_old it is only 3 of 7
        // in C_new.
        let cfg = ClusterConfig::joint([0, 1, 2], [0, 1, 2, 3, 4, 5, 6]);
        assert!(
            !cfg.is_quorum(&set(&[0, 1, 2])),
            "3 of 7 in C_new is not a majority"
        );
        assert!(cfg.is_quorum(&set(&[0, 1, 2, 3, 4])));
    }

    #[test]
    fn a_joint_configuration_spans_both_halves() {
        let cfg = ClusterConfig::joint([0, 1, 2], [2, 3, 4]);
        assert_eq!(cfg.voters(), set(&[0, 1, 2, 3, 4]));
        // A node joining votes from the moment the joint entry is appended.
        assert!(cfg.contains(4));
        // And a node leaving keeps voting until C_new commits.
        assert!(cfg.contains(0));
        assert!(!cfg.contains(9));
    }

    #[test]
    fn transitioning_drops_the_old_half() {
        let cfg = ClusterConfig::joint([0, 1, 2], [2, 3, 4]);
        let new = cfg.to_new().expect("joint configurations transition");
        assert!(!new.is_joint());
        assert_eq!(new.voters(), set(&[2, 3, 4]));
        assert!(!new.contains(0), "the departing node is gone");
    }

    #[test]
    fn a_simple_configuration_has_nothing_to_transition_to() {
        assert!(ClusterConfig::new([0, 1, 2]).to_new().is_none());
    }

    #[test]
    fn disjoint_halves_still_need_both() {
        // The worst case for the naive approach: no overlap at all.
        let cfg = ClusterConfig::joint([0, 1, 2], [3, 4, 5]);
        assert!(!cfg.is_quorum(&set(&[0, 1, 2])));
        assert!(!cfg.is_quorum(&set(&[3, 4, 5])));
        assert!(cfg.is_quorum(&set(&[0, 1, 3, 4])));
    }
}
