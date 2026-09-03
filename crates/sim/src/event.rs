//! The event queue.
//!
//! Ordering here *is* the run. Two events at the same tick must be processed in
//! a defined order or the same seed produces different results, so every event
//! carries a globally monotonic sequence number and the queue is keyed by
//! `(tick, seq)` -- a total order, with no reliance on insertion order,
//! iteration order, or a hasher.

use std::collections::BTreeMap;

use raft::{ClientRequest, NodeId, RaftMessage, Tick};
use serde::{Deserialize, Serialize};

/// Monotonic tie-breaker for events scheduled at the same tick.
pub type Seq = u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    /// A message finished traversing the network.
    Deliver {
        from: NodeId,
        to: NodeId,
        msg: RaftMessage,
        /// When it was handed to the network.
        ///
        /// Carried purely so the visualizer can draw a message part-way along
        /// its wire: with the send and arrival ticks, its position at any
        /// moment is a interpolation. Nothing in the simulator reads it, and
        /// the queue is ordered by `(tick, seq)` regardless.
        sent_at: Tick,
    },
    /// A node's timer may have expired. Delivered as `Input::Tick`; a no-op if
    /// no deadline has actually been reached.
    Timer { node: NodeId },
    /// A client submitted a request to a node.
    Client { node: NodeId, req: ClientRequest },
    /// The fault schedule disturbs or repairs the network.
    Fault { fault: crate::faults::Fault },
    /// Time for this node to snapshot its state machine and drop the log
    /// prefix.
    Compact { node: NodeId },
}

#[derive(Clone, Debug, Default)]
pub struct EventQueue {
    /// `BTreeMap`, so `pop_first` gives the smallest `(tick, seq)` and the
    /// order is total and reproducible.
    ///
    /// A binary heap would be faster; a BTreeMap is used because the visualizer
    /// wants to look at pending events in time order without draining them, and
    /// because range queries make "everything scheduled in this tick" cheap.
    /// Both structures give the same total order, so this is a performance
    /// choice, not a correctness one.
    events: BTreeMap<(Tick, Seq), Event>,
    next_seq: Seq,
}

impl EventQueue {
    pub fn new() -> Self {
        EventQueue::default()
    }

    /// Schedule `event` at `tick`. Returns the sequence number assigned.
    pub fn push(&mut self, tick: Tick, event: Event) -> Seq {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.events.insert((tick, seq), event);
        seq
    }

    pub fn pop_next(&mut self) -> Option<((Tick, Seq), Event)> {
        self.events.pop_first()
    }

    pub fn next_tick(&self) -> Option<Tick> {
        self.events.keys().next().map(|(t, _)| *t)
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Pending events in processing order. For the timeline view.
    pub fn iter(&self) -> impl Iterator<Item = (&(Tick, Seq), &Event)> {
        self.events.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timer(node: NodeId) -> Event {
        Event::Timer { node }
    }

    #[test]
    fn pops_in_tick_then_sequence_order() {
        let mut q = EventQueue::new();
        q.push(10, timer(2));
        q.push(5, timer(0));
        q.push(10, timer(1));
        let order: Vec<Event> = std::iter::from_fn(|| q.pop_next().map(|(_, e)| e)).collect();
        assert_eq!(order, vec![timer(0), timer(2), timer(1)]);
    }

    #[test]
    fn same_tick_same_node_keeps_both_events() {
        let mut q = EventQueue::new();
        q.push(1, timer(0));
        q.push(1, timer(0));
        assert_eq!(
            q.len(),
            2,
            "sequence numbers must keep identical events distinct"
        );
    }
}
