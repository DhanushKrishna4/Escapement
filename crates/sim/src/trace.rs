//! Run traces.
//!
//! The trace is the artifact the determinism test compares. It records every
//! input delivered to every node and every output that node produced, tagged
//! with the virtual time and the event sequence number. Two runs of the same
//! seed must produce byte-identical traces -- anything weaker (comparing final
//! state, say) would let a divergence that happens to reconverge slip through.

use kvstore::{KvCommand, KvResult};
use raft::{Index, Input, NodeId, Output, Term, Tick};
use serde::{Deserialize, Serialize};

use crate::event::Seq;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceKind {
    /// An input was delivered to a node.
    In(Input),
    /// A node produced an output.
    Out(Output),
    /// A message was created but the network swallowed it.
    Dropped { to: NodeId },
    /// The network delivered a copy of this message as well.
    Duplicated { to: NodeId },
    /// A message was blocked by a partition rather than randomly lost.
    Partitioned { to: NodeId },
    /// A message arrived for a node that was not there to receive it.
    LostToCrash { from: NodeId },
    /// A node replaced its state machine from a leader's snapshot.
    SnapshotInstalled { through: Index, bytes: usize },
    /// A node died. Everything not on its disk is gone.
    Crashed,
    /// A step was torn: some prefix of its writes landed and the process died
    /// before sending anything.
    TornStep,
    /// A node came back, rebuilt from its durable state alone.
    Restarted {
        current_term: Term,
        voted_for: Option<NodeId>,
        last_index: Index,
    },
    /// The network was disturbed or repaired.
    Fault { fault: crate::faults::Fault },
    /// A membership change was requested (§6).
    MembershipChange { voters: Vec<NodeId> },
    /// A read was answered through ReadIndex, without entering the log.
    ReadServed { read_index: Index, result: KvResult },
    /// An entry reached the replicated state machine.
    Applied {
        index: Index,
        term: Term,
        command: Option<KvCommand>,
        result: Option<KvResult>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRecord {
    pub tick: Tick,
    pub seq: Seq,
    pub node: NodeId,
    pub kind: TraceKind,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    records: Vec<TraceRecord>,
    /// Recording can be switched off.
    ///
    /// A 2M-event run produces ~3.4M records and about half a gigabyte, which
    /// is fine for one interactive run and impossible for a fuzzer sweeping
    /// tens of thousands of seeds. The fuzzer runs with it off and re-runs a
    /// failing seed with it on — which costs nothing, because the run is
    /// perfectly reproducible.
    enabled: bool,
}

impl Trace {
    pub fn new() -> Self {
        Trace {
            records: Vec::new(),
            enabled: true,
        }
    }

    pub fn disabled() -> Self {
        Trace {
            records: Vec::new(),
            enabled: false,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn push(&mut self, tick: Tick, seq: Seq, node: NodeId, kind: TraceKind) {
        if !self.enabled {
            return;
        }
        self.records.push(TraceRecord {
            tick,
            seq,
            node,
            kind,
        });
    }

    pub fn records(&self) -> &[TraceRecord] {
        &self.records
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.records).expect("trace is serializable")
    }

    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.records).expect("trace is serializable")
    }

    /// FNV-1a over the serialized trace.
    ///
    /// Cheap enough to compare thousands of fuzz runs; when it differs, the
    /// full JSON says exactly where. Not a security hash and does not need to
    /// be -- it only has to be a deterministic function of the bytes.
    pub fn digest(&self) -> u64 {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut hash = OFFSET;
        for byte in self.to_json().as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(PRIME);
        }
        hash
    }

    /// Index of the first record where two traces differ, if any.
    /// Turns "the runs diverged" into "the runs diverged here".
    pub fn first_difference(&self, other: &Trace) -> Option<usize> {
        for (i, (a, b)) in self.records.iter().zip(other.records.iter()).enumerate() {
            if a != b {
                return Some(i);
            }
        }
        if self.records.len() != other.records.len() {
            return Some(self.records.len().min(other.records.len()));
        }
        None
    }
}
