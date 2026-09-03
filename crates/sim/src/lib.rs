//! Deterministic simulation of a Raft cluster.
//!
//! The simulator owns everything the Raft nodes are not allowed to own: the
//! clock, the network, the disks, and all randomness. A run is a pure function
//! of its [`SimConfig`], so a seed is a complete, shareable description of
//! everything that happened.

pub mod cluster;
pub mod event;
pub mod faults;
pub mod history;
pub mod invariants;
pub mod linearizability;
pub mod network;
pub mod storage;
pub mod timeline;
pub mod trace;
pub mod workload;

pub use cluster::{
    AppliedEntry, ClientOutcome, Cluster, NodeStatus, OutcomeKind, RunStats, SimConfig,
};
pub use event::{Event, EventQueue, Seq};
pub use faults::{DiskConfig, Fault, FaultConfig, Partitions};
pub use history::{History, Operation, Outcome};
pub use invariants::{Invariant, Invariants, Violation};
pub use linearizability::{check, Verdict};
pub use network::{LatencyModel, Network, NetworkConfig, NetworkStats, Routed};
pub use storage::Storage;
pub use timeline::{Moment, Timeline};
pub use trace::{Trace, TraceKind, TraceRecord};
pub use workload::{ClientDriver, Request, Workload, WorkloadConfig};
