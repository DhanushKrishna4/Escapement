/** The shapes the wasm boundary sends over. Mirrors `crates/wasm/src/lib.rs`. */

export interface EntryView {
  index: number;
  term: number;
  kind: "noop" | "cmd" | "config";
  committed: boolean;
}

export interface NodeView {
  id: number;
  role: "follower" | "candidate" | "leader";
  status: "running" | "crashed" | "paused";
  term: number;
  commitIndex: number;
  lastApplied: number;
  lastIndex: number;
  logStart: number;
  votedFor: number | null;
  leaderId: number | null;
  snapshotIndex: number;
  config: string;
  isJoint: boolean;
  pendingReads: number;
  log: EntryView[];
}

export interface MessageView {
  from: number;
  to: number;
  kind:
    | "RequestVote"
    | "RequestVoteResp"
    | "AppendEntries"
    | "AppendEntriesResp"
    | "InstallSnapshot"
    | "InstallSnapshotResp";
  sentAt: number;
  arrivesAt: number;
}

export interface RunStats {
  electionsStarted: number;
  leadersElected: number;
  maxTerm: number;
  logTruncations: number;
  entriesTruncated: number;
  entriesApplied: number;
  clientResponses: number;
  faultsInjected: number;
  crashes: number;
  tornSteps: number;
  restarts: number;
  pauses: number;
  messagesDeferred: number;
  snapshotsTaken: number;
  snapshotsInstalled: number;
  membershipChanges: number;
  readsServed: number;
}

export interface StateView {
  tick: number;
  eventsProcessed: number;
  nodes: NodeView[];
  inFlight: MessageView[];
  blockedLinks: [number, number][];
  leaders: number[];
  stats: RunStats;
  violations: string[];
  kv: [string, string][];
}
