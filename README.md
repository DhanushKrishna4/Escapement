# Escapement

A Raft consensus implementation in Rust, tested with deterministic simulation.

> **es·cape·ment** *noun* — the part of a clock that converts continuous force
> into discrete, countable ticks; the reason a clock ticks rather than simply
> unwinding. This one does it to a distributed system.

**[Open the visualizer](https://dhanushkrishna4.github.io/Escapement/)**
&nbsp;·&nbsp;
**[What the fuzzer found](https://dhanushkrishna4.github.io/Escapement/results.html)**

The Raft implementation is table stakes. The deterministic simulator and the
linearizability checker are the project.

## The idea

A Raft node is a pure state machine:

```rust
fn step(&mut self, input: Input, now: Tick) -> Vec<Output>
```

It never does I/O, never reads a clock, never generates randomness. It receives
an input and returns a list of things it *wants* done — send this message,
persist this state, apply this entry. The simulator owns the clock, the network
and the disks, so a whole run is a pure function of its seed: reproducible,
replayable, and shareable as a single number.

## Status

Complete. All 16 build-order steps are done, and the visualizer and fuzz
results are deployed.

- [x] 1. Raft message types, log, node skeleton with `step()`
- [x] 2. Simulator: virtual clock, event queue, perfect network — a 3-node
      cluster elects a leader and replicates entries
- [x] 3. Determinism test
- [x] 4. Invariant checkers running every tick, validated against deliberate bugs
- [x] 5. Network faults: drop, delay, reorder, duplicate
- [x] 6. Partitions, including asymmetric
- [x] 7. Crash/restart with persistence, pauses, clock skew
- [x] 8. Fuzz harness + repro minimization
- [x] 9. History recording + linearizability checker
- [x] 10. Snapshots and log compaction
- [x] 11. Joint-consensus membership changes
- [x] 12. ReadIndex + client session dedup
- [x] 13. WASM boundary + cluster rendering
- [x] 14. Full visualization: logs, timeline, partitions
- [x] 15. Fuzz results page + bug writeups
- [x] 16. GitHub Pages deploy

## Layout

```
crates/raft/      pure state machine. no I/O, no time, no randomness.
  node.rs         step(), role transitions, the safety rules
  log.rs          log storage, matching, truncation
  message.rs      RPC types
  config.rs       cluster membership, including joint consensus
  rand.rs         SplitMix64, the only source of randomness
  snapshot.rs     snapshots and the compaction boundary
crates/kvstore/   the replicated state machine
crates/fuzz/      runs N seeds, minimizes any failure
crates/wasm/      thin wasm-bindgen boundary
web/              Vite + TypeScript cluster view
crates/sim/       the simulator
  cluster.rs      nodes, virtual clock, main loop
  invariants.rs   the safety properties, checked every event
  event.rs        BTreeMap<(Tick, Seq), Event>
  network.rs      latency models, drops, duplication, reordering
  faults.rs       partitions, crashes, pauses, fault schedules
  storage.rs      simulated per-node disk
  trace.rs        run traces, for the determinism test
  workload.rs     client request generators
  history.rs      what clients asked for, and what they were told
  linearizability.rs   the checker
docs/JOURNAL.md   dev journal
```

## Running

```bash
cargo test
```

```bash
cargo run --release --example throughput -p sim
```

### The browser view

```bash
./tools/build-web.sh && cd web && npm run dev
```

**The cluster view.** Nodes in a ring, coloured by role; messages are dots
placed along their wire by how much of their flight has elapsed, which makes the
animation a property of the simulated clock rather than the frame rate.
Partitions are dashed links, with one-way cuts in a different shade from full
splits — they behave very differently, and it helps to see which is on screen.

**The log view**, which is the one worth building the rest for. A strip per
node, a cell per entry, coloured by term, filled once committed and outlined
while not. Every strip shares one index range, so a column means the same log
position on every node — during a partition you watch the rows drift apart, and
when it heals you watch the losing side get overwritten.

**Time travel by replay.** Click any event on the timeline and the simulator
rebuilds from the seed and re-runs to that tick. No snapshot history, no undo
log: a run is a pure function of its configuration plus whatever you did to it,
and both are recorded.

**Direct manipulation.** Pick a tool — partition, one-way cut, crash/restart,
pause — and click nodes, or drag a box around a group to split it off.

The configuration lives in the URL, so a link reproduces a run exactly:

```
?seed=7&nodes=7&net=long_tail&faults=aggressive&load=1&speed=20
```

## What is enforced, and where

Section references are to Ongaro & Ousterhout, *In Search of an Understandable
Consensus Algorithm* (extended version). Every rule that carries a safety
property is commented with why it exists.

| Rule | Where |
|---|---|
| Election restriction, term before index (§5.4.1) | `LogId::at_least_as_up_to_date_as` |
| One vote per term, persisted before the reply (§5.2) | `RaftNode::on_request_vote` |
| Log Matching consistency check (§5.3) | `RaftNode::on_append_entries` |
| Truncate only at a genuine conflict (§5.3) | `RaftNode::on_append_entries` |
| Conflict-hint backoff, skip a term per round trip (§5.3) | `RaftNode::on_append_entries_resp` |
| Commit only current-term entries (§5.4.2) | `RaftNode::maybe_advance_commit` |
| New-leader no-op (§5.4.2) | `RaftNode::become_leader` |
| Persist before send | `Outputs::finish` |
| Joint quorum needs BOTH halves (§6) | `ClusterConfig::is_quorum_by` |
| Config applies on append, not commit (§6) | `RaftNode::adopt_configs` |
| Ignore vote requests near a live leader (§6) | `RaftNode::ignores_vote_requests` |
| ReadIndex: confirm leadership before serving (§6.4) | `RaftNode::resolve_reads` |
| Session dedup lives in the state machine (§8) | `KvStore::apply_for` |
| Compaction keeps the boundary answerable (§7) | `Log::term_at` / `Log::compact` |
| Snapshot and log reconciled on recovery (§7) | `Log::reconcile_with_snapshot` |

## Network faults

Four presets — `perfect`, `flaky`, `long_tail`, `hostile` — combining a latency
model (fixed, uniform, or long-tailed) with drop, duplication and explicit
reordering, all as integer per-mille probabilities. A knob left at zero draws no
randomness at all, so turning one fault on does not shift every other draw:
"same seed, one knob changed" stays a meaningful comparison.

## Partitions

Reachability is a set of **directed** links, so asymmetric partitions — A cannot
reach B, but B can still reach A — cost nothing extra. Faults can be injected
manually (`partition`, `cut`, `isolate`, `heal`) or driven by a schedule
generated from the seed, where each disturbance schedules its own repair.

### What the faults actually reach

24 seeds, 5 nodes, 30,000 ticks each. Asserted by tests so it cannot silently
regress:

| schedule | truncations | elections | max term |
|---|---|---|---|
| none | 0 | 33 | 1 |
| occasional | 63 | 472 | 55 |
| aggressive | 93 | 859 | 95 |
| asymmetric_only | **0** | 213 | 17 |
| aggressive + long_tail | **128** | 887 | 83 |

**The two fault classes attack different properties.** One-way cuts never give a
node a private log to grow, so nothing diverges — they make a node deaf, it
campaigns forever, and the term inflates. That is an attack on *liveness* with
every log left consistent. Symmetric splits do the opposite: an isolated leader
appends entries that can never commit, which is what causes divergence and
truncation. A fuzzer needs both.

Message loss on its own reaches neither: at a few per cent loss a follower never
misses enough consecutive heartbeats to time out, so leadership never changes.

## Crash and recovery

A crash throws away everything volatile — the role, `commitIndex`, `lastApplied`,
and the state machine — leaving only what reached the disk. The node comes back
and replays its log into an empty store. Persist too little and recovery loses
something; keep anything extra and the test passes for the wrong reason.

`commitIndex` legitimately restarts at 0 (Figure 2 makes it volatile), so the
checker is told about restarts and clears exactly that watermark and
`lastApplied` — and deliberately clears nothing else. `currentTerm` and
`votedFor` are persistent, so a node that comes back in a lower term is a real
bug, which is what `skip_hard_state_persistence` demonstrates.

Nodes also run on independently skewed clocks, and can be paused (a GC-pause
model that holds their messages rather than dropping them).

### The three fault classes target different things

| fault | attacks | evidence |
|---|---|---|
| symmetric partition | log consistency | 37–93 truncations |
| asymmetric cut | liveness (term inflation) | 213 elections, 0 truncations |
| crash / restart | availability and recovery | 260 restarts, ~0 truncations |

Crashes barely diverge logs, and that is correct: a leader appends and
broadcasts in the same step, so by the time it can crash the messages are
already in the network. Divergence needs the append to survive while the send
does not — which is what a partition provides.

### Two places Raft loses liveness on purpose

Both are pinned down as tests rather than papered over. Safety holds in both.

- **The zombie leader** — a leader that can send but not receive keeps followers
  quiet with heartbeats, but never sees an acknowledgement, so nothing commits.
  The cluster looks healthy and is stuck. This is what CheckQuorum fixes.
- **The deaf follower** — a node that cannot hear the leader campaigns forever,
  and every RequestVote deposes a healthy leader. This is what Pre-Vote fixes.

## Membership changes

Joint consensus (§6), not the one-server-at-a-time shortcut. A change goes
C_old -> C_old,new -> C_new, and while joint **every decision needs a majority of
both sets independently** — one function, `ClusterConfig::is_quorum_by`, which
both vote counting and commit counting go through.

Configurations take effect when their entry is **appended**, not when it
commits, and are un-applied if that entry is later truncated away. A leader
outside C_new keeps leading until C_new commits — it is the one replicating the
entry that removes it — then steps down. Snapshots carry the configuration, or a
recovering node would resurrect a membership the cluster had left behind.

## Client guarantees

**Linearizable reads via ReadIndex (§6.4).** A read never enters the log. The
leader records the commit index, confirms it is still the leader by hearing back
from a quorum on a fresh heartbeat round, waits for its state machine to reach
that index, and only then answers. The round carries an id that the
acknowledgement echoes, so a delayed reply from an older round cannot be
mistaken for confirmation.

**Deduplication (§8) lives in the state machine**, not on the leader — so it is
replicated, and a retry that lands on a *different* leader is still recognised.
A leader-local table would forget everything exactly when clients retry. §8
assumes one outstanding request per client; concurrency comes from having
several clients.

## Safety checking

Ten properties are checked after every single event, not at the end. They are
cheap — O(entries changed) per event, about 19% of runtime — and they say which
property broke and why:

```
[tick 201] Single Vote Per Term (§5.2) violated: node 4 voted for 4 and then for 2 in term 1
    likely cause: votedFor was not checked, or was lost across a term change
```

| Property | Paper |
|---|---|
| Election Safety — at most one leader per term | §5.2 |
| Log Matching — same (index, term) implies same history | §5.3 |
| Leader Completeness — a new leader holds every committed entry | §5.4 |
| State Machine Safety — no two nodes apply differently at one index | §5.4.3 |
| Committed Entries Are Stable — a committed entry is never replaced | §5.3 |
| Applied In Order, Single Vote Per Term, Term Monotonic, Commit Index Monotonic, Commit Index Within Log | Figure 2 |

### Validating the checkers

A checker that always says "OK" passes every test until it matters, so
`RaftConfig::bugs` carries three deliberate, off-by-default bugs and
`tests/checker_validation.rs` confirms each property fires:

- `commit_prior_term_entries` — commit by replica count regardless of term
  (§5.4.2, the Figure 8 bug)
- `vote_twice_per_term` — ignore `votedFor` (§5.2)
- `trust_leader_commit_blindly` — take `leaderCommit` uncapped (§5.3)

The Figure 8 test is the interesting one: it runs the *same* event sequence
twice. With §5.4.2 enforced, a later leader overwrites the entry and nothing is
wrong, because nothing was committed there. With the rule broken, the identical
takeover destroys a committed entry and the checker fires.

## Fuzzing

```bash
cargo run --release -p fuzz -- --seeds 100000
```

100,000 seeds in 58 seconds on 8 threads. One thread per core pulling seeds off
an atomic counter — safe precisely because a seed's run is a pure function of its
config, which `parallelism_does_not_change_results` asserts.

Latest clean sweep:

| | |
|---|---|
| violations | **0** |
| elections | 2,726,106 |
| log truncations | 184,202 |
| crashes / restarts | 781,963 / 766,073 |
| torn steps | 102,233 |
| messages sent | 533,046,838 |
| dropped / partitioned | 33,353,990 / 25,979,913 |
| max term reached | 157 |
| client operations | 6,200,531 |
| never answered (pending) | 270,436 |
| histories linearizable | 50,000 of 50,000 |
| membership changes | 15,722 |
| snapshots taken / installed | 727,889 / 147,858 |

On failure it prints the seed, writes the full trace, and shrinks the fault
schedule by delta debugging — chunks first, then single faults — until every
remaining fault is load-bearing. On a planted §5.4.2 bug it cut 49 faults to 12.

To check the harness can actually find something:

```bash
cargo run --release -p fuzz -- --seeds 500 --bug double-vote
```

### Bugs it found in this implementation

Written up in full on the results page, with links that reproduce each one live
— the three real defects are kept as regression switches so the writeups can
show the failure rather than describe it.


Snapshots (step 10) were the first step where the fuzzer found real bugs rather
than planted ones. Both are written up in the journal:

1. **Index 0 stops being an anchor once you compact.** `term_at(0)` returned
   `Some(0)` as the before-the-log sentinel, so a leader probing at
   `prevLogIndex = 0` passed the consistency check against a follower that had
   compacted through index 8 — which then appended entries 1..7 onto a log
   beginning at 9 and silently corrupted itself. Minimized to **zero faults**:
   compaction alone reproduces it.
2. **The snapshot and the log are two durable artifacts that can disagree.** A
   torn write between them left recovery pairing a snapshot at index 120 with a
   log ending at 119. Reconciliation has to be written back to disk, not just
   applied in memory.

Both were only findable because `Log::append`'s contiguity check was promoted
from `debug_assert!` to a hard `assert!` — release builds skip debug asserts,
and release is where the fuzzer runs.

ReadIndex (step 12) produced three more, all caught by the linearizability
checker and none visible to the per-event invariants:

1. **The read index was captured too early** — a node that had just restarted
   answered a read with `read_index = 0` from an empty state machine, before
   replaying a single entry. The paper assigns the index only after the leader
   knows what is committed.
2. **Reads resolved before entries applied**, so `ServeRead` was emitted ahead
   of the `Apply`s it was waiting for.
3. **§8's session table assumes one request in flight per client.** With several,
   an old retry gets a newer command's cached answer — a `put` that returned
   `cas-failed`.

Membership changes (step 11) surfaced a *missing rule* rather than a bug:
a server removed by a change is left holding only the joint entry, which still
includes it, so it times out forever and every RequestVote it sends deposes a
healthy leader. §6 prescribes the fix — ignore vote requests received within the
minimum election timeout of hearing from a leader — and I had implemented joint
consensus while skipping the paragraph after it. Terms reached 178 and the
cluster never settled.

### Disk faults

The **torn step** is modelled: some prefix of a step's writes lands, then the
process dies before any message goes out. It is the sharpest test of the
persist-before-send contract — a node that could reply "I voted for you" and then
lose the record would come back free to vote twice in one term — and it is the
only fault that lets a crash diverge a log.

**Silent write loss is deliberately not modelled.** Raft's correctness assumes an
acknowledged write is durable; if fsync lies, the algorithm can genuinely violate
safety. That is a documented property of Raft, not a bug in this implementation,
and injecting it would give the fuzzer a noise floor.

## Linearizability

Given the recorded history of client operations, is there a sequential order —
consistent with real-time precedence — that a correct single-threaded store
could have produced?

Wing & Gong's search with Lowe's optimizations: per-key decomposition
(P-compositionality), linearize-earliest-candidate with backtracking, and
memoization on `(state, remaining set)`. When the budget runs out or a
sub-history exceeds the cap, the answer is `Unknown` with a reason — never a
cheerful "linearizable".

### Pending operations

The part that decides whether a checker is worth anything. An operation with no
response **may or may not have taken effect**:

| outcome | treatment |
|---|---|
| completed | must be linearized, with the observed result |
| rejected ("not leader") | never entered any log — dropped |
| pending **read** | observed nothing, constrains nothing — dropped |
| pending **write** | *optional*: may be linearized, or may never have happened |

Recording a pending operation as failed invents violations; recording it as
succeeded hides them. Both directions are tests.

### A bug no invariant can catch

The `stale_reads` switch lets a leader answer reads from its own state machine
with no ReadIndex round. A leader deposed behind a partition then serves stale
values:

```
seed 4 FAILED (3 nodes, 0 violation(s))
  no sequential ordering of the operations on key "k2" can explain what clients saw.
      [2647..2714] c2 put(k2, v14) = ok
      [4086..4127] c0 del(k2)      = ok
      [4454..4454] c1 get(k2)      = v14
```

Zero invariant violations — every Raft property holds, because the read never
enters the log. Only the client-visible history is wrong. That is what the
linearizability checker is for, and what ReadIndex will prevent.

## Determinism

Non-negotiable and enforced two ways:

- `crates/sim/tests/determinism.rs` runs seeds twice and compares the **full
  trace** — every input delivered and every output produced — byte for byte.
  Comparing final state instead would let a divergence that happens to
  reconverge slip through. It also runs two clusters interleaved in lockstep,
  which catches divergence caused by process-global state that a sequential
  comparison would miss, and asserts that different seeds actually produce
  different runs, so the test cannot pass by doing nothing.
- `crates/sim/tests/no_nondeterminism.rs` greps core source for
  `Instant::now`, `SystemTime`, `thread_rng`, `HashMap`, `HashSet`, floats,
  `async`, and `unsafe`. Every one of these compiles fine and only shows up as
  two runs quietly disagreeing, possibly thousands of events later.
