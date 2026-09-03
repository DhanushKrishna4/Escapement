# Dev journal

Kept from day one so the "bugs the simulator found in my own implementation"
writeup at the end has real material in it. One entry per working session.
Record decisions, surprises, and anything the fuzzer catches — with the seed.

---

## Entry 1 — Raft skeleton, simulator, determinism test (build order steps 1–3)

**Goal:** a 3-node cluster elects a leader and replicates entries on a perfect
network, and the same seed produces byte-identical traces.

**Status:** done. 70 tests pass. 5.7M events/sec on 3 nodes (release).

### What got built

- `crates/raft` — the node as a pure state machine. `step(input, now) -> Vec<Output>`.
  Elections with the §5.4.1 restriction, replication with the §5.3 conflict-hint
  optimization, the §5.4.2 commit rule, the new-leader no-op.
- `crates/kvstore` — the replicated state machine. Deliberately boring.
- `crates/sim` — virtual clock, `BTreeMap<(Tick, Seq), Event>` queue, perfect
  network, per-node simulated disk, full run trace.
- Determinism test comparing full traces, plus a source scan for the constructs
  that break determinism silently.

### Decisions made

**Node-owned PRNG, seeded by the simulator.** The spec says randomness comes
from the simulator, but `step(&mut self, input, now)` has nowhere to thread an
`&mut Rng`. Compromise: each node holds a `SplitMix64` seeded `Rng::derive(seed,
node_id)`, constructed by the simulator. The seed still fully determines the
run. If threading an explicit `&mut Rng` through `step` is preferred, the
signature change is small and localized to `reset_election_deadline`.

**Timers as scheduled events, not per-tick polling.** The simulator asks each
node for `next_deadline()` and schedules one `Timer` event there, rather than
delivering `Input::Tick` on every tick. Purely a performance decision —
`Input::Tick` is a no-op when no deadline has been reached, so a stale timer
changes nothing. Without this the loop would burn ~150 no-op ticks per election
timeout. Stale timers are bounded: a node's pending timer is tracked in
`timer_at` and only cleared when the timer that fires is the tracked one.

**Client identity lives in the log entry**, not in a leader-side pending map.
Means a new leader can still tell the simulator which request an applied entry
belongs to, and it makes the deliberate choice below expressible.

**A leader that steps down sends no failure to in-flight clients.** Its entries
may still commit under the next leader, so reporting failure would be a lie.
The client simply never hears back. That is the PENDING case the linearizability
checker has to consider both ways in step 9 — getting this wrong here would make
the checker unsound later, so it is worth the silence now.

### Things found while building

**Two hard-state writes per step.** A `RequestVote` arriving at a higher term
made the node persist `{term, votedFor: None}` (stepping down) and then
`{term, votedFor: Some(candidate)}` (granting) — two disk writes for one logical
change. Found by a test asserting persist-before-send ordering, which picked up
the *first* of the two and failed on its contents. Fixed by collecting hard
state into one slot per step, emitted first among persists. Ordering matters:
after a crash `currentTerm` must be at least the term of any entry on disk, or a
recovered node could hold an entry from a term it does not believe in and vote
in that term again.

**Two test failures that were the test's fault, worth writing down** because
both looked like bugs for a minute:

1. `run_until_leader` returns the instant a candidate wins, *before* its first
   AppendEntries has been delivered. A follower asked to redirect at that moment
   correctly answers `NotLeader { leader: None }` — it has genuinely never heard
   of this leader. Real behaviour, bad test timing.
2. A "heartbeat" right after an election is not empty: it carries the election
   no-op, because nobody has acknowledged it yet. Heartbeats only go empty once
   followers are caught up.

**A conflict hint I wrote by hand was impossible.** The test asserted that a
leader receiving `ConflictHint { term: Some(1), first_index: 1 }` would resume
after its own last term-1 entry — but the hint's term is by definition the term
of the follower's entry at `prevLogIndex`, which differs from the leader's entry
there. So the leader's last entry of that term is always strictly before
`prevLogIndex`, and the clamp that forces `nextIndex` backwards can never bind
for a well-formed hint. The clamp stays as defence against a malformed or
duplicated response; the test was rewritten around a hint a real follower could
actually send.

### Known gaps / follow-ups

- **Duplicated rejections cost an extra probe each.** The leader cannot tell a
  duplicated `AppendEntriesResp` rejection from a fresh one, so each one walks
  `nextIndex` back another step. Harmless today (the perfect network never
  duplicates) but should be fixed when duplication lands in step 5, by echoing
  the probed `prevLogIndex` in the response and ignoring rejections that do not
  match the outstanding probe. Noted in a comment at the site.
- **The trace grows without bound.** 2M events produced 3.4M trace records. Fine
  for interactive runs and single repros; the fuzzer (step 8) will need either a
  bounded ring buffer or tracing off by default with a re-run to capture.
- **No CheckQuorum.** A partitioned leader stays leader until it sees a higher
  term. That is the paper's behaviour and it is safe — it just cannot commit.
  Needed only once leases back ReadIndex in step 12.
- `Log` already carries `start_index` / `last_included` so that snapshotting in
  step 10 is a prefix drop rather than a rewrite. Nothing uses them yet.
- **Deliberate-bug drill not done yet.** The spec calls for writing the broken
  §5.4.2 commit rule once, on purpose, to confirm the checker fires. Do it when
  the invariant checkers land in step 4 — right now
  `a_leader_may_not_commit_a_prior_term_entry_by_counting_replicas` is the only
  thing standing guard, and a unit test is not a checker.

### Numbers

| | |
|---|---|
| Tests | 70 passing |
| Throughput, 3 nodes | 5.69M events/sec |
| Throughput, 5 nodes | 5.24M events/sec |
| Determinism | 24 seeds, byte-identical traces, plus interleaved lockstep |

---

## Entry 2 — Invariant checkers (build order step 4)

**Goal:** the safety properties checked continuously during a run, not at the
end, with reports that name the property and point at the cause.

**Status:** done. 87 tests pass. Checking costs ~19% throughput (5.00M vs 6.15M
events/sec on 3 nodes), which is cheap enough to leave on by default.

### What got built

- `crates/sim/invariants.rs` — ten properties, checked after every event:
  Election Safety, Log Matching, Leader Completeness, State Machine Safety,
  Committed Entries Are Stable, Commit Index Monotonic, Commit Index Within Log,
  Applied In Order, Single Vote Per Term, Term Monotonic.
- `RaftConfig::bugs` — three deliberate bugs, off by default, for validating the
  checkers.
- `crates/sim/tests/checker_validation.rs` — a scenario that trips each
  property, plus controls asserting a correct run stays clean.

Everything is incremental. The simulator already sees every log mutation as a
`PersistOp`, so it passes the lowest index touched as a `dirty_from` hint and
the Log Matching scan only looks at entries that actually changed. Cost per
event is O(entries changed); the O(committed) Leader Completeness scan runs once
per election, not per event.

### The finding that changed the design

**Core was panicking on exactly the conditions the checker exists to report.**
Two `debug_assert!(false, ...)` guards — "a leader received AppendEntries for
its own term" and "commitIndex ran past the end of the log" — fired the moment a
deliberate bug was switched on, aborting the run before any violation could be
reported.

That is backwards. When the implementation is broken, a fuzzer wants a violation
report and a minimized repro, not a process abort. Both sites now degrade
gracefully (the leader concedes; the apply loop stops) and the checker reports
the problem with the nodes, the term, and the likely cause. The `debug_assert`
in `send_append_entries` stays — that one is about compaction being unimplemented
(step 10), not about a safety property.

Worth remembering for later steps: an assertion in core is only appropriate for
"this code is being called wrong", never for "the algorithm is violating a
safety property". The latter belongs to the checker.

### Building Figure 8 was harder than expected, for a good reason

Reproducing the §5.4.2 bug needs a leader to commit a previous-term entry by
replica count. Two things got in the way, and both are the algorithm defending
itself:

1. **The no-op defeats it.** A leader always appends an entry of its own term on
   election, and `AppendEntries` sends a contiguous suffix — so the backfilled
   old entry and the leader's own-term entry arrive together, commit together,
   and the commit is legitimate. The scenario only works with
   `max_entries_per_append: 1`, which separates them. That is a nice
   demonstration of *why* the no-op exists.
2. **The crash has to be modelled precisely.** The leader's first message to
   each follower necessarily carries its own-term entry — that is where
   `nextIndex` points — and the short follower rejects it, which is how the
   leader learns to back up. So the own-term entry gets exactly one shot per
   follower and node 0 "crashes" before any retry. A cruder filter that just
   dropped every message carrying the own-term entry also blocked the rejection,
   and the backfill never started.

The payoff: `the_commit_rule_is_what_stops_a_committed_entry_being_overwritten`
and `breaking_the_commit_rule_trips_the_checker` run the *same* event sequence.
With §5.4.2 enforced, node 4's later takeover overwrites index 2 and nothing is
wrong, because nothing was committed there. With the rule broken, the identical
takeover destroys a committed entry and `CommittedEntriesStable` fires. That is
the difference the rule makes, made visible.

### Validation coverage, honestly stated

| Property | How it is validated |
|---|---|
| Election Safety | end to end: `vote_twice_per_term` on a perfect network, 200 seeds |
| Single Vote Per Term | end to end, same bug |
| Committed Entries Stable | end to end: `commit_prior_term_entries` in the Figure 8 scenario |
| Commit Index Within Log | end to end: `trust_leader_commit_blindly` |
| Log Matching (x2) | direct: two nodes fed conflicting entries |
| Leader Completeness | direct: a leader elected with an empty log |
| State Machine Safety | direct: divergent applies at one index |
| Applied In Order | direct: a gap in the applied sequence |
| Term / Commit Monotonic | direct: two states for one node id, out of order |

The last two are not reachable by a correct node at all — a node cannot un-learn
a term without a crash, and crashes arrive in step 7. Revisit then and make them
end to end.

### Notes for later

- The `vote_twice_per_term` bug trips Single Vote Per Term *before* Election
  Safety, which is right: the double vote is the cause, the second leader is the
  symptom. When the fuzzer reports, order violations by tick and lead with the
  earliest — it will usually be the cause.
- `Invariants::broken()` returns the distinct properties currently violated,
  which is what the verification panel's green/red lights will read.
- Leader Completeness is O(committed entries) per election. Fine now; if long
  fuzz runs make it hot, cap it to entries committed since the previous election.

### Numbers

| | |
|---|---|
| Tests | 87 passing (was 70) |
| Throughput, checks on | 5.00M events/sec (3 nodes), 4.42M (5 nodes) |
| Throughput, checks off | 6.15M events/sec (3 nodes), 5.23M (5 nodes) |
| Checker cost | ~19% |

---

## Entry 3 — Network faults (build order step 5)

**Goal:** drops, variable latency, duplication and reordering, all deterministic.

**Status:** done. 107 tests pass.

### What got built

- `LatencyModel`: `Fixed`, `Uniform`, and `LongTail` (mostly fast, occasionally
  20x worse). Integers throughout — probabilities are per mille, never floats,
  because floats are not guaranteed identical across platforms and these values
  decide behaviour.
- Drop, duplicate and explicit-reorder knobs. Explicit reordering makes a
  message overtake the furthest-out delivery already scheduled on its link;
  variable latency reorders by chance, this forces the case.
- Presets: `perfect`, `flaky`, `long_tail`, `hostile`.
- `NetworkStats` and `RunStats` coverage counters — elections, truncations,
  entries applied, drops, duplicates, reorders, max delay.

**A knob at zero draws no randomness.** Each fault check is guarded by its
per-mille value being non-zero, so turning one fault on does not shift every
other draw. That makes "same seed, one knob changed" a meaningful comparison
instead of an unrelated universe, and it is asserted by
`a_disabled_fault_changes_nothing`.

### Step 3's follow-up, now done

Duplicated rejections used to walk `nextIndex` back once per copy, turning the
conflict-hint optimization into a crawl the moment the network duplicates.
`AppendEntriesResp` now echoes the `prevLogIndex` it is answering, and the
leader ignores any rejection that does not match its outstanding probe. Tested
by `a_duplicated_rejection_does_not_move_next_index_twice`.

### The finding that matters: these faults do not reach very far

Coverage counters over 24 seeds x 12,000 ticks, 5 nodes:

| preset | elections | truncations | entries truncated | max term |
|---|---|---|---|---|
| perfect | 33 | 0 | 0 | 1 |
| flaky | 42 | **0** | 0 | 3 |
| long_tail | 36 | **0** | 0 | 2 |
| hostile | 297 | 10 | 10 | 20 |

**Message loss alone does not produce log divergence.** A follower needs several
consecutive heartbeats to go missing before it times out — at 5% loss that is
vanishingly rare — so leadership never changes, and with one leader there is
nothing to diverge from. Only `hostile` (25% loss) churns hard enough to
truncate anything, and even then only ten single-entry truncations.

I also tried a "sticky client" workload that keeps aiming at a stale leader
hint, on the theory that an isolated leader would keep appending entries nobody
accepts. It changed almost nothing (11 truncations instead of 10), for the same
reason: without a partition the old leader does not stay isolated long enough.

So **"24 seeds x 4 presets, no violations" is weaker evidence than it looks**,
and the suite now says so out loud: `how_much_of_raft_these_faults_actually_reach`
asserts both that the mild presets do *not* truncate (a real property — a 5%-loss
network should not destabilise leadership) and that `hostile` does. If either
changes, the test fails and the accounting gets revisited.

The truncation path is covered precisely by the handcrafted Figure 8 scenario.
Covering it *randomly* needs partitions (step 6) and crashes (step 7), where an
isolated leader keeps appending entries a later leader has never seen. That is
where the fault testing gets real.

### A performance regression that was not one

Throughput appeared to drop from 5.0M to 1.0M events/sec. I bisected the new
per-message `BTreeMap` bookkeeping, the coverage counters, and message sizes
(`AppendEntriesResp` is still 56 bytes; `RaftMessage` still 64 — the extra field
fits in existing padding). None of them.

It was the machine: another application was using 68% CPU with load average 3.
The tell was that the *ratio* held — invariant checking cost 19% before and 19%
after — while only the absolute scale moved. Lesson recorded because I will
measure again in step 8: report a ratio against a baseline in the same process,
and check `uptime` before believing a throughput number.

### Numbers

| | |
|---|---|
| Tests | 107 passing (was 87) |
| Throughput | 1.12M events/sec measured under heavy external CPU load; ~5M on an idle machine |
| Checker cost | ~19% (stable across both measurements) |

---

## Entry 4 — Partitions, including asymmetric (build order step 6)

**Goal:** arbitrary node-set splits, one-way cuts, and fault schedules derived
from the seed.

**Status:** done. 130 tests pass. Log divergence is finally being reached
randomly, not just in handcrafted scenarios.

### What got built

- `crates/sim/faults.rs`. Reachability is a set of **directed** links that are
  down, not a set of groups. That costs nothing and buys the asymmetric case for
  free: `Partitions::cut(a, b)` blocks one direction only.
- `Fault`: `Partition { a, b }`, `AsymmetricCut { from, to }`, `Isolate`, `Heal`.
- Manual control — `partition`, `cut`, `isolate`, `heal` — which is both what
  handcrafted tests use and what the UI's click-to-partition will call.
- `FaultConfig` schedules: `none`, `occasional`, `aggressive`, `asymmetric_only`.

**Schedules are generated lazily**: each disturbance schedules its own repair,
each repair schedules the next disturbance, all from a dedicated PRNG stream.
Equivalent to generating the whole schedule up front from the seed, but with no
horizon, so a run can be extended without changing what already happened.

**Partitions consume no randomness.** A link that is down is down, not down with
some probability, and the check happens before the drop roll. So partitioning a
link does not shift the random stream for every other message in the run.

### The payoff

Step 5 ended with an honest admission: message loss alone produced **zero** log
truncations except under the most hostile network. With partitions
(24 seeds, 5 nodes, 30,000 ticks each):

| schedule | truncations | entries truncated | elections | max term |
|---|---|---|---|---|
| none | 0 | 0 | 33 | 1 |
| occasional | 63 | 84 | 472 | 55 |
| aggressive | 93 | 111 | 859 | 95 |
| asymmetric_only | **0** | 0 | 213 | 17 |
| aggressive + long_tail | **128** | 186 | 887 | 83 |

### The finding: the two fault classes attack different properties

Look at `asymmetric_only`: 213 elections and terms up to 17, but **zero
truncations**. One-way cuts never give any node a private log to grow, so
nothing ever diverges. They make a node deaf, it campaigns forever, and the term
inflates — an attack on *liveness* that leaves every log perfectly consistent.

Symmetric splits do the opposite: an isolated leader keeps appending entries
that can never commit, and that is what produces divergence and truncation.

Running only one kind would leave half of Raft untested while looking busy. Now
asserted by `asymmetric_cuts_attack_liveness_while_symmetric_ones_attack_consistency`.

### Two places where Raft-as-written loses liveness on purpose

Both are now tests that pin down what actually happens, rather than pretending
otherwise:

1. **The zombie leader.** A leader that can send but not receive keeps
   heartbeating, so no follower ever times out and nobody campaigns — but no
   acknowledgement gets back, so `matchIndex` never advances and nothing commits.
   The cluster looks healthy and is completely stuck. This is precisely what a
   leader lease / CheckQuorum exists to fix, and it arrives with ReadIndex in
   step 12. Safety never wobbles.
2. **The deaf follower.** A node that cannot hear the leader campaigns
   repeatedly, and every RequestVote reaches a healthy cluster and deposes a
   perfectly good leader (§5.1: any higher term wins). One deaf node out of five
   drove the term past 17 in 30,000 ticks. This is what Pre-Vote prevents; the
   paper's base algorithm has no defence and neither does this. Again, safety
   holds throughout.

Worth being clear in the writeup later: these are not bugs, they are known
properties of the algorithm as specified. The simulator makes them visible,
which is most of the point.

### A footgun my own test caught

`FaultConfig::none()` had `weight_partition: 1` and `next_fault` ignored the
`enabled` flag, so a "disabled" config happily handed out partitions — it only
stayed quiet because the *caller* checked `enabled`. A control run that is
quietly not a control is about the worst failure mode a test suite can have.
`next_fault` now checks `enabled` itself, and `none()` has all weights at zero,
so it is inert for two independent reasons.

### Notes for later

- `inject` applies a fault immediately; messages already in flight are
  unaffected, because they were handed to the network before the link went down.
  That matches a real network and is worth keeping.
- The trace now carries `Partitioned` separately from `Dropped`, so the
  visualizer can show *why* a message vanished.
- `Cluster::faults_injected()` returns every fault with its tick — that is what
  the timeline view will read.
- Still no crash/restart, so `TermMonotonic` and `CommitIndexMonotonic` remain
  validated only by direct state injection. Step 7.

### Numbers

| | |
|---|---|
| Tests | 130 passing (was 107) |
| Truncations reached randomly | 128 per 24 seeds (was 10) |
| Max term reached | 95 (was 20) |

---

## Entry 5 — Crash, restart, pause, clock skew (build order step 7)

**Goal:** a node that dies and comes back with nothing but its disk must rejoin
correctly.

**Status:** done. 147 tests pass.

### What got built

- `Fault::Crash` / `Restart` / `Pause`, and `RaftNode::restore`.
- **Crash throws away everything volatile**: the role, `commitIndex`,
  `lastApplied`, every leader-side index, *and the state machine*, which is
  emptied so the log has to replay into it. That last part is what makes the
  test meaningful — persist too little and recovery loses something; keep
  anything extra and it would pass for the wrong reason.
- **Pause** holds incoming messages rather than dropping them (a kernel socket
  buffer, not a black hole) and redelivers them on resume, so the node wakes to
  a backlog and expired timers at once.
- **Clock skew**: each node runs on its own clock, `local = global * rate / 1000`,
  with the rate fixed per run. The simulator converts node deadlines back to
  global time with *ceiling* division — rounding down would fire the timer a hair
  early, the node would find its deadline unreached, do nothing, and be
  rescheduled a tick later, burning one event per tick until the clocks agreed.

### `commitIndex` legitimately goes backwards

Figure 2 makes `commitIndex` and `lastApplied` volatile, "reinitialized after
restart". So a recovering node really does un-know what was committed, relearn
it from the leader, and re-apply its entire log. The checker has to be told, or
every restart looks like a violation — hence `Invariants::note_restart`, which
clears exactly those two watermarks and deliberately clears nothing else:

* `currentTerm` and `votedFor` are **persistent**, so their watermarks stay. A
  node that comes back in a lower term has failed to persist something the paper
  requires, and that is the bug this is here to catch.
* Global knowledge — what was committed, what was applied at each index — is
  about the cluster, not the node, and survives untouched.

Getting that split backwards would make the checker either blind or wrong, so it
is spelled out at the function.

### The step 4 gap is finally closed

`TermMonotonic` could only ever be validated by handing the checker fabricated
states, because a correct node cannot un-learn a term while running. With
crashes it is reachable for real: the new `skip_hard_state_persistence` bug
switch stops `currentTerm`/`votedFor` reaching the disk, which is *completely
invisible* until the node crashes — and then it comes back believing it is in
term 0 having voted for nobody. `a_node_that_does_not_persist_its_term_regresses_on_restart`
watches the checker fire, and its control runs the identical crash with
persistence working and stays clean.

### The finding: three fault classes, three different targets

24 seeds, 5 nodes, 30,000 ticks:

| schedule | truncations | crashes | restarts | elections | max term |
|---|---|---|---|---|---|
| none | 0 | 0 | 0 | 33 | 1 |
| crash_only | **2** | 268 | 260 | 233 | 14 |
| occasional | 37 | 50 | 48 | 341 | 42 |
| aggressive | 76 | 104 | 100 | 654 | 62 |
| everything + skew | 64 | 104 | 100 | 729 | 70 |

**Crashes alone barely diverge any logs** — 2 truncations against 268 crashes.
And that is correct, not a modelling gap. A leader appends an entry and
broadcasts it in the *same step*, so by the time it can crash the messages are
already in the network and will be delivered anyway. The followers have the
entry too, so there is nothing to diverge from. Divergence needs the append to
survive while the send does not, which is what a partition provides.

So the three classes line up as:

| fault | attacks | evidence |
|---|---|---|
| symmetric partition | log consistency | 37-93 truncations |
| asymmetric cut | liveness (term inflation) | 213 elections, 0 truncations |
| crash / restart | availability and recovery | 260 restarts, ~0 truncations |

A fuzzer that ran only one of these would leave two thirds of the failure space
untouched while looking extremely busy. Now asserted by
`crashes_attack_availability_while_partitions_attack_consistency`.

Also worth noting: `applied` jumps from ~18,000 to ~36,500 under `crash_only`,
because every restart replays the whole log. That is the recovery path being
exercised, visible as a number.

### Coverage dilution to keep an eye on

Adding crash and pause weights to `occasional` and `aggressive` diluted their
partition frequency: truncations fell from 63 to 37 and from 93 to 76
respectively. Still plenty, but the presets are now sharing a fixed weight
budget between five fault kinds. If step 8's fuzzer wants maximum divergence per
second it should probably run several narrow schedules rather than one broad
one.

### Open decision: disk faults

The spec lists "a Persist output that silently fails, or succeeds out of order".
I have deliberately not implemented silent write loss, and it needs a decision
rather than a default:

1. **Silent loss is unsound to test against the current invariants.** Raft's
   correctness *assumes* that an acknowledged write is durable. If fsync lies,
   Raft can genuinely violate safety — that is a documented property of the
   algorithm, not a bug in this implementation. Injecting it would produce real
   violations that are not implementation bugs, and the fuzzer would report
   noise.
2. **The sound and valuable version is a torn step**: apply a prefix of a step's
   persist outputs, then crash before any message goes out. That is a legitimate
   crash point Raft must survive, and it is the one case that would let a crash
   produce log divergence (persist the entry, die before the send). Steps are
   currently atomic, so it is not modelled.
3. **Write-failure-then-crash** (fail-stop disk) is also sound and nearly free.

My recommendation is (2) and (3), and to skip (1) or gate it behind a flag that
tells the checker to expect violations. Worth confirming before step 8, because
the fuzzer's noise floor depends on it.

### Numbers

| | |
|---|---|
| Tests | 147 passing (was 130) |
| Restarts exercised | 260 per 24 seeds under `crash_only` |
| Entries re-applied on recovery | ~2x the no-fault baseline |

---

## Entry 6 — Disk faults, and the fuzz harness (build order step 8)

**Goal:** run tens of thousands of seeds in minutes, and shrink any failure to a
repro a person can read.

**Status:** done. 171 tests pass. 100,000 seeds in 58s, clean.

### The disk-fault decision

I took my own recommendation from entry 5: implement the torn step, skip silent
write loss.

**Torn step** — some prefix of a step's persists lands, then the process dies
before any message goes out. Because outputs are ordered persists-first, "stop
after k outputs" is exactly "k writes landed and nothing was ever sent". It
covers the fail-stop disk error too (the write fails, the process dies), so one
knob does both.

It is the sharpest available test of the persist-before-send contract. The case
that matters: if a node could reply "I voted for you" and then die having lost
the record, it would come back free to vote again in the same term and elect a
second leader. It cannot, because the reply is ordered *after* the write being
torn away.

It is also the only fault that lets a crash diverge a log — persist the entry,
die before the send — which entry 5 predicted and
`torn_steps_are_the_crash_that_can_diverge_a_log` confirms.

**Silent write loss stays out**, and the reasoning is in `DiskConfig`'s doc
comment so nobody re-adds it: Raft's correctness *assumes* an acknowledged write
is durable. If fsync lies, the algorithm can genuinely violate safety — a
documented property of Raft, not a bug in this implementation. Injecting it
would fill the fuzzer with real violations that no code change could fix, and a
fuzzer with a noise floor is a fuzzer nobody reads.

### The harness

`crates/fuzz`, a library plus a binary. One thread per core pulling seeds off an
atomic counter, which is safe precisely because a seed's run is a pure function
of its config — nothing is shared, so nothing can make two runs differ.
`parallelism_does_not_change_results` asserts exactly that.

Everything the spec asks to randomize is derived from the seed: cluster size,
network preset, fault schedule, disk, clock skew, workload, and batch size.
Tracing is off by default (a long run produces hundreds of megabytes) and a
failing seed is simply re-run with it on — free, because the run is reproducible.

Minimization is delta debugging over the captured fault list: large chunks
first, then singles, until a full pass removes nothing. Chunks matter — a
partition and its matching heal usually have to go together, so removing them
one at a time makes no progress. On the found `commit-rule` failure it shrank 49
faults to 12 in 84 attempts.

### Finding 1: the fuzzer could not find half the bugs I planted

The first run with `--bug commit-rule` came back clean over 300 seeds. So did
`blind-commit`. Two of four deliberate bugs were invisible.

Both had the same cause: **`max_entries_per_append` was pinned at 64**.

* With a large batch, a leader's backfill of an old entry and its own election
  no-op always travel in the same message, so they commit together and a §5.4.2
  violation can never appear. The handcrafted Figure 8 test needed
  `max_entries_per_append: 1` for precisely this reason — and I had not carried
  that lesson into the fuzzer's search space.
* The same parameter decides whether a follower can fall far enough behind for
  an uncapped `leaderCommit` to run past the end of its log.

Adding batch size to the search space made `blind-commit` show up in 226 of 400
seeds. `commit-rule` is still genuinely rare — 1 seed in 30,000 — because it
needs the prior-term entry to reach a majority while the leader's own-term entry
does not, and then a node holding a *higher-term* entry at that index to survive
and win. That is a narrow window, and it is a good argument for keeping
handcrafted scenarios alongside random search: the Figure 8 test finds it every
time, in milliseconds.

The general lesson, recorded because it will recur: **a configuration constant
left fixed is a whole region of the search space that does not exist.**

### Finding 2: the fuzzer found two false positives in my own checker

This is the one I did not expect, and it is the most valuable thing in this
entry. Both were found by running at a scale the handcrafted tests never reach.

**1. "A committed entry must never be replaced", checked per node.** Fired on
seed 7 of the combined-fault suite. The report read: *node 4 now has term 41 at
index 100, but node 2 committed a term 42 entry there*.

That is not a violation. A lagging follower is entitled to hold a stale entry at
an index a majority has already committed — it just has not been caught up yet,
and the leader will truncate it on contact. Raft guarantees a committed entry
survives *in the cluster* and appears in every future leader, not that no
replica anywhere ever holds an older value.

Replaced with a sound rule: `CommittedPrefixNeverTruncated` — a node must not
truncate at or below its *own* `commitIndex`. That is about what a node does to
itself rather than how it compares to others, so a lagging replica does not trip
it.

**2. Leader Completeness did not filter by term.** Found by the 50,000-seed
sweep: *node 0 became leader of term 12 without ... index 27 should be term 13*.
An entry from term 13 cannot possibly be expected in a leader of term 12 — it
did not exist when that leader was elected. §5.4 requires an entry committed in
term T to appear in leaders of terms *greater than* T, and my check compared
against every committed entry regardless. One `filter(|(_, fact)| fact.term < term)`.

Both fixes are pinned by tests, including
`leader_completeness_ignores_entries_committed_in_later_terms`, which asserts
the false positive specifically.

The reflection worth keeping: checker validation runs in **two** directions. Step
4 established that the checkers fire on real bugs. It took 50,000 seeds to
establish that they do not fire on correct behaviour — and a checker with a noise
floor is worse than no checker, because it trains you to ignore it.

### Results

100,000 seeds, 30,000 ticks each, 8 threads, 58 seconds:

| | |
|---|---|
| violations | **0** |
| elections | 2,726,106 |
| log truncations | 184,202 |
| crashes / restarts | 781,963 / 766,073 |
| torn steps | 102,233 |
| messages sent | 533,046,838 |
| dropped / partitioned | 33,353,990 / 25,979,913 |
| duplicated / reordered | 11,419,734 / 2,249,655 |
| max term reached | 157 |
| seeds with faults scheduled | 92,832 |

Bug detection rates over 500 seeds (30,000 for `commit-rule`):

| bug | seeds to first find |
|---|---|
| double-vote | 418 of 500 fail |
| blind-commit | 275 of 500 fail |
| no-persist | 66 of 500 fail |
| commit-rule | 1 of 30,000 |

### Notes for later

- The report deliberately prints a "weak coverage" section when a sweep never
  truncated a log, never elected anyone, or never committed anything. A green
  result that reached nothing is worse than a red one.
- Violations are listed earliest-first, because the first is usually the cause
  and the rest are downstream symptoms. On the `commit-rule` find, Leader
  Completeness came first and State Machine Safety followed.
- Leader Completeness is still checked once, at the first observation of a node
  as leader of a term. An entry committed in an earlier term but *recorded*
  after that moment would be missed. That is a false negative, not a false
  positive, so it is acceptable — but worth revisiting if a real bug ever slips
  through.
- The minimizer preserves the *invariant kind*, not the exact tick, because
  removing a fault shifts every subsequent random draw. A removal is only kept
  when the failure survives, so the result is always a genuine reproduction.

---

## Entry 7 — History recording and the linearizability checker (build order step 9)

**Goal:** decide whether the story told to clients could have come from a
correct single-threaded key/value store.

**Status:** done. 211 tests pass. 50,000 seeds, every history linearizable.

### History recording

`crates/sim/history.rs`. The entire value of the file is one distinction:

| outcome | means | how the checker treats it |
|---|---|---|
| `Completed` | the client got an answer | must be linearized, with that result |
| `Rejected` | the node said "not leader" | never entered any log — dropped |
| `Pending` | no response ever arrived | **may or may not have happened** |

A pending operation is not a failed one. Recording it as failed invents
violations; recording it as succeeded hides them. `Rejected` is genuinely
different from `Pending` and worth its own variant: a refusal is information,
silence is not.

### The checker

`crates/sim/linearizability.rs`. Wing & Gong's search with Lowe's optimizations:

* **P-compositionality.** Operations on different keys cannot interact, so the
  history splits per key and each sub-history is checked alone. Not a
  nice-to-have — the search is exponential in what it is handed.
* **Linearize the earliest candidate, recurse, backtrack.** An operation may go
  next only if nothing still outstanding had to finish before it started.
* **Memoize on (state, remaining bitmask).** Two orders reaching the same state
  with the same work left are the same subproblem.

Pending operations are handled by kind, and each rule needed thinking about:

* A pending **read** observed nothing, so it constrains nothing. Dropped.
* A pending **write** is *optional*. The search may place it (a later read might
  need it to explain what it saw) or leave it out. Its response time is
  unbounded, so nothing forces it to come before anything.

The optional treatment matters in both directions, and both are tests:
`a_pending_write_can_explain_a_later_read` (a checker treating pending as failed
would report a violation that is not there) and
`a_pending_write_need_not_have_taken_effect` (one treating it as succeeded would
report the opposite). `a_pending_write_does_not_excuse_a_real_violation` stops
the optionality becoming a universal excuse.

On the state model: the search keeps one key's value as an `Option<String>`
rather than reusing `KvStore`, because that state is cloned and compared on
every memo lookup and the cost dominates. `agrees_with_the_real_store` runs
5,000 random commands through both and asserts they never diverge, so the two
cannot drift.

**Honesty.** The problem is NP-hard. When the budget runs out, or a sub-history
exceeds the 128-operation cap, the answer is `Unknown` with a reason. For an
over-long sub-history it checks a prefix — linearizability is prefix-closed, so
a violation found there is real — but reports `Unknown` rather than
`Linearizable`, because a clean prefix says nothing about the rest.

### The finding: a bug that no invariant can catch

Every deliberate bug so far was caught by the per-event invariant checks, which
raises a fair question: what is the linearizability checker actually for?

So I added `stale_reads` — a leader answering reads straight from its own state
machine, with no ReadIndex round to confirm it is still the leader. A leader
deposed behind a partition then serves values the cluster has long since
overwritten.

The fuzzer's report on seed 4:

```
seed 4 FAILED (3 nodes, 0 violation(s))

  the client-visible history is impossible:
  no sequential ordering of the operations on key "k2" can explain what clients saw.
      [2647..2714] c2 put(k2, v14) = ok
      [4086..4127] c0 del(k2)      = ok
      [4454..4454] c1 get(k2)      = v14
```

**Zero invariant violations.** Every Raft property still holds — one leader per
term, logs matching, nothing committed and lost — because the read never enters
the log at all. A delete is acknowledged at tick 4127 and a read at 4454 returns
the deleted value. Only the client-visible history is wrong, and only the
linearizability check can see it.

That is the argument for the checker, and it is now a test
(`the_linearizability_checker_catches_what_no_invariant_can`) with a control
asserting the identical sweep is clean without the bug. It is also a preview of
exactly what ReadIndex exists to prevent in step 12.

### Results

50,000 seeds, 30,000 ticks each, 30 seconds:

| | |
|---|---|
| violations | **0** |
| histories linearizable | 50,000 |
| histories undecided | 0 |
| client operations | 6,200,531 |
| never answered (pending) | 270,436 |
| log truncations | 91,702 |
| crashes / restarts | 390,747 / 382,801 |

The pending count matters: 270,436 stranded operations means the hard path is
being exercised on real data, not just in handcrafted tests. The report warns
when it is zero, for exactly that reason.

Bug detection over 500 seeds:

| bug | failing seeds | caught by |
|---|---|---|
| double-vote | 418 | invariants |
| blind-commit | 275 | invariants |
| no-persist | 66 | invariants |
| stale-read | 5 | **linearizability only** |

Linearizability checking costs about 2% of sweep throughput (1688 vs 1724
seeds/sec), which is cheap enough to leave on for every seed.

### Notes for later

- Reads currently go through the log, so they are linearizable by construction.
  When ReadIndex lands in step 12 the checker becomes the primary defence for
  that path — the `stale_reads` switch is already there to validate it.
- The 128-operation-per-key cap has not been hit by any fuzz workload
  (`histories undecided: 0`), but a longer run or a narrower key space would hit
  it. If that starts happening, splitting sub-histories at quiescent points —
  moments when nothing is in flight — is the sound way to extend the cap.
- Client retries are not modelled yet: every request is issued once, so
  `Rejected` really is terminal. Once session-based deduplication arrives in
  step 12, a retried request becomes one logical operation spanning several
  invocations, and the history recorder will need to merge them.

---

## Entry 8 — Snapshots and log compaction (build order step 10)

**Goal:** stop the log growing forever, and catch up a follower whose entries no
longer exist.

**Status:** done. 237 tests pass. 100,000 seeds clean, with 1,195,983 snapshots
taken and 230,884 shipped to followers.

**Two real bugs in my implementation, both found by the fuzzer.** This is the
first step where the fuzzer earned its keep on the actual code rather than on
deliberately planted bugs.

### What got built

- `Snapshot { last_included: LogId, data: Vec<u8> }`, `InstallSnapshot` RPC,
  `Input::Compact`, `Output::RestoreSnapshot`, and three new persist ops
  (`Snapshot`, `Compact`, `ResetLog`).
- Compaction is driven from *outside* the node, because the state machine is
  outside it: the simulator snapshots the store at `lastApplied` every N entries
  and hands the bytes back through `Input::Compact`. Raft never learns what is
  in them.
- A follower installing a snapshot keeps its log suffix when it can confirm the
  boundary entry, and discards the log entirely when it cannot.

Snapshots are sent whole rather than chunked with an offset/done flag. Chunking
is a transport concern with no bearing on correctness, and modelling it would
add reassembly state to every follower without exercising anything
Raft-specific. Noted rather than silently skipped.

### Bug 1: index 0 stops being an anchor once you compact

The trap the spec warns about, hit exactly.

`term_at(0)` returned `Some(0)` as the "before the beginning of the log"
sentinel, which is what lets a leader replicating from scratch pass the
`prevLogIndex` check with no special case. **After compaction that is wrong.** A
leader whose `nextIndex` for some follower had walked back to 1 would probe with
`prev_log_index = 0`; a follower that compacted through index 8 answered
`Some(0)`, the check passed, and the follower appended entries 1..7 onto a log
that began at index 9.

The result was a silently corrupt log: slot 10 held an entry claiming to be
index 1, `last_index()` computed 16 from the offset while `last_log_id()`
reported 7. The fuzzer surfaced it as a Log Matching violation on seed 11278,
minimized to **zero faults** — compaction alone reproduces it.

The fix is small and makes the sentinel uniform: **the anchor is always
`last_included`**, which is `(0, 0)` for a log that has never been compacted. So
index 0 answers on a fresh log and stops answering the moment anything is
discarded.

That change made the leader's back-off loop stall — a follower whose log begins
after the probe now rejects, and the clamp from entry 3 only ever moved
`nextIndex` backwards. Rejections carrying "my log starts at N" are now allowed
to move it *forward*.

### Bug 2: the snapshot and the log are two artifacts that can disagree

Found by torn steps, which is exactly what they are for.

`on_install_snapshot` persists the snapshot and then resets the log. I had
written a comment claiming the order was safe because a crash in between "leaves
the snapshot stored and the log merely un-trimmed — redundant, not lossy". That
is true for the *compaction* case, where the log is a superset. It is false for
the *reset* case, where the surviving log is a stale, shorter one. Recovery then
paired a snapshot at index 120 with a log ending at 119 and came back with
`commitIndex` past the end of its own log.

Fixing it in `RaftNode::restore` alone made things worse in a way worth
recording: the node's in-memory log got reconciled while the **durable** log did
not, so the very next `Append` landed at an index the disk was not expecting.
The reconciliation is a recovery action and has to be written back to disk, not
just applied in memory. It now lives in one place,
`Log::reconcile_with_snapshot`, called from both `Storage::recover` and
`RaftNode::restore` — idempotent, so running it twice is a no-op.

### The assert that made both findable

`Log::append` had a `debug_assert_eq!` on contiguity. Release builds skip it, and
release is where the fuzzer runs — so the first bug corrupted logs silently and
only showed up much later as a confusing Log Matching failure with no obvious
cause.

It is now a hard `assert!`. One comparison per entry, against the alternative of
silent corruption whose symptoms appear far from the cause. When the second bug
was introduced, the assert caught it immediately and pointed straight at
`Storage::apply` in the backtrace instead of producing another puzzle.

General lesson: **a `debug_assert` guarding an invariant that the fuzzer is
meant to catch is not guarding anything.** Worth auditing the rest.

### Checker adjustments compaction forced

- **Leader Completeness** looked up committed entries with `log.get(index)`,
  which returns `None` for a compacted index. Every compacted leader therefore
  looked like a violation. Entries below the log's start are inside the node's
  snapshot and by construction present, so they are skipped now.
- **AppliedInOrder** had to learn about snapshot installs: a follower that
  installs a snapshot jumps its state machine forward without applying the
  entries in between, so the expected sequence resumes after the boundary.
- **Restart** now restores the applied watermark to the snapshot index rather
  than clearing it, because a node with a snapshot does not replay the entries
  below it.

### Results

100,000 seeds, 30,000 ticks each, 57 seconds:

| | |
|---|---|
| violations | **0** |
| snapshots taken / installed | 1,195,983 / 230,884 |
| histories linearizable | 100,000 of 100,000 |
| client operations | 12,406,055 |
| never answered | 538,626 |
| log truncations | 171,813 |
| crashes / restarts | 784,116 / 768,148 |
| torn steps | 103,382 |

### Notes for later

- Compaction is now part of the fuzzer's search space, and so is *not*
  compacting — the uncompacted runs are the control that makes any
  snapshot-specific failure attributable.
- The report warns when a sweep takes no snapshots or ships none, for the same
  reason it warns about the other coverage gaps.
- Joint consensus (step 11) will have to put the configuration inside the
  snapshot: a node restoring from one needs to know the membership it applies
  to, and right now `Snapshot` carries only state machine bytes.

---

## Entry 9 — Joint consensus (build order step 11)

**Goal:** change cluster membership through C_old,new, the way §6 specifies.

**Status:** done. 267 tests pass.

**One missing rule found, and it was load-bearing.**

### What got built

- `ClusterConfig` now holds an optional second voter set. `is_quorum_by` is the
  whole safety argument in one function: while joint, a decision needs a
  majority of C_old **and** a majority of C_new, checked independently. Both
  vote counting and commit counting go through it, so there is one place to get
  it right.
- `EntryPayload::Config`, `Input::ChangeMembership`, and the automatic
  C_old,new -> C_new transition once the joint entry commits.
- A leader outside C_new keeps leading until C_new commits — it is the one
  replicating the entry that removes it — and steps down immediately after.
- Snapshots carry the configuration. Without it a node recovering from one has
  discarded every config entry below the boundary and would silently resurrect
  whatever membership it was started with.

**Applied on append, not on commit.** A node adopts a config entry the moment it
lands in its log, and *un*-adopts it if that entry is later truncated away
(`refresh_config` scans back for the last surviving one, falling back to the
snapshot's). The reason it cannot wait for the commit: the very quorum needed to
commit the entry may be one it would otherwise refuse to count.

### The finding: §6 needs its disruption guard, and I had not implemented it

The first end-to-end run of a membership change did not settle. Probing showed
the same shape on every seed:

```
node 3: role=Leader    term=178  cfg={0,1,3}                   commit=87
node 4: role=Candidate term=178  cfg={0,1,2,3,4} -> {0,1,3}    commit=6
```

A server *removed* by the change was stuck holding only the **joint** entry —
which still includes it — and had stopped receiving heartbeats because the
leader had moved on to C_new. So it timed out forever, and under §5.1 every
RequestVote it sent, each with a higher term, deposed a perfectly healthy
leader. Term 178, commit index stuck at 6, no progress.

This is not an exotic corner: §6 describes it explicitly and prescribes the fix.

> "if a server receives a RequestVote RPC within the minimum election timeout of
> hearing from a current leader, it does not update its term or grant its vote."

I had implemented §6's joint consensus and skipped the paragraph after it.
`ignores_vote_requests` now implements it — no term update, no reply at all —
and the cluster settles.

Worth noting *why* it had gone unnoticed: without membership changes, a node
that stops hearing from the leader is genuinely partitioned, and its disruption
was something I had already observed, tested and written up in entry 4 as "what
Pre-Vote fixes". It looked like a known limitation rather than a missing rule.
Membership changes turn the same situation into an ordinary consequence of a
routine operation, which is what made it impossible to keep waving at.

The guard also retroactively improved the step 6 behaviour. That test now
asserts the stronger and correct property: a deaf follower's *own* term runs
away, while the healthy cluster's term and leadership are completely untouched.

### A test that tested nothing

The first version of `a_joint_configuration_is_actually_reached` partitioned the
incoming nodes away from `{0,1,2}` and expected the cluster to be stuck in
C_old,new. It was not, and the reason is worth remembering: the change was
`{0,1,2}` to `{0,1,2,3,4}`, and `{0,1,2}` is *already* a majority of the larger
set. Growing a cluster does not need the new members to agree to anything.

Freezing a change genuinely requires C_new to depend on the unreachable nodes,
so the test now moves `{0,1,2}` to `{2,3,4}` — one node of overlap — and
asserts the joint entry is still uncommitted while it is stuck.

### And a third false positive in the checker

The 50,000-seed sweep that followed reported a Leader Completeness violation:
node 2 became leader of term 37 without entries committed by node 0 in term 33.
Tracing it tick by tick:

```
tick 26309  node 2 -> Candidate term 37   (log ends 101)
tick 26483  node 0 -> Candidate term 38   (log ends 104)
tick 26501  node 0 -> Leader    term 38   (appends 105 @ t38)
tick 26561  node 0 commit reaches 105     (102-104 @ t33 committed indirectly)
tick 26577  node 2 -> Leader    term 37   (last vote arrived late)
```

Node 2 *won* term 37 back at 26309, when 102-104 were still uncommitted and
denying it a vote would have been wrong. Its final vote response was delayed by
the long-tail network, so it only transitioned to Leader at 26577 — by which
time node 0 had become leader of term **38** and committed those entries
indirectly under §5.4.2.

§5.4 says an entry committed in term T appears in the logs of leaders of terms
*greater than* T. These were committed in term 38, and 37 is not greater than
38. My checker was filtering on the term the entry was **created** in (33)
rather than the term it was **committed** in (38) — and because §5.4.2 exists
precisely so that earlier-term entries can be committed later, those two terms
are routinely far apart.

`CommittedFact` now records both, and the filter uses the commit term. Pinned by
`leader_completeness_ignores_entries_committed_after_the_leader_was_elected`.

That is the third false positive a large sweep has found in the invariant
checkers, after the per-node "committed entry replaced" rule (entry 6) and the
missing term filter (entry 6). The pattern is consistent and worth naming: **the
checkers are hardest to get right where Raft's guarantees are conditional.**
Every one of these was a case where the property holds only under a
qualification — a lagging replica is exempt, a stale leader is exempt, a leader
elected before the commit is exempt — and the first draft asserted the
unqualified version.

### Coverage

50,000 seeds, 40,000 ticks each, with membership churn folded into the search
space alongside everything else:

| | |
|---|---|
| violations | **0** |
| membership changes | 15,722 |
| snapshots taken / installed | 727,889 / 147,858 |
| histories linearizable | 50,000 of 50,000 |
| elections | 2,229,641 |
| max term | 246 |
| crashes / restarts | 599,622 / 591,579 |
| client ops / never answered | 8,130,924 / 379,104 |

Around a third of seeds start with only three of their nodes as voters, so a
change has somewhere to grow into; the rest never change membership and are the
control.

### Notes for later

- ReadIndex (step 12) has to consult the *current* configuration when it
  confirms leadership with a heartbeat round, which means a joint configuration
  needs both halves there too. `is_quorum_by` already expresses that.
- Non-voting members (§6's catch-up phase) are not modelled: a node added by a
  change starts participating in quorums immediately, so a slow new member can
  stall commitment until it catches up. The paper adds them as learners first.
  Worth doing if the visualization is going to demonstrate adding a node.
- `config_at` scans backwards from the snapshot boundary on every compaction.
  Configs are rare so it has never shown up, but it is O(log length) and could
  be cached alongside `config_index` if it ever matters.

---

## Entry 10 — ReadIndex and client sessions (build order step 12)

**Goal:** linearizable reads without writing to the log, and a retried request
that is applied exactly once.

**Status:** done. 293 tests pass. 60,000 seeds clean.

**Three bugs in my own code, all caught by the linearizability checker.** The
per-event invariants said nothing about any of them — which is the argument
entry 7 made in the abstract, now demonstrated three times over.

### What got built

- **ReadIndex (§6.4).** A read never enters the log. The leader records the
  commit index, confirms it is still the leader by hearing back from a quorum on
  a fresh heartbeat round, waits for the state machine to reach that index, and
  only then answers. The round carries an id and the acknowledgement echoes it,
  so a delayed reply from an older round — which proves nothing about the
  present — cannot be mistaken for confirmation.
- **Session deduplication (§8)** in the state machine, not on the leader. That
  placement is the point: the table is replicated, so a retry that lands on a
  *different* leader is still recognised. A leader-local table would forget
  everything exactly when clients retry.
- **A retrying client** (`ClientDriver`), because without retries the session
  table never does anything and "exactly once" is an untested claim.
- History recording learned that a retry is the *same* logical operation: the
  invocation time stays at the first attempt, and a refusal is a failed attempt
  rather than an outcome.

### Bug 1: the read index was captured too early

The very first fuzz run after wiring ReadIndex up reported an impossible
history. The trace:

```
tick  9040  node 0 RESTARTED term=15 last=22
tick 10099  node 0 READ read_index=0 -> Value(None)
tick 10099  node 0 APPLIED idx=4  ... Cas { k1 } -> Ok
tick 10099  node 0 APPLIED idx=6  ... Put { k1, v6 }
```

A node restarted with an empty state machine, took a read, and answered `nil`
*before replaying a single entry*.

I had captured `read_index = commitIndex` when the request arrived. A node that
has just restarted — or just been elected — has `commitIndex` 0 until an entry
of its own term commits, and a read pinned to 0 requires nothing of the state
machine at all. The paper assigns the read index only *after* the leader knows
what is committed, and I had read that as a liveness detail rather than the
whole point.

`read_index` is now `Option<Index>`, assigned when the read first becomes
eligible, and the confirmation round starts at the same moment.

### Bug 2: reads resolved before entries applied

Outputs are delivered in order, and `resolve_reads` ran before `advance_apply`
in `step()`. So a read that became ready in the same step as the applies it was
waiting for was emitted *ahead* of them, and the application answered it from a
store it had not finished updating.

Resolving now happens after applying. The check was conservative enough to hide
this most of the time, which is exactly why it needed the ordering fixed rather
than the check tightened.

### Bug 3: §8's session table assumes one request in flight per client

With retries on, the checker reported something that cannot be an ordering
problem at all:

```
[4655..5929] c2 put(k0, v23) = cas-failed(actual v20)
```

A `put` that answered `cas-failed`. The session table keeps a single slot per
client — the latest sequence number and its response — so when a client has
several requests in flight, an old retry can arrive after a newer request has
claimed the slot and be handed a completely different command's answer.

§8 assumes a client sends commands one at a time; my driver did not. Fixed in
the driver, with the assumption written down at `apply_for` where the next
person will look. Concurrency comes from having several clients, which is the
model the paper describes.

That bug also produced a *much* better diagnostic than it deserved to. The
checker now shape-checks results before searching — a `Get` must answer
`Value`, a `Put` or `Delete` must answer `Ok`, a `Cas` must answer `Ok` or
`CasFailed` — so an answer belonging to a different command is reported as
exactly that, instead of as a twenty-operation "no ordering exists".

### The bug switch finally has something to validate

`stale_reads` was added in step 9 with nothing on the other side of it — reads
went through the log, so they were linearizable by construction. Now it means
"skip the ReadIndex round", and the pair of tests is the real thing: the same
200 seeds are clean with ReadIndex and produce impossible histories without it.

### Coverage

60,000 seeds, 40,000 ticks each, retries and membership churn in the search
space:

| | |
|---|---|
| violations | **0** |
| reads served via ReadIndex | 594,502 per 20,000 seeds |
| histories linearizable | all |
| client ops / never answered | 2,835,874 / 66,136 |
| snapshots / installed | 204,762 / 42,014 |
| membership changes | 6,404 |

Bug detection still works across the board: double-vote 371/400, blind-commit
184/400, no-persist 67/400, stale-read 2/400.

### Notes for later

- **CheckQuorum is still not implemented**, and with ReadIndex it is not needed
  for safety: an unconfirmed leader simply cannot answer, so the client hears
  nothing rather than something stale. It would help liveness — the zombie
  leader from entry 4 still cannot commit and still will not step down — and it
  is the prerequisite for the *lease* optimisation that skips the heartbeat
  round entirely. Worth doing only if the visualization wants to show it.
- Reads are answered by the leader only. Follower reads with ReadIndex (ask the
  leader for the index, then serve locally) are a natural extension and would
  make the visualization more interesting.
- Session state lives in the `KvStore` and therefore inside snapshots already,
  which is what makes deduplication survive compaction. There is no eviction
  policy: a long run accumulates one slot per client forever. Fine at this
  scale, wrong for a real system.

---

## Entry 11 — The wasm boundary and the cluster view (build order step 13)

**Goal:** the simulator running in a browser, with a picture of the cluster.

**Status:** done. 293 tests pass, wasm module 466 KB after `wasm-opt`, site 11 KB
of JS.

### What got built

- `crates/wasm`: a `Sim` handle exposing `step`, `stepOnce`, `snapshotState`,
  `inject`, `clientRequest`, `clientRead`, `history`, `check` and `faults`.
- `web/`: Vite + TypeScript, a canvas cluster view, and the controls needed to
  drive it.
- `tools/build-web.sh`, which builds both halves with the `BASE_PATH` that
  GitHub Pages needs.

The boundary is genuinely thin — it marshals and nothing else. Everything worth
testing stayed in `sim`, where it is tested natively rather than through a
browser.

**JSON strings rather than `JsValue`.** Structured marshalling would mean
`serde-wasm-bindgen`, which is outside the dependency budget this project set
itself. A JSON string parsed on the JS side costs one serialization per frame
and keeps the boundary to `wasm-bindgen` alone. If rendering ever becomes the
bottleneck the fix is to send only what changed, not to add a dependency.

### `sent_at` on delivery events

Messages are drawn as dots part-way along their wire, and their position is
`(now - sent_at) / (arrives_at - sent_at)`. The queue already knew when a
message would arrive; it now records when it was sent, purely so the view can
interpolate.

That makes the animation a property of the *simulated* clock rather than of the
frame rate: slow the simulation down and the dots slow down with it, because
they are showing where a message actually is at tick N. Nothing in the simulator
reads the field, and the queue is still ordered by `(tick, seq)`.

### Frame rate is not simulation rate

The first version advanced `min(0.1s, elapsed) * speed` ticks per frame. Under
the headless browser pane — which throttles `requestAnimationFrame` to roughly
1Hz — that produced 4 ticks per second at a nominal 4x, and the cluster never
got as far as an election.

The throttling is an artifact of the environment, but the coupling was a real
design flaw: with few, long frames the simulation crawls, and the `dt` cap made
it worse. The loop now takes elapsed time up to 0.25s and caps the *work* at
20,000 ticks per frame as a separate bound. Those are two different concerns —
one keeps a backgrounded tab from simulating an hour on return, the other keeps
a single frame from blocking the main thread — and conflating them made both
worse.

### What it looks like

Seven nodes, long-tail network, aggressive faults, seed 7: node 3 leading in
term 1 at commit 8, node 2 campaigning in term 4 at commit 2, and the wires
between the two groups drawn dashed. The partition is visible as a gap rather
than something to infer from the numbers, and the two sides sitting at different
commit indices is the whole point of the exercise made literal.

One-way cuts are drawn in a different shade from full splits, because they
behave completely differently — entry 4 measured that asymmetric cuts inflate
terms without ever diverging a log — and it helps to see which one is on screen.

### Notes for later

- The determinism source scan now covers `crates/wasm` as well, with `Date::now`
  and `Math::random` added to the banned list. The boundary is exactly where
  reaching for a browser clock would be easy and would silently destroy
  reproducibility.
- Step 14 adds the log strip, the timeline and click-to-partition. The state
  snapshot already carries the log window and the blocked links, so the data is
  there.
- `snapshotState` sends a 48-entry window of each log. A cluster with seven
  nodes and long logs is a few kilobytes per frame, which is fine; if the
  timeline needs full history it should be a separate call rather than riding
  along on every frame.

---

## Entry 12 — The full visualization (build order step 14)

**Goal:** the log view, a scrubbable timeline, direct manipulation of the
cluster, and the verification panel.

**Status:** done. 297 tests pass. 477 KB of wasm, 18 KB of JS.

### Time travel is just replay

The timeline can jump backwards, and the implementation is the payoff for every
constraint this project has kept: **rebuild from the seed and re-run**. No
snapshot history, no undo log, no state diffing. A run is a pure function of its
configuration, so tick 8,241 is reproducible from nothing but the seed and the
number 8,241.

The one wrinkle is that it is a pure function of the configuration *plus the
things the person watching did*. A partition dragged out at tick 4,000 is not
in the seed, so `Sim` records every user action with its tick and replays them
at the same ticks during a seek. The generated fault schedule needs no such help
— it comes back on its own.

Verified live: jumping from tick 9,008 back to 8,241 rebuilt the whole state —
2,490 events down to 2,286, eleven elections down to nine, the log strips
redrawn to their earlier shape.

### The log view earns its billing

The plan called the log view "the single most illuminating thing in the whole
project", and having built it, that reads as an understatement. One strip per
node, one cell per entry, coloured by the term that created it, filled once
committed and outlined while not.

Every strip is drawn against the *same* index range, so a column means the same
log position on every node. Per-node scaling would have been easier and would
have destroyed the entire point: comparing the rows is the thing.

Running seed 11 with an aggressive schedule produced exactly the picture the
plan describes — terms in four different colours across the strips, hollow
uncommitted cells at the tails of the two nodes on the far side of a partition,
and the split visible simultaneously as dashed wires in the cluster view and as
an amber `partition [0, 2, 3] | [1, 4]` line in the timeline.

A compacted prefix is drawn as a flat band labelled "snapshot" rather than left
blank, because a log that starts at index 90 is not a log with 89 missing
entries, and blank space would say the wrong thing.

### Tools instead of modifier keys

The plan asks for click-two-nodes-to-partition, drag-a-box-to-split, and
click-a-node-to-crash. Those are three different meanings for a click, and
overloading them onto modifiers would make the interesting features invisible.

There is a row of tools instead — partition, one-way cut, crash/restart, pause —
with a hint line that changes to say what a click will do. Box-dragging always
partitions whatever it encloses from everything else, since there is nothing to
confuse it with.

### Bounded timeline, unbounded run

The full trace is millions of records; the timeline is a ring buffer of 2,000
moments. Commits are recorded sparsely — every commit would bury the elections
and faults under a wall of noise — and violations are mirrored into a list of
their own so a failure can never scroll away.

The DOM list is only rebuilt when its contents actually change, because
re-rendering 140 rows every frame fought the scroll position and made the list
unusable while the simulation ran.

### Notes for later

- Seeking backwards replays from tick 0. At a few million events a second that
  is imperceptible for the runs this UI produces, but a seek into the middle of
  a ten-million-tick run would stall a frame. If that ever matters, the fix is
  periodic checkpoints, not incremental undo.
- `snapshotState` is serialized every frame. At five nodes it is a few
  kilobytes; the log window is capped at 48 entries per node for exactly this
  reason.
- The `break ReadIndex` checkbox is wired to the stale-read switch, so the
  linearizability panel can be watched going red on a run where every internal
  invariant still holds. That is the demonstration the checker exists for, and
  it is now two clicks away.

---

## Entry 13 — The fuzz results page (build order step 15)

**Goal:** the writeups, with links that reproduce each failure live.

**Status:** done. 297 tests pass. 100,000 seeds, 0 violations.

### Making fixed bugs reproducible again

The plan asks for "a link that loads it live in the visualizer", and every bug
worth writing up had already been fixed. Describing them would have been the
easy path and a much weaker artifact.

So the three real defects are now regression switches alongside the planted
ones — `compaction_anchor_at_zero`, `skip_snapshot_reconcile`,
`read_index_at_arrival` — each restoring the original behaviour exactly, each
reachable from a URL, each marked in the source as a deliberate regression.

Getting them to reproduce *faithfully* took more care than expected, and each
correction was itself informative:

**The compaction bug needed its silence back.** After finding it I promoted
`Log::append`'s contiguity check from `debug_assert` to a hard `assert`, so
re-enabling the bug aborted the worker thread instead of corrupting a log. But
when that bug shipped, the check was a `debug_assert` and release builds skipped
it — the corruption was silent, which is exactly why it took a Log Matching
violation to notice. The switch now suppresses the guard, and the checker
reports it the way it originally did. It also had to be applied to the *durable*
log: the corruption propagates through the persist stream, so the disk hits the
same guard.

**The read-path bug needed both halves.** Re-introducing only the early
`read_index` capture found nothing across 3,000 seeds. It needed the second
mistake too — resolving reads before applying entries — and, on closer reading of
the original trace, a third detail: the confirmation round also started on
arrival, which is what let acknowledgements pile up while the leader still did
not know what was committed. All three are one switch now, because it took all
three to fail. That is worth knowing about the original defect: it was not one
mistake, it was three that only mattered together.

### A link that takes ninety seconds is a bad link

The first repro seeds came from a grid search that ran each candidate to 40,000
ticks and reported whether it failed. They did fail — eventually. Clicking one
and waiting a minute and a half for a red panel is not a demonstration.

The search now records the *earliest* tick at which the failure is visible and
prefers seeds that fail early. All three now break within six thousand ticks, a
few seconds at 1000x.

### The search has to match the code path

An earlier version of that search pre-rolled with `run_until_leader` before
starting the workload. The browser does not: the client driver is active from
tick zero. That shifts every subsequent draw, so the seeds it found did not
reproduce in the visualizer at all.

Obvious in hindsight, and a nice illustration of the property the whole project
rests on: determinism is only useful if you reproduce the *same* run, and "same
seed" is not sufficient — the sequence of calls has to match too.

### The footgun that cost the most time

The three repro links did not work in the browser, and I spent a long stretch
chasing it through the UI: reading the DOM, comparing check results, suspecting
the client driver, suspecting stale module scopes.

**The wasm module was stale.** I had added the regression switches, rebuilt the
native fuzz binary, and never re-run `wasm-pack`. The browser was parsing
`bugs: ["early-read-index"]` with an older `UiConfig` that did not know the name
and — because unknown names are ignored — silently ran a perfectly healthy
cluster.

Two lessons. The first is mine: `tools/build-web.sh` rebuilds both halves and I
should have been using it rather than hand-running pieces. The second is a
design smell worth remembering — `UiConfig` ignores bug names it does not
recognise, which turned a version mismatch into silence instead of an error.
Lenient parsing at a version boundary is how you get a demo that quietly shows
the wrong thing.

### What the page says

The headline sweep, a coverage table with the numbers that make a clean result
mean something, five bug writeups, the three checker false positives, and a
detection-rate table.

The last row of that table is the honest one. The §5.4.2 violation — Figure 8,
the most commonly botched rule in Raft — was found once in 20,000 seeds earlier
in development and has not been reproduced since the search space widened; it is
0 in 30,000 now. The handcrafted Figure 8 scenario finds it every single time,
in milliseconds. Random search covers the space you did not think of;
handcrafted tests cover the space you did. Neither replaces the other, and the
page says so.

---

## Entry 14 — CI and the Pages deploy (build order step 16)

**Goal:** the project on GitHub, tested on every push, deployed to Pages.

**Status:** done. 297 tests pass, clippy clean, formatted.

### CI gates on the things that would invalidate the project

`ci.yml` runs three jobs. The test job is the important one: the determinism
test and the source scan for wall-clock time, ambient randomness and hash
iteration both live in `cargo test`. Any of those creeping in makes every
recorded seed in the repository mean something different, so they gate the build
rather than living in a nightly job that nobody reads.

The fuzz job sweeps a **fixed** seed range, so a regression shows up as the same
seed failing rather than as a number that drifts run to run. It then runs a
short sweep with a bug deliberately switched on and fails if the fuzzer comes
back clean — a harness reporting "no violations" is only worth something if it
would have said otherwise.

### The base path is derived, not written down

Pages serves from a repository subpath, so every asset URL has to be relative to
it. The workflow computes it from `GITHUB_REPOSITORY` rather than hardcoding a
string, so renaming the repository cannot silently break every asset. The 404
page bounces back to the visualizer keeping the query string, because a shared
link with a seed in it should survive a mistyped path.

### Making CI pass took a real cleanup

Adding `clippy -D warnings` and `fmt --check` to CI meant actually satisfying
them, and the codebase had accumulated eleven lints. Most were cosmetic, but two
were worth the change on their own merits:

* `Workload::next` reads like `Iterator::next` and is not one. Renamed to
  `next_request`.
* Three test files declared bare `[(&str, fn() -> NetworkConfig); 4]` types.
  Aliased, because that signature is unreadable at every use site.

One had to be done carefully. Clippy wanted the PRNG stream labels regrouped
into even hex digit groups, and those labels *are* the streams — changing a
value changes every seed derived from it, which would have invalidated the three
repro links on the results page. They were padded with leading zeros instead, so
the values are byte-identical, and the repros were re-verified afterwards
through the wasm boundary.

### Attribution

Commits are authored solely by the repository owner, with no co-author trailers.

### Notes for later

- `Swatinem/rust-cache` is doing the heavy lifting on CI time; a cold run builds
  the whole workspace plus a release fuzz binary.
- The Pages workflow installs `wasm-pack` by curl on every run. Pinning a
  version would be more reproducible; the installer script is a moving target.
- `tools/build-web.sh` is the only correct way to build the site by hand. Doing
  the halves separately is how the wasm module went stale in step 15 and cost an
  hour.

### Postscript: two things CI caught immediately

**A real test bug, hidden by a bad verification command.** The first CI run
failed on `compaction_and_crashes_together_still_converge`. It failed locally
too, identically — I had simply not noticed, because the one-liner I had been
using to summarise `cargo test` all along was

```
awk -F'[ ;]' '{p+=$4; f+=$8}'
```

and on a `test result:` line `$8` is the *word* "failed", not a number. Awk
coerces it to 0. **My failure count had been zero by construction for the entire
project.** The passing count was real, which is why the totals always looked
plausible.

The lesson is not subtle and is worth writing down: check the exit code. A
summary that cannot express failure is not a summary, it is a decoration.

**And the failure it was hiding was real.** At seed 7, node 1 finished the run
with `commitIndex` exactly equal to its snapshot index — it had been restarted by
the fault schedule moments before the assertion and had not caught up.

The test called `heal()` and waited, on the assumption that healing stops the
disturbances. It does not: `heal` repairs what is broken *now*, while the
schedule carries on injecting. So `Cluster::stop_faults` and `settle` exist now,
because "let it settle and see if it converges" is only a meaningful question
once something can turn the tap off. Without that, a node crashed a moment
before the check is indistinguishable from one that never converges — which is
exactly the kind of ambiguity this project spent fourteen entries removing
everywhere else.
