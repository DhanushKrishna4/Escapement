/**
 * Driving the simulator from the browser.
 *
 * The wasm module is stepped inside a requestAnimationFrame loop, a bounded
 * number of virtual ticks per frame, so the tab stays responsive: the simulator
 * never runs to completion on the main thread, it advances a little and hands
 * back a snapshot to draw.
 */

import init, { Sim } from "./pkg/raftsim.js";
import { renderLogs, logsHeightFor } from "./logs";
import { nodeAt, nodesInBox, render, type Overlay, type Size } from "./render";
import type { StateView } from "./types";

/** Ceiling on virtual ticks simulated in a single frame. */
const MAX_TICKS_PER_FRAME = 20_000;

/** Human-readable multiplier for a slider position. */
function speedLabel(step: number): string {
  const x = speedFor(step) / 30;
  return x >= 10 ? `${Math.round(x)}x` : `${x.toFixed(1)}x`;
}

/** Slider position to virtual ticks per second. */
function speedFor(step: number): number {
  // Geometric, so the slider spans 1x to ~1000x without the low end being
  // unusable. 1x is 30 ticks/second: a message with a 10-tick latency then
  // takes about a third of a second to cross, slow enough to follow.
  return Math.round(30 * Math.pow(1000, step / 30));
}

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;

const cluster = $<HTMLCanvasElement>("cluster");
const clusterCtx = cluster.getContext("2d")!;
const logs = $<HTMLCanvasElement>("logs");
const logsCtx = logs.getContext("2d")!;

/**
 * Logical (CSS-pixel) sizes of the two canvases.
 *
 * The backing stores are devicePixelRatio times larger and the contexts carry a
 * matching transform, so everything is drawn in CSS pixels and comes out sharp
 * on a retina display. Without this the whole view is visibly soft, which on a
 * page whose entire job is showing fine detail is not a small thing.
 */
let clusterSize: Size = { w: 880, h: 470 };
let logsSize: Size = { w: 880, h: 180 };

const el = {
  play: $<HTMLButtonElement>("play"),
  stepOnce: $<HTMLButtonElement>("stepOnce"),
  restart: $<HTMLButtonElement>("restart"),
  seed: $<HTMLInputElement>("seed"),
  preset: $<HTMLSelectElement>("preset"),
  nodes: $<HTMLSelectElement>("nodes"),
  network: $<HTMLSelectElement>("network"),
  faults: $<HTMLSelectElement>("faults"),
  workload: $<HTMLInputElement>("workload"),
  staleReads: $<HTMLInputElement>("staleReads"),
  speed: $<HTMLInputElement>("speed"),
  speedLabel: $<HTMLSpanElement>("speedLabel"),
  healAll: $<HTMLButtonElement>("healAll"),
  hint: $<HTMLElement>("hint"),
  key: $<HTMLInputElement>("key"),
  value: $<HTMLInputElement>("value"),
  doWrite: $<HTMLButtonElement>("doWrite"),
  doRead: $<HTMLButtonElement>("doRead"),
  lastResult: $<HTMLElement>("lastResult"),
  tick: $<HTMLElement>("tick"),
  events: $<HTMLElement>("events"),
  leader: $<HTMLElement>("leader"),
  inflight: $<HTMLElement>("inflight"),
  elections: $<HTMLElement>("elections"),
  maxterm: $<HTMLElement>("maxterm"),
  truncations: $<HTMLElement>("truncations"),
  applied: $<HTMLElement>("applied"),
  snapshots: $<HTMLElement>("snapshots"),
  reads: $<HTMLElement>("reads"),
  logRange: $<HTMLElement>("logRange"),
  kv: $<HTMLElement>("kv"),
  invariants: $<HTMLElement>("invariants"),
  checkNow: $<HTMLButtonElement>("checkNow"),
  checkResult: $<HTMLElement>("checkResult"),
  scrub: $<HTMLInputElement>("scrub"),
  scrubMax: $<HTMLElement>("scrubMax"),
  eventsList: $<HTMLElement>("events-list"),
};

type Tool = "partition" | "cut" | "crash" | "pause";

const HINTS: Record<Tool, string> = {
  partition: "pick two nodes to split them apart, or drag a box around a group",
  cut: "pick a source then a target — traffic stops one way only",
  crash: "click a node to kill it; click again to bring it back",
  pause: "click a node to freeze it for 3000 ticks, GC-pause style",
};

/** Presets from the plan, as configuration rather than prose. */
const PRESETS: Record<string, Partial<Record<string, string | boolean>>> = {
  clean: { network: "perfect", faults: "none", workload: true },
  flaky: { network: "flaky", faults: "none", workload: true },
  churn: { network: "perfect", faults: "crash_only", workload: true },
  asym: { network: "perfect", faults: "asymmetric_only", workload: true },
  everything: { network: "long_tail", faults: "aggressive", workload: true },
};

let sim: Sim | null = null;
let state: StateView | null = null;
let running = false;
let lastFrame = 0;
/** Ticks left over from the previous frame, so slow speeds still advance. */
let tickDebt = 0;
let tool: Tool = "partition";
let selected: number | null = null;
let dragBox: Overlay["box"] = null;
/** True while the scrubber is being dragged, so the loop does not fight it. */
let scrubbing = false;
let maxTick = 0;
const crashed = new Set<number>();

/**
 * Extra knobs the writeups' links need but the control bar does not show.
 *
 * They are URL-only on purpose: batch size and torn-write rate matter enormously
 * for reproducing a specific failure and mean nothing to somebody who just wants
 * to watch an election. A run carrying any of them announces itself with the
 * banner below rather than pretending to be a normal one.
 */
let extras = { bugs: [] as string[], snapshotEvery: null as number | null, torn: 0, batch: 64 };

const BUG_NAMES: Record<string, string> = {
  "commit-rule": "commits previous-term entries by replica count (§5.4.2)",
  "double-vote": "ignores votedFor (§5.2)",
  "blind-commit": "takes leaderCommit uncapped (§5.3)",
  "no-persist": "never writes currentTerm/votedFor to disk (§5.1)",
  "compaction-anchor": "index 0 still anchors after compaction (§7)",
  "no-reconcile": "snapshot and log not reconciled on recovery (§7)",
  "early-read-index": "read index captured on arrival (§6.4)",
};

/* --- canvas sizing ------------------------------------------------------- */

function fit(canvas: HTMLCanvasElement, ctx: CanvasRenderingContext2D, height: number): Size {
  // Capped: a 3x backing store on a wide canvas costs real fill rate every
  // frame and buys nothing the eye can see.
  const dpr = Math.min(2, window.devicePixelRatio || 1);
  const w = Math.max(320, Math.round(canvas.clientWidth));
  canvas.style.height = `${height}px`;
  canvas.width = Math.round(w * dpr);
  canvas.height = Math.round(height * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { w, h: height };
}

function fitCanvases() {
  const width = Math.max(320, cluster.clientWidth);
  // Keep the ring comfortable at any width without letting it dominate a tall
  // window: a wide screen gets a wide map, not a giant one.
  const clusterH = Math.round(Math.min(455, Math.max(330, width * 0.43)));
  clusterSize = fit(cluster, clusterCtx, clusterH);
  logsSize = fit(logs, logsCtx, logsHeightFor(state?.nodes.length ?? 5));
  draw();
}

/* --- url <-> controls ---------------------------------------------------- */

function readUrl() {
  const p = new URLSearchParams(location.search);
  const get = (k: string, d: string) => p.get(k) ?? d;
  el.seed.value = get("seed", "1");
  el.nodes.value = get("nodes", "5");
  el.network.value = get("net", "perfect");
  el.faults.value = get("faults", "none");
  el.workload.checked = get("load", "1") === "1";
  el.staleReads.checked = get("stale", "0") === "1";
  el.speed.value = get("speed", "6");

  const bugs = p.get("bugs");
  extras.bugs = bugs ? bugs.split(",").filter(Boolean) : [];
  const snap = p.get("snap");
  extras.snapshotEvery = snap ? Number(snap) : null;
  extras.torn = Number(p.get("torn") ?? "0");
  extras.batch = Number(p.get("batch") ?? "64");
  showBanner();
}

function showBanner() {
  const banner = document.getElementById("banner");
  if (!banner) return;
  if (extras.bugs.length === 0 && !el.staleReads.checked) {
    banner.textContent = "";
    banner.className = "";
    return;
  }
  const names = [
    ...extras.bugs.map((b) => BUG_NAMES[b] ?? b),
    ...(el.staleReads.checked ? ["reads served without a ReadIndex round (§6.4)"] : []),
  ];
  banner.className = "banner";
  banner.textContent = `This run is deliberately broken: ${names.join("; ")}. Watch the verification panel.`;
}

/**
 * Keep the address bar in step with the controls.
 *
 * The whole premise is that a run is a pure function of its configuration, so
 * the URL genuinely is the run: anyone who opens it sees the same thing, tick
 * for tick.
 */
function writeUrl() {
  const params = new URLSearchParams({
    seed: el.seed.value,
    nodes: el.nodes.value,
    net: el.network.value,
    faults: el.faults.value,
    load: el.workload.checked ? "1" : "0",
    stale: el.staleReads.checked ? "1" : "0",
    speed: el.speed.value,
  });
  // Only carried when they differ from the defaults, so an ordinary link stays
  // readable.
  if (extras.bugs.length) params.set("bugs", extras.bugs.join(","));
  if (extras.snapshotEvery !== null) params.set("snap", String(extras.snapshotEvery));
  if (extras.torn) params.set("torn", String(extras.torn));
  if (extras.batch !== 64) params.set("batch", String(extras.batch));
  history.replaceState(null, "", `${location.pathname}?${params}`);
}

function build() {
  const config = {
    nodes: Number(el.nodes.value),
    network: el.network.value,
    faults: el.faults.value,
    staleReads: el.staleReads.checked,
    bugs: extras.bugs,
    snapshotEvery: extras.snapshotEvery,
    tornStepPermille: extras.torn,
    maxEntriesPerAppend: extras.batch,
  };
  sim = new Sim(BigInt(el.seed.value || "0"), JSON.stringify(config));
  sim.setWorkload(el.workload.checked);
  tickDebt = 0;
  maxTick = 0;
  selected = null;
  crashed.clear();
  el.checkResult.className = "";
  el.checkResult.textContent = "not checked yet";
  writeUrl();
  // A handle for poking at a run from the console. The whole point of the page
  // is that a run is inspectable and reproducible; hiding the object behind a
  // module scope would be a strange place to draw the line.
  (window as unknown as { raftsim: unknown }).raftsim = sim;
  const next = JSON.parse(sim.snapshotState()) as StateView;
  logsSize = fit(logs, logsCtx, logsHeightFor(next.nodes.length));
  refresh(next);
}

function draw() {
  if (!state) return;
  render(clusterCtx, clusterSize, state, { selected, box: dragBox });
  renderLogs(logsCtx, logsSize, state);
}

/** Set a counter, flashing it when the value actually moved. */
function bump(node: HTMLElement, value: string) {
  if (node.textContent === value) return;
  node.textContent = value;
  node.classList.remove("bump");
  void node.offsetWidth;
  node.classList.add("bump");
  window.setTimeout(() => node.classList.remove("bump"), 40);
}

function refresh(next: StateView) {
  state = next;
  maxTick = Math.max(maxTick, next.tick);
  draw();

  el.tick.textContent = next.tick.toLocaleString();
  el.events.textContent = next.eventsProcessed.toLocaleString();
  el.inflight.textContent = String(next.inFlight.length);

  const leaders = next.leaders;
  el.leader.textContent = leaders.length ? leaders.map((n) => `node ${n}`).join(", ") : "none";
  el.leader.className = leaders.length ? "lead" : "none";

  bump(el.elections, String(next.stats.electionsStarted));
  bump(el.maxterm, String(next.stats.maxTerm));
  bump(el.truncations, String(next.stats.logTruncations));
  bump(el.applied, next.stats.entriesApplied.toLocaleString());
  bump(el.snapshots, String(next.stats.snapshotsTaken));
  bump(el.reads, String(next.stats.readsServed));

  renderKv(next.kv);

  if (next.nodes.length) {
    const lo = Math.min(
      ...next.nodes.map((n) => (n.log.length ? n.log[0].index : n.lastIndex + 1)),
    );
    const hi = Math.max(...next.nodes.map((n) => n.lastIndex), lo);
    el.logRange.textContent = `${lo} → ${hi}`;
  }

  const msg = el.invariants.querySelector(".msg") as HTMLElement;
  if (next.violations.length === 0) {
    el.invariants.className = "status ok";
    msg.textContent = "all invariants hold";
  } else {
    el.invariants.className = "status bad";
    msg.textContent = next.violations.slice(0, 2).join("\n\n");
  }

  if (!scrubbing) {
    el.scrub.max = String(Math.max(1, maxTick));
    el.scrub.value = String(next.tick);
    el.scrubMax.textContent = maxTick.toLocaleString();
  }
  renderTimeline();
}

/** The committed key-value state, which is what the whole algorithm is for. */
let kvSignature = "";

function renderKv(pairs: [string, string][]) {
  const signature = pairs.map(([k, v]) => `${k}=${v}`).join("\u0000");
  if (signature === kvSignature) return;
  kvSignature = signature;
  el.kv.replaceChildren(
    ...pairs.map(([k, v]) => {
      const row = document.createElement("div");
      const key = document.createElement("k");
      key.textContent = k;
      const val = document.createElement("v");
      val.textContent = v;
      row.append(key, val);
      return row;
    }),
  );
}

interface TimelineEntry {
  tick: number;
  severity: string;
  label: string;
}

let timelineSignature = "";

function renderTimeline() {
  if (!sim) return;
  const entries = JSON.parse(sim.timeline()) as TimelineEntry[];
  // Rebuilding the list every frame would fight the scroll position, so only
  // touch the DOM when it has actually changed.
  const signature = `${entries.length}:${entries.at(-1)?.tick ?? 0}`;
  if (signature === timelineSignature) return;
  timelineSignature = signature;

  const recent = entries.slice(-140).reverse();
  el.eventsList.replaceChildren(
    ...recent.map((entry) => {
      const row = document.createElement("div");
      row.className = entry.severity;
      const t = document.createElement("span");
      t.className = "t";
      t.textContent = entry.tick.toLocaleString();
      const m = document.createElement("span");
      m.className = "m";
      m.textContent = entry.label;
      row.append(t, m);
      row.addEventListener("click", () => seekTo(entry.tick));
      return row;
    }),
  );
}

function seekTo(tick: number) {
  if (!sim) return;
  setRunning(false);
  refresh(JSON.parse(sim.seek(BigInt(tick))) as StateView);
}

function frame(now: number) {
  requestAnimationFrame(frame);
  if (!sim || !running || scrubbing) {
    lastFrame = now;
    return;
  }
  // Cap the elapsed time so a tab that was backgrounded for a minute does not
  // come back and try to simulate all of it in one frame.
  const dt = Math.min(0.25, (now - lastFrame) / 1000);
  lastFrame = now;

  tickDebt += speedFor(Number(el.speed.value)) * dt;
  // And cap the work itself, independently. Frame rate and simulation rate are
  // different things: a throttled tab gives few, long frames, and without a
  // ceiling here one of them could block the main thread for seconds.
  const ticks = Math.min(MAX_TICKS_PER_FRAME, Math.floor(tickDebt));
  if (ticks <= 0) return;
  tickDebt -= ticks;

  refresh(JSON.parse(sim.step(ticks)) as StateView);
}

function setRunning(next: boolean) {
  running = next;
  el.play.textContent = running ? "Pause" : "Play";
  el.play.dataset.state = running ? "running" : "paused";
  lastFrame = performance.now();
}

function inject(fault: unknown) {
  if (!sim) return;
  sim.inject(JSON.stringify(fault));
  refresh(JSON.parse(sim.snapshotState()) as StateView);
}

function onNodeClicked(id: number) {
  switch (tool) {
    case "crash":
      if (crashed.has(id)) {
        crashed.delete(id);
        inject({ kind: "restart", node: id });
      } else {
        crashed.add(id);
        inject({ kind: "crash", node: id });
      }
      return;
    case "pause":
      inject({ kind: "pause", node: id, ticks: 3000 });
      return;
    case "partition":
    case "cut": {
      if (selected === null) {
        selected = id;
        draw();
        return;
      }
      if (selected === id) {
        selected = null;
        draw();
        return;
      }
      if (tool === "partition") {
        // Splitting exactly two nodes apart is a partition of {a} from {b}: the
        // rest of the cluster keeps talking to both.
        inject({ kind: "partition", a: [selected], b: [id] });
      } else {
        inject({ kind: "cut", from: selected, to: id });
      }
      selected = null;
      return;
    }
  }
}

function canvasPoint(ev: MouseEvent) {
  const rect = cluster.getBoundingClientRect();
  // The canvas is laid out responsively, so client pixels are not necessarily
  // the logical pixels the renderer draws in.
  return {
    x: ((ev.clientX - rect.left) / rect.width) * clusterSize.w,
    y: ((ev.clientY - rect.top) / rect.height) * clusterSize.h,
  };
}

function wireCanvas() {
  let dragStart: { x: number; y: number } | null = null;

  cluster.addEventListener("mousedown", (ev) => {
    if (!state) return;
    const p = canvasPoint(ev);
    if (nodeAt(clusterSize, state, p.x, p.y) === null) dragStart = p;
  });

  cluster.addEventListener("mousemove", (ev) => {
    if (!dragStart) return;
    const p = canvasPoint(ev);
    dragBox = { x0: dragStart.x, y0: dragStart.y, x1: p.x, y1: p.y };
    draw();
  });

  window.addEventListener("mouseup", (ev) => {
    if (!dragStart || !state) {
      dragStart = null;
      return;
    }
    const p = canvasPoint(ev);
    const box = { x0: dragStart.x, y0: dragStart.y, x1: p.x, y1: p.y };
    dragStart = null;
    dragBox = null;
    // A stray click is not a drag.
    if (Math.hypot(box.x1 - box.x0, box.y1 - box.y0) < 12) {
      draw();
      return;
    }
    const inside = nodesInBox(clusterSize, state, box);
    const outside = state.nodes.map((n) => n.id).filter((id) => !inside.includes(id));
    if (inside.length && outside.length) {
      inject({ kind: "partition", a: inside, b: outside });
    } else {
      draw();
    }
  });

  cluster.addEventListener("click", (ev) => {
    if (!state) return;
    const p = canvasPoint(ev);
    const id = nodeAt(clusterSize, state, p.x, p.y);
    if (id !== null) onNodeClicked(id);
  });
}

interface CheckResult {
  verdict: string;
  detail: string;
  operations: number;
  completed: number;
  pending: number;
  invariantsBroken: string[];
}

function runCheck() {
  if (!sim) return;
  const result = JSON.parse(sim.check()) as CheckResult;
  const summary = `${result.operations} ops (${result.completed} answered, ${result.pending} never answered)`;
  if (result.verdict === "linearizable") {
    el.checkResult.className = "ok";
    el.checkResult.textContent = `linearizable — ${summary}`;
  } else if (result.verdict === "unknown") {
    el.checkResult.className = "";
    el.checkResult.textContent = `could not determine — ${result.detail}`;
  } else {
    el.checkResult.className = "bad";
    el.checkResult.textContent = result.detail;
  }
}

function applyPreset(name: string) {
  const preset = PRESETS[name];
  if (!preset) return;
  if (typeof preset.network === "string") el.network.value = preset.network;
  if (typeof preset.faults === "string") el.faults.value = preset.faults;
  if (typeof preset.workload === "boolean") el.workload.checked = preset.workload;
  build();
}

/** Space to play, right arrow to step, R to restart — as long as you are not typing. */
function wireKeys() {
  window.addEventListener("keydown", (ev) => {
    const target = ev.target as HTMLElement | null;
    if (target && /^(INPUT|SELECT|TEXTAREA)$/.test(target.tagName)) return;
    if (ev.metaKey || ev.ctrlKey || ev.altKey) return;
    if (ev.code === "Space") {
      ev.preventDefault();
      setRunning(!running);
    } else if (ev.key === "ArrowRight") {
      ev.preventDefault();
      if (!sim) return;
      setRunning(false);
      refresh(JSON.parse(sim.stepOnce()) as StateView);
    } else if (ev.key === "r" || ev.key === "R") {
      setRunning(false);
      build();
    }
  });
}

async function main() {
  await init();
  readUrl();
  el.speedLabel.textContent = speedLabel(Number(el.speed.value));
  wireCanvas();
  wireKeys();
  fitCanvases();
  build();
  fitCanvases();

  new ResizeObserver(() => fitCanvases()).observe(cluster.parentElement as Element);

  el.play.addEventListener("click", () => setRunning(!running));
  el.stepOnce.addEventListener("click", () => {
    if (!sim) return;
    setRunning(false);
    refresh(JSON.parse(sim.stepOnce()) as StateView);
  });
  el.restart.addEventListener("click", () => {
    setRunning(false);
    build();
  });
  el.healAll.addEventListener("click", () => {
    crashed.clear();
    inject({ kind: "heal" });
  });
  el.checkNow.addEventListener("click", runCheck);

  el.doWrite.addEventListener("click", () => {
    if (!sim) return;
    sim.clientRequest(el.key.value, el.value.value);
    el.lastResult.textContent = `sent ${el.key.value} = ${el.value.value}`;
    refresh(JSON.parse(sim.snapshotState()) as StateView);
  });
  el.doRead.addEventListener("click", () => {
    if (!sim) return;
    sim.clientRead(el.key.value);
    el.lastResult.textContent = `reading ${el.key.value} through ReadIndex`;
    refresh(JSON.parse(sim.snapshotState()) as StateView);
  });

  for (const button of document.querySelectorAll<HTMLButtonElement>("button.tool")) {
    button.addEventListener("click", () => {
      document.querySelectorAll("button.tool").forEach((b) => b.classList.remove("active"));
      button.classList.add("active");
      tool = button.dataset.tool as Tool;
      selected = null;
      el.hint.textContent = HINTS[tool];
      draw();
    });
  }

  el.speed.addEventListener("input", () => {
    el.speedLabel.textContent = speedLabel(Number(el.speed.value));
    writeUrl();
  });
  el.preset.addEventListener("change", () => {
    setRunning(false);
    applyPreset(el.preset.value);
  });
  for (const control of [el.seed, el.nodes, el.network, el.faults, el.workload, el.staleReads]) {
    control.addEventListener("change", () => {
      setRunning(false);
      el.preset.value = "custom";
      showBanner();
      build();
    });
  }

  el.scrub.addEventListener("pointerdown", () => {
    scrubbing = true;
    setRunning(false);
  });
  el.scrub.addEventListener("input", () => seekTo(Number(el.scrub.value)));
  el.scrub.addEventListener("pointerup", () => {
    scrubbing = false;
  });

  requestAnimationFrame(frame);
}

main().catch((err) => {
  el.invariants.className = "status bad";
  const msg = el.invariants.querySelector(".msg") as HTMLElement;
  if (msg) msg.textContent = String(err);
});
