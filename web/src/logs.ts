/**
 * The log view.
 *
 * One horizontal strip per node, one cell per entry, coloured by the term that
 * created it and filled in once the entry is committed.
 *
 * This is the view worth building the rest of the project for. During a
 * partition the strips visibly drift apart — same index, different colour — and
 * when the partition heals you can watch the losing side's cells get overwritten
 * in a single frame. An abstract safety property becomes something you can point
 * at.
 */

import type { NodeView, StateView } from "./types";
import type { Size } from "./render";

/**
 * Colour per term. Cycled rather than hashed: adjacent terms need to be
 * distinguishable, and a hash would occasionally put two neighbours on nearly
 * the same hue exactly when a divergence is happening.
 */
const TERM_COLORS = [
  "#6aa6f0", "#37d99a", "#f5b544", "#a97cf0", "#ff7a8a",
  "#3fc9c0", "#e07ac0", "#9bbf4a", "#5b86d6", "#d68b45",
];

const ROW_H = 26;
const RULER_H = 24;
const PAD_BOTTOM = 10;
const LABEL_W = 78;
const GAP = 2;

const MONO = '%spx "JetBrains Mono", ui-monospace, SFMono-Regular, monospace';

/** Canvas height that fits this many strips exactly. */
export function logsHeightFor(nodes: number): number {
  return RULER_H + Math.max(1, nodes) * ROW_H + PAD_BOTTOM;
}

function termColor(term: number): string {
  return TERM_COLORS[term % TERM_COLORS.length];
}

export function renderLogs(ctx: CanvasRenderingContext2D, size: Size, state: StateView) {
  ctx.clearRect(0, 0, size.w, size.h);
  const nodes = state.nodes;
  if (nodes.length === 0) return;

  // Every strip is drawn against the same index range, so a column means the
  // same log position on every node. Comparing them is the entire point, and
  // per-node scaling would make that impossible.
  const lo = Math.min(...nodes.map((n) => (n.log.length ? n.log[0].index : n.lastIndex + 1)));
  const hi = Math.max(...nodes.map((n) => n.lastIndex), lo);
  const span = Math.max(1, hi - lo + 1);
  const trackW = size.w - LABEL_W - 14;
  const cellW = Math.max(2, trackW / span - GAP);
  const pitch = cellW + GAP;

  ctx.textBaseline = "middle";

  drawRuler(ctx, size, lo, hi, span, pitch, cellW);

  nodes.forEach((node, row) => {
    const y = RULER_H + row * ROW_H;
    // A hairline between strips: with five or seven of them, the eye needs the
    // separation to track a single node across the width.
    if (row > 0) {
      ctx.fillStyle = "rgba(148, 162, 179, 0.07)";
      ctx.fillRect(10, y - 0.5, size.w - 20, 1);
    }
    drawRow(ctx, node, y, lo, pitch, cellW, state);
  });
}

/** Index ticks along the top, at a round interval that fits the width. */
function drawRuler(
  ctx: CanvasRenderingContext2D,
  size: Size,
  lo: number,
  hi: number,
  span: number,
  pitch: number,
  cellW: number,
) {
  ctx.font = MONO.replace("%s", "10");
  ctx.fillStyle = "#5d6a79";
  ctx.textAlign = "left";

  // Aim for a label roughly every 70px, snapped to 1/5/10/25/50/100/...
  const want = Math.max(1, Math.round(70 / pitch));
  const steps = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000];
  const step = steps.find((s) => s >= want) ?? steps[steps.length - 1];
  const first = Math.ceil(lo / step) * step;

  for (let i = first; i <= hi; i += step) {
    const x = LABEL_W + (i - lo) * pitch + cellW / 2;
    if (x > size.w - 12) break;
    ctx.fillStyle = "rgba(148, 162, 179, 0.16)";
    ctx.fillRect(Math.round(x), RULER_H - 7, 1, 5);
    ctx.fillStyle = "#5d6a79";
    ctx.textAlign = "center";
    ctx.fillText(String(i), x, RULER_H - 15);
  }

  ctx.textAlign = "right";
  ctx.fillStyle = "#5d6a79";
  ctx.fillText(`${span} idx`, LABEL_W - 10, RULER_H - 13);
}

function drawRow(
  ctx: CanvasRenderingContext2D,
  node: NodeView,
  y: number,
  lo: number,
  pitch: number,
  cellW: number,
  state: StateView,
) {
  const h = ROW_H - 9;
  const top = y + 4;
  const isLeader = state.leaders.includes(node.id);
  const down = node.status !== "running";

  ctx.textAlign = "right";
  ctx.font = MONO.replace("%s", "10.5");
  ctx.fillStyle = down ? "#ff5470" : isLeader ? "#37d99a" : "#94a2b3";
  ctx.fillText(`node ${node.id}`, LABEL_W - 22, top + h / 2);
  // A leader mark that does not depend on an emoji font being present.
  if (isLeader) {
    ctx.fillStyle = "#37d99a";
    ctx.beginPath();
    ctx.arc(LABEL_W - 13, top + h / 2, 2.6, 0, Math.PI * 2);
    ctx.fill();
  }

  // Anything below the log's start is inside a snapshot: it exists, it is
  // committed, and the entries themselves are gone. Drawn as a hatched band so
  // a compacted prefix does not look like a missing one.
  if (node.logStart > lo) {
    const w = (node.logStart - lo) * pitch - GAP;
    if (w > 0) {
      ctx.save();
      ctx.beginPath();
      ctx.rect(LABEL_W, top, w, h);
      ctx.clip();
      ctx.fillStyle = "rgba(169, 124, 240, 0.10)";
      ctx.fillRect(LABEL_W, top, w, h);
      ctx.strokeStyle = "rgba(169, 124, 240, 0.22)";
      ctx.lineWidth = 1;
      for (let o = -h; o < w; o += 5) {
        ctx.beginPath();
        ctx.moveTo(LABEL_W + o, top + h);
        ctx.lineTo(LABEL_W + o + h, top);
        ctx.stroke();
      }
      ctx.restore();
      if (w > 62) {
        ctx.fillStyle = "#a97cf0";
        ctx.textAlign = "center";
        ctx.font = MONO.replace("%s", "9.5");
        ctx.fillText("SNAPSHOT", LABEL_W + w / 2, top + h / 2);
      }
    }
  }

  for (const entry of node.log) {
    const x = LABEL_W + (entry.index - lo) * pitch;
    const color = termColor(entry.term);
    if (entry.committed) {
      ctx.fillStyle = color;
      ctx.fillRect(x, top, cellW, h);
      // A brighter cap on top. Purely to stop a long committed stretch reading
      // as one undifferentiated bar.
      ctx.fillStyle = "rgba(255, 255, 255, 0.16)";
      ctx.fillRect(x, top, cellW, 1);
    } else {
      // Outlined, not filled: uncommitted entries are the ones that can still
      // vanish, and that difference is the thing to watch during a partition.
      ctx.fillStyle = hexA(color, 0.13);
      ctx.fillRect(x, top, cellW, h);
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.strokeRect(x + 0.5, top + 0.5, Math.max(1, cellW - 1), h - 1);
    }
    // Configuration entries get a notch, so a membership change is findable.
    if (entry.kind === "config" && cellW >= 4) {
      ctx.fillStyle = "#06080a";
      ctx.fillRect(x + cellW / 2 - 1, top + h / 2 - 1.5, 2, 3);
    }
  }
}

function hexA(hex: string, alpha: number): string {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
}
