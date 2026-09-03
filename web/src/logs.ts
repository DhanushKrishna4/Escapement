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

/**
 * Colour per term. Cycled rather than hashed: adjacent terms need to be
 * distinguishable, and a hash would occasionally put two neighbours on nearly
 * the same hue exactly when a divergence is happening.
 */
const TERM_COLORS = [
  "#6a8dd6", "#4a9d6b", "#d8a13a", "#a76ad6", "#d1685b",
  "#4aa8a0", "#c07ab0", "#8a9a4a", "#5b7fb5", "#b5793f",
];

function termColor(term: number): string {
  return TERM_COLORS[term % TERM_COLORS.length];
}

export function renderLogs(
  ctx: CanvasRenderingContext2D,
  canvas: HTMLCanvasElement,
  state: StateView,
) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  const nodes = state.nodes;
  if (nodes.length === 0) return;

  const labelWidth = 74;
  const rowH = Math.min(26, (canvas.height - 26) / nodes.length);
  const cellGap = 2;

  // Every strip is drawn against the same index range, so a column means the
  // same log position on every node. Comparing them is the entire point, and
  // per-node scaling would make that impossible.
  const lo = Math.min(...nodes.map((n) => (n.log.length ? n.log[0].index : n.lastIndex + 1)));
  const hi = Math.max(...nodes.map((n) => n.lastIndex), lo);
  const span = Math.max(1, hi - lo + 1);
  const cellW = Math.max(3, (canvas.width - labelWidth - 12) / span - cellGap);

  ctx.font = "11px ui-monospace, SFMono-Regular, monospace";
  ctx.textBaseline = "middle";

  // Index ruler.
  ctx.fillStyle = "#6d768c";
  ctx.textAlign = "left";
  ctx.fillText(`index ${lo} → ${hi}`, 6, 12);

  nodes.forEach((node, row) => {
    const y = 26 + row * rowH;
    drawRow(ctx, node, y, rowH, lo, labelWidth, cellW, cellGap, state);
  });
}

function drawRow(
  ctx: CanvasRenderingContext2D,
  node: NodeView,
  y: number,
  rowH: number,
  lo: number,
  labelWidth: number,
  cellW: number,
  cellGap: number,
  state: StateView,
) {
  const h = Math.max(8, rowH - 8);
  const isLeader = state.leaders.includes(node.id);

  ctx.textAlign = "right";
  ctx.fillStyle = isLeader ? "#4a9d6b" : "#8b93a7";
  ctx.fillText(`node ${node.id}${isLeader ? " ★" : ""}`, labelWidth - 8, y + h / 2);

  // Anything below the log's start is inside a snapshot: it exists, it is
  // committed, and the entries themselves are gone. Drawn as a flat band so a
  // compacted prefix does not look like a missing one.
  if (node.logStart > lo) {
    const w = (node.logStart - lo) * (cellW + cellGap) - cellGap;
    if (w > 0) {
      ctx.fillStyle = "#2b3340";
      ctx.fillRect(labelWidth, y, w, h);
      if (w > 54) {
        ctx.fillStyle = "#6d768c";
        ctx.textAlign = "center";
        ctx.fillText("snapshot", labelWidth + w / 2, y + h / 2);
      }
    }
  }

  for (const entry of node.log) {
    const x = labelWidth + (entry.index - lo) * (cellW + cellGap);
    const color = termColor(entry.term);
    if (entry.committed) {
      ctx.fillStyle = color;
      ctx.fillRect(x, y, cellW, h);
    } else {
      // Outlined, not filled: uncommitted entries are the ones that can still
      // vanish, and that difference is the thing to watch during a partition.
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.strokeRect(x + 0.5, y + 0.5, cellW - 1, h - 1);
    }
    // Configuration entries get a notch, so a membership change is findable.
    if (entry.kind === "config" && cellW >= 5) {
      ctx.fillStyle = "#0f1116";
      ctx.fillRect(x + cellW / 2 - 1, y + h / 2 - 1, 2, 2);
    }
  }
}
