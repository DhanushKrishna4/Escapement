/**
 * The cluster view.
 *
 * Nodes sit in a ring; a message is a dot placed along its wire by how much of
 * its flight has elapsed. That is the whole reason `sentAt` and `arrivesAt`
 * cross the wasm boundary: with both, a message's position at any tick is an
 * interpolation, so the animation is a property of the simulated clock rather
 * than of the frame rate. Slow the simulation down and the dots slow down with
 * it.
 */

import type { MessageView, NodeView, StateView } from "./types";

const ROLE_FILL: Record<NodeView["role"], string> = {
  follower: "#5a6478",
  candidate: "#d8a13a",
  leader: "#4a9d6b",
};

/** One colour per RPC, so a glance at the wires tells you what phase it is in. */
const MSG_COLOR: Record<MessageView["kind"], string> = {
  RequestVote: "#d8a13a",
  RequestVoteResp: "#a8823a",
  AppendEntries: "#6a8dd6",
  AppendEntriesResp: "#4a6699",
  InstallSnapshot: "#a76ad6",
  InstallSnapshotResp: "#7d4a99",
};

export interface Layout {
  cx: number;
  cy: number;
  radius: number;
  nodeRadius: number;
}

export function layoutFor(canvas: HTMLCanvasElement, count: number): Layout {
  const cx = canvas.width / 2;
  const cy = canvas.height / 2;
  const radius = Math.min(canvas.width, canvas.height) * 0.34;
  // Shrink the circles as the ring gets crowded, so seven nodes do not overlap.
  const nodeRadius = Math.min(42, (2 * Math.PI * radius) / (count * 2.6));
  return { cx, cy, radius, nodeRadius };
}

export function nodePosition(layout: Layout, index: number, count: number) {
  // Start at the top and go clockwise, so node 0 is always in the same place
  // and the eye can track it between runs.
  const angle = -Math.PI / 2 + (index / count) * Math.PI * 2;
  return {
    x: layout.cx + Math.cos(angle) * layout.radius,
    y: layout.cy + Math.sin(angle) * layout.radius,
  };
}

/** Which node is under this canvas point, if any. */
export function nodeAt(
  canvas: HTMLCanvasElement,
  state: StateView,
  x: number,
  y: number,
): number | null {
  const count = state.nodes.length;
  const layout = layoutFor(canvas, count);
  for (let i = 0; i < count; i++) {
    const p = nodePosition(layout, i, count);
    if (Math.hypot(p.x - x, p.y - y) <= layout.nodeRadius) return state.nodes[i].id;
  }
  return null;
}

/** Node ids whose centres fall inside this rectangle. */
export function nodesInBox(
  canvas: HTMLCanvasElement,
  state: StateView,
  box: { x0: number; y0: number; x1: number; y1: number },
): number[] {
  const count = state.nodes.length;
  const layout = layoutFor(canvas, count);
  const lo = { x: Math.min(box.x0, box.x1), y: Math.min(box.y0, box.y1) };
  const hi = { x: Math.max(box.x0, box.x1), y: Math.max(box.y0, box.y1) };
  const out: number[] = [];
  for (let i = 0; i < count; i++) {
    const p = nodePosition(layout, i, count);
    if (p.x >= lo.x && p.x <= hi.x && p.y >= lo.y && p.y <= hi.y) out.push(state.nodes[i].id);
  }
  return out;
}

export interface Overlay {
  selected: number | null;
  box: { x0: number; y0: number; x1: number; y1: number } | null;
}

export function render(
  ctx: CanvasRenderingContext2D,
  canvas: HTMLCanvasElement,
  state: StateView,
  overlay: Overlay = { selected: null, box: null },
) {
  const count = state.nodes.length;
  const layout = layoutFor(canvas, count);
  const pos = state.nodes.map((_, i) => nodePosition(layout, i, count));
  const indexOf = new Map(state.nodes.map((n, i) => [n.id, i]));

  ctx.clearRect(0, 0, canvas.width, canvas.height);

  drawWires(ctx, state, pos, indexOf);
  drawMessages(ctx, state, pos, indexOf, layout);
  state.nodes.forEach((node, i) =>
    drawNode(ctx, node, pos[i], layout, state, overlay.selected === node.id),
  );

  if (overlay.box) {
    const b = overlay.box;
    ctx.strokeStyle = "#6a8dd6";
    ctx.setLineDash([4, 4]);
    ctx.lineWidth = 1;
    ctx.strokeRect(
      Math.min(b.x0, b.x1),
      Math.min(b.y0, b.y1),
      Math.abs(b.x1 - b.x0),
      Math.abs(b.y1 - b.y0),
    );
    ctx.setLineDash([]);
  }
}

function drawWires(
  ctx: CanvasRenderingContext2D,
  state: StateView,
  pos: { x: number; y: number }[],
  indexOf: Map<number, number>,
) {
  // A link is drawn broken when traffic cannot pass in either direction, so a
  // partition is visible as a gap rather than something you have to infer.
  const blocked = new Set(state.blockedLinks.map(([a, b]) => `${a}>${b}`));
  ctx.lineWidth = 1;
  for (let i = 0; i < state.nodes.length; i++) {
    for (let j = i + 1; j < state.nodes.length; j++) {
      const a = state.nodes[i].id;
      const b = state.nodes[j].id;
      const down = blocked.has(`${a}>${b}`) || blocked.has(`${b}>${a}`);
      const both = blocked.has(`${a}>${b}`) && blocked.has(`${b}>${a}`);
      ctx.beginPath();
      ctx.setLineDash(down ? [4, 6] : []);
      // One-way cuts get their own shade: they behave very differently from a
      // clean split and it helps to see which one you are looking at.
      ctx.strokeStyle = both ? "#54313a" : down ? "#6b5230" : "#242833";
      ctx.moveTo(pos[i].x, pos[i].y);
      ctx.lineTo(pos[j].x, pos[j].y);
      ctx.stroke();
    }
  }
  ctx.setLineDash([]);
  void indexOf;
}

function drawMessages(
  ctx: CanvasRenderingContext2D,
  state: StateView,
  pos: { x: number; y: number }[],
  indexOf: Map<number, number>,
  layout: Layout,
) {
  for (const msg of state.inFlight) {
    const from = indexOf.get(msg.from);
    const to = indexOf.get(msg.to);
    if (from === undefined || to === undefined) continue;

    const span = Math.max(1, msg.arrivesAt - msg.sentAt);
    const progress = Math.min(1, Math.max(0, (state.tick - msg.sentAt) / span));
    // Nudge the endpoints to the circle edges so dots emerge from a node rather
    // than from behind it.
    const a = pos[from];
    const b = pos[to];
    const dx = b.x - a.x;
    const dy = b.y - a.y;
    const len = Math.hypot(dx, dy) || 1;
    const inset = layout.nodeRadius + 3;
    const sx = a.x + (dx / len) * inset;
    const sy = a.y + (dy / len) * inset;
    const ex = b.x - (dx / len) * inset;
    const ey = b.y - (dy / len) * inset;

    const x = sx + (ex - sx) * progress;
    const y = sy + (ey - sy) * progress;

    // Responses are drawn smaller: the flow of requests is the part worth
    // following, and acknowledgements would otherwise dominate the picture.
    const isResp = msg.kind.endsWith("Resp");
    ctx.beginPath();
    ctx.fillStyle = MSG_COLOR[msg.kind] ?? "#888";
    ctx.globalAlpha = isResp ? 0.55 : 0.95;
    ctx.arc(x, y, isResp ? 2.5 : 4, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalAlpha = 1;
  }
}

function drawNode(
  ctx: CanvasRenderingContext2D,
  node: NodeView,
  p: { x: number; y: number },
  layout: Layout,
  state: StateView,
  selected: boolean,
) {
  const r = layout.nodeRadius;
  const down = node.status !== "running";

  if (selected) {
    ctx.beginPath();
    ctx.arc(p.x, p.y, r + 6, 0, Math.PI * 2);
    ctx.strokeStyle = "#6a8dd6";
    ctx.lineWidth = 2;
    ctx.stroke();
  }

  ctx.beginPath();
  ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
  ctx.fillStyle = down ? "#22262f" : ROLE_FILL[node.role];
  ctx.fill();
  ctx.lineWidth = node.role === "leader" ? 3 : 1.5;
  ctx.strokeStyle = down ? "#8c3b3b" : "rgba(255,255,255,0.28)";
  ctx.stroke();

  // A crashed or paused node is struck through, so its state is obvious even
  // at a glance across the whole ring.
  if (down) {
    ctx.beginPath();
    ctx.strokeStyle = "#8c3b3b";
    ctx.lineWidth = 2;
    ctx.moveTo(p.x - r * 0.6, p.y - r * 0.6);
    ctx.lineTo(p.x + r * 0.6, p.y + r * 0.6);
    ctx.stroke();
  }

  ctx.fillStyle = "#e6e8ee";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.font = `600 ${Math.round(r * 0.55)}px ui-sans-serif, system-ui, sans-serif`;
  ctx.fillText(String(node.id), p.x, p.y - r * 0.16);
  ctx.font = `${Math.round(r * 0.34)}px ui-monospace, SFMono-Regular, monospace`;
  ctx.fillStyle = "rgba(255,255,255,0.72)";
  ctx.fillText(`t${node.term}`, p.x, p.y + r * 0.36);

  // Commit index and applied index just outside the circle. Pushed away from
  // the ring's centre so the labels never collide with the wires.
  const outward = Math.atan2(p.y - layout.cy, p.x - layout.cx);
  const dirX = Math.cos(outward);
  const lx = p.x + dirX * (r + 12);
  const ly = p.y + Math.sin(outward) * (r + 14);
  ctx.font = "11px ui-monospace, SFMono-Regular, monospace";
  ctx.fillStyle = "#8b93a7";
  // Anchor the label away from the ring rather than centring it, or a node on
  // the left or right of the circle has its own text drawn across it.
  ctx.textAlign = dirX > 0.3 ? "left" : dirX < -0.3 ? "right" : "center";
  const label = down
    ? node.status
    : `commit ${node.commitIndex} / log ${node.lastIndex}`;
  ctx.fillText(label, lx, ly);
  ctx.textAlign = "center";
  void state;
}
