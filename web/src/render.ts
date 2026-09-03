/**
 * The cluster view.
 *
 * Nodes sit in a ring; a message is a dot placed along its wire by how much of
 * its flight has elapsed. That is the whole reason `sentAt` and `arrivesAt`
 * cross the wasm boundary: with both, a message's position at any tick is an
 * interpolation, so the animation is a property of the simulated clock rather
 * than of the frame rate. Slow the simulation down and the dots slow down with
 * it.
 *
 * Everything here is drawn in logical (CSS) pixels. The caller has already
 * scaled the context by devicePixelRatio, so `Size` is the logical box and not
 * the backing store.
 */

import type { MessageView, NodeView, StateView } from "./types";

export interface Size {
  w: number;
  h: number;
}

const INK = "#e8eef5";
const INK_2 = "#94a2b3";
const INK_3 = "#5d6a79";
const ACCENT = "#7de3ff";
const DOWN = "#ff5470";

const ROLE: Record<NodeView["role"], string> = {
  follower: "#55647a",
  candidate: "#f5b544",
  leader: "#37d99a",
};

/** One colour per RPC, so a glance at the wires tells you what phase it is in. */
const MSG_COLOR: Record<MessageView["kind"], string> = {
  RequestVote: "#f5b544",
  RequestVoteResp: "#a8823a",
  AppendEntries: "#6aa6f0",
  AppendEntriesResp: "#3f6ba8",
  InstallSnapshot: "#a97cf0",
  InstallSnapshotResp: "#6f4fa0",
};

const SANS = '600 %spx "Space Grotesk", ui-sans-serif, system-ui, sans-serif';
const MONO = '%spx "JetBrains Mono", ui-monospace, SFMono-Regular, monospace';

export interface Layout {
  cx: number;
  cy: number;
  rx: number;
  ry: number;
  nodeRadius: number;
}

export function layoutFor(size: Size, count: number): Layout {
  const cx = size.w / 2;
  const cy = size.h / 2;
  // An ellipse rather than a circle: the panel is much wider than it is tall,
  // and a circle inscribed in it leaves two large dead margins either side.
  const rx = Math.min(size.w * 0.37, size.h * 0.92);
  const ry = size.h * 0.305;
  // Shrink the circles as the ring gets crowded, so seven nodes do not overlap.
  const spacing = Math.min(rx, ry);
  const nodeRadius = Math.max(20, Math.min(38, (2 * Math.PI * spacing) / (count * 2.7)));
  return { cx, cy, rx, ry, nodeRadius };
}

export function nodePosition(layout: Layout, index: number, count: number) {
  // Start at the top and go clockwise, so node 0 is always in the same place
  // and the eye can track it between runs.
  const angle = -Math.PI / 2 + (index / count) * Math.PI * 2;
  return {
    x: layout.cx + Math.cos(angle) * layout.rx,
    y: layout.cy + Math.sin(angle) * layout.ry,
  };
}

/** Which node is under this canvas point, if any. */
export function nodeAt(size: Size, state: StateView, x: number, y: number): number | null {
  const count = state.nodes.length;
  const layout = layoutFor(size, count);
  for (let i = 0; i < count; i++) {
    const p = nodePosition(layout, i, count);
    if (Math.hypot(p.x - x, p.y - y) <= layout.nodeRadius) return state.nodes[i].id;
  }
  return null;
}

/** Node ids whose centres fall inside this rectangle. */
export function nodesInBox(
  size: Size,
  state: StateView,
  box: { x0: number; y0: number; x1: number; y1: number },
): number[] {
  const count = state.nodes.length;
  const layout = layoutFor(size, count);
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
  size: Size,
  state: StateView,
  overlay: Overlay = { selected: null, box: null },
) {
  const count = state.nodes.length;
  const layout = layoutFor(size, count);
  const pos = state.nodes.map((_, i) => nodePosition(layout, i, count));
  const indexOf = new Map(state.nodes.map((n, i) => [n.id, i]));

  ctx.clearRect(0, 0, size.w, size.h);
  drawBackdrop(ctx, size, layout);
  drawWires(ctx, state, pos);
  drawMessages(ctx, state, pos, indexOf, layout);
  state.nodes.forEach((node, i) =>
    drawNode(ctx, node, pos[i], layout, size, state, overlay.selected === node.id),
  );

  if (overlay.box) {
    const b = overlay.box;
    const x = Math.min(b.x0, b.x1);
    const y = Math.min(b.y0, b.y1);
    const w = Math.abs(b.x1 - b.x0);
    const h = Math.abs(b.y1 - b.y0);
    ctx.fillStyle = "rgba(125, 227, 255, 0.06)";
    ctx.fillRect(x, y, w, h);
    ctx.strokeStyle = ACCENT;
    ctx.setLineDash([3, 4]);
    ctx.lineWidth = 1;
    ctx.strokeRect(x + 0.5, y + 0.5, w, h);
    ctx.setLineDash([]);
  }
}

/** A faint orbit and centre mark, so the ring reads as a deliberate layout. */
function drawBackdrop(ctx: CanvasRenderingContext2D, size: Size, layout: Layout) {
  ctx.save();
  ctx.strokeStyle = "rgba(148, 162, 179, 0.075)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.ellipse(layout.cx, layout.cy, layout.rx, layout.ry, 0, 0, Math.PI * 2);
  ctx.stroke();

  ctx.strokeStyle = "rgba(148, 162, 179, 0.13)";
  const t = 5;
  for (const [dx, dy] of [
    [-1, 0],
    [1, 0],
    [0, -1],
    [0, 1],
  ]) {
    ctx.beginPath();
    ctx.moveTo(layout.cx + dx * 2, layout.cy + dy * 2);
    ctx.lineTo(layout.cx + dx * t, layout.cy + dy * t);
    ctx.stroke();
  }
  ctx.restore();
  void size;
}

function drawWires(
  ctx: CanvasRenderingContext2D,
  state: StateView,
  pos: { x: number; y: number }[],
) {
  // A link is drawn broken when traffic cannot pass in either direction, so a
  // partition is visible as a gap rather than something you have to infer.
  const blocked = new Set(state.blockedLinks.map(([a, b]) => `${a}>${b}`));
  ctx.lineWidth = 1;
  for (let i = 0; i < state.nodes.length; i++) {
    for (let j = i + 1; j < state.nodes.length; j++) {
      const a = state.nodes[i].id;
      const b = state.nodes[j].id;
      const ab = blocked.has(`${a}>${b}`);
      const ba = blocked.has(`${b}>${a}`);
      if (!ab && !ba) {
        ctx.setLineDash([]);
        ctx.strokeStyle = "rgba(148, 162, 179, 0.11)";
        ctx.lineWidth = 1;
      } else if (ab && ba) {
        // A clean split. Loud, because it is the fault people come here to see.
        ctx.setLineDash([3, 7]);
        ctx.strokeStyle = "rgba(255, 84, 112, 0.62)";
        ctx.lineWidth = 1.4;
      } else {
        // One-way cuts get their own shade: they behave very differently from a
        // clean split and it helps to see which one you are looking at.
        ctx.setLineDash([2, 6]);
        ctx.strokeStyle = "rgba(245, 181, 68, 0.55)";
        ctx.lineWidth = 1.2;
      }
      ctx.beginPath();
      ctx.moveTo(pos[i].x, pos[i].y);
      ctx.lineTo(pos[j].x, pos[j].y);
      ctx.stroke();
    }
  }
  ctx.setLineDash([]);
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
    const color = MSG_COLOR[msg.kind] ?? "#888";
    const r = isResp ? 2.2 : 3.4;

    // A short trail behind the dot. It gives direction at a glance, which a
    // bare dot on a straight wire does not.
    const trail = Math.min(22, Math.hypot(ex - sx, ey - sy) * 0.16);
    const tx = x - (dx / len) * trail;
    const ty = y - (dy / len) * trail;
    const grad = ctx.createLinearGradient(tx, ty, x, y);
    grad.addColorStop(0, "rgba(0,0,0,0)");
    grad.addColorStop(1, color);
    ctx.strokeStyle = grad;
    ctx.globalAlpha = isResp ? 0.35 : 0.6;
    ctx.lineWidth = r * 0.9;
    ctx.lineCap = "round";
    ctx.beginPath();
    ctx.moveTo(tx, ty);
    ctx.lineTo(x, y);
    ctx.stroke();

    ctx.globalAlpha = 1;
    ctx.fillStyle = color;
    if (!isResp) {
      ctx.shadowColor = color;
      ctx.shadowBlur = 8;
    }
    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.fill();
    ctx.shadowBlur = 0;
  }
  ctx.globalAlpha = 1;
  ctx.lineCap = "butt";
}

function drawNode(
  ctx: CanvasRenderingContext2D,
  node: NodeView,
  p: { x: number; y: number },
  layout: Layout,
  size: Size,
  state: StateView,
  selected: boolean,
) {
  const r = layout.nodeRadius;
  const down = node.status !== "running";
  const color = down ? DOWN : ROLE[node.role];
  const isLeader = !down && node.role === "leader";

  if (selected) {
    ctx.beginPath();
    ctx.arc(p.x, p.y, r + 7, 0, Math.PI * 2);
    ctx.strokeStyle = ACCENT;
    ctx.setLineDash([3, 4]);
    ctx.lineWidth = 1.2;
    ctx.stroke();
    ctx.setLineDash([]);
  }

  // A leader gets a halo. It is the single most important thing on the canvas
  // and it should be findable without reading anything.
  if (isLeader) {
    const halo = ctx.createRadialGradient(p.x, p.y, r * 0.8, p.x, p.y, r * 2.1);
    halo.addColorStop(0, "rgba(55, 217, 154, 0.24)");
    halo.addColorStop(1, "rgba(55, 217, 154, 0)");
    ctx.fillStyle = halo;
    ctx.beginPath();
    ctx.arc(p.x, p.y, r * 2.1, 0, Math.PI * 2);
    ctx.fill();
  }

  // Tinted disc plus a solid ring: the role still reads as a block of colour
  // from across the room, but the node looks like an instrument rather than a
  // flat sticker.
  ctx.beginPath();
  ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
  ctx.fillStyle = down ? "rgba(255, 84, 112, 0.07)" : hexA(color, 0.17);
  ctx.fill();
  ctx.fillStyle = "rgba(6, 8, 10, 0.55)";
  ctx.fill();
  ctx.lineWidth = isLeader ? 2.6 : 1.6;
  ctx.strokeStyle = down ? "rgba(255, 84, 112, 0.65)" : color;
  ctx.stroke();

  if (down) {
    // A crashed or paused node is struck through, so its state is obvious even
    // at a glance across the whole ring.
    ctx.save();
    ctx.beginPath();
    ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
    ctx.clip();
    ctx.strokeStyle = "rgba(255, 84, 112, 0.22)";
    ctx.lineWidth = 1;
    for (let o = -r * 2; o < r * 2; o += 6) {
      ctx.beginPath();
      ctx.moveTo(p.x + o, p.y - r);
      ctx.lineTo(p.x + o + r * 2, p.y + r);
      ctx.stroke();
    }
    ctx.restore();
  }

  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillStyle = down ? "rgba(232, 238, 245, 0.45)" : INK;
  ctx.font = SANS.replace("%s", String(Math.round(r * 0.62)));
  ctx.fillText(String(node.id), p.x, p.y - r * 0.14);
  ctx.font = MONO.replace("%s", String(Math.round(r * 0.32)));
  ctx.fillStyle = down ? "rgba(255, 84, 112, 0.75)" : hexA(color, 0.92);
  ctx.fillText(down ? node.status : `term ${node.term}`, p.x, p.y + r * 0.42);

  // Commit index and applied index just outside the circle. Pushed away from
  // the ring's centre so the labels never collide with the wires.
  ctx.font = MONO.replace("%s", "10.5");
  if (!down) {
    // On a phone the ring is narrow enough that two full labels meet in the
    // middle, so the same fact is written shorter rather than written over.
    const label =
      size.w < 560
        ? `c${node.commitIndex}/l${node.lastIndex}`
        : `commit ${node.commitIndex} \u00b7 log ${node.lastIndex}`;
    const outward = Math.atan2(p.y - layout.cy, p.x - layout.cx);
    const dirX = Math.cos(outward);
    let lx = p.x + dirX * (r + 13);
    let ly = p.y + Math.sin(outward) * (r + 15);
    // Anchor the label away from the ring rather than centring it, or a node on
    // the left or right of the circle has its own text drawn across it.
    let align: CanvasTextAlign = dirX > 0.3 ? "left" : dirX < -0.3 ? "right" : "center";
    // ...but a node near an edge would then have its label run off the canvas.
    // Measure rather than guess: the string length varies with the indices, and
    // a guess that is wrong by a few characters clips it at exactly the moment
    // the numbers get interesting.
    const w = ctx.measureText(label).width;
    const left = (x: number) => (align === "left" ? x : align === "right" ? x - w : x - w / 2);
    if (left(lx) < 5) {
      align = "left";
      lx = p.x + r + 11;
    } else if (left(lx) + w > size.w - 5) {
      align = "right";
      lx = p.x - r - 11;
    }
    // If flipping did not help either (a very narrow canvas), centre it and let
    // the clamp below keep it on screen.
    if (left(lx) < 5 || left(lx) + w > size.w - 5) {
      align = "center";
      lx = Math.max(w / 2 + 5, Math.min(size.w - w / 2 - 5, p.x));
      ly = p.y + (p.y < layout.cy ? -(r + 15) : r + 15);
    }
    ly = Math.max(11, Math.min(size.h - 11, ly));

    ctx.textAlign = align;
    ctx.fillStyle = isLeader ? INK_2 : INK_3;
    ctx.fillText(label, lx, ly);
    if (node.isJoint) {
      ctx.fillStyle = "#a97cf0";
      ctx.fillText("joint config", lx, Math.min(size.h - 3, ly + 13));
    }
  }
  ctx.textAlign = "center";
  void state;
  void INK_2;
}

/** `#rrggbb` plus an alpha, since the palette is written as hex. */
function hexA(hex: string, alpha: number): string {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
}
