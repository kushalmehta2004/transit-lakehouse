// Screen recorder for demo clips: Chrome DevTools screencast -> constant-30fps H.264.
//
// The screencast only emits a frame when the page repaints, so each frame is stored with its
// timestamp and held on screen until the next one (ffmpeg concat with per-frame durations,
// then fps=30). Static stretches therefore still fill real time.
//
// A cursor overlay (SVG arrow + click ripple) is injected on every navigation and follows the
// real mouse events Playwright sends, so moves made with humanMove() show up in the video.
import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export const FFMPEG = execFileSync("python", ["-c", "import imageio_ffmpeg;print(imageio_ffmpeg.get_ffmpeg_exe())"])
  .toString().trim();

export const W = 1920;
export const H = 1080;

const CURSOR_SCRIPT = `
(() => {
  const install = () => {
    if (document.getElementById('__demo_cursor')) return;
    const style = document.createElement('style');
    style.textContent = \`
      #__demo_cursor { position: fixed; left: 0; top: 0; width: 26px; height: 26px; z-index: 2147483647;
        pointer-events: none; transform: translate(-100px,-100px); transition: none; }
      .__demo_ripple { position: fixed; z-index: 2147483646; pointer-events: none; width: 44px; height: 44px;
        margin: -22px 0 0 -22px; border-radius: 50%; border: 3px solid rgba(60,180,196,.95);
        background: rgba(60,180,196,.18); animation: __demo_rip .6s ease-out forwards; }
      @keyframes __demo_rip { from { transform: scale(.3); opacity: 1 } to { transform: scale(1.4); opacity: 0 } }
      ::-webkit-scrollbar { width: 0 !important; height: 0 !important; display: none !important; }
      html, body, * { scrollbar-width: none !important; }
    \`;
    document.documentElement.appendChild(style);
    const c = document.createElement('div');
    c.id = '__demo_cursor';
    c.innerHTML = '<svg width="26" height="26" viewBox="0 0 26 26"><path d="M3 2 L3 21 L8.2 16.4 L11.6 24 L15 22.5 L11.7 15 L19 15 Z" fill="#111" stroke="#fff" stroke-width="1.6" stroke-linejoin="round"/></svg>';
    document.documentElement.appendChild(c);
    const pos = window.__demoPos || { x: -100, y: -100 };
    c.style.transform = 'translate(' + pos.x + 'px,' + pos.y + 'px)';
    document.addEventListener('mousemove', e => {
      window.__demoPos = { x: e.clientX, y: e.clientY };
      c.style.transform = 'translate(' + e.clientX + 'px,' + e.clientY + 'px)';
    }, true);
    document.addEventListener('mousedown', e => {
      const r = document.createElement('div');
      r.className = '__demo_ripple';
      r.style.left = e.clientX + 'px'; r.style.top = e.clientY + 'px';
      document.documentElement.appendChild(r);
      setTimeout(() => r.remove(), 700);
    }, true);
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', install); else install();
})();
`;

export async function preparePage(context) {
  await context.addInitScript(CURSOR_SCRIPT);
  const page = await context.newPage();
  page.__mouse = { x: W / 2, y: H / 2 };
  return page;
}

/** Re-install the overlay on the current document (SPA routes don't trigger init scripts). */
export async function ensureCursor(page) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      await page.waitForLoadState("domcontentloaded");
      await page.evaluate(CURSOR_SCRIPT);
      break;
    } catch {   // navigation in flight: the init script will reinstall it on the new document
      await page.waitForTimeout(300);
    }
  }
  await page.mouse.move(page.__mouse.x, page.__mouse.y);
}

const ease = (t) => (t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2);

/** Eased, slightly curved mouse move, like a person would make. */
export async function humanMove(page, x, y, ms = 750) {
  const from = { ...page.__mouse };
  const steps = Math.max(12, Math.round(ms / 16));
  const bow = (Math.random() - 0.5) * 0.12 * Math.hypot(x - from.x, y - from.y);
  for (let i = 1; i <= steps; i++) {
    const t = ease(i / steps);
    const arc = Math.sin(Math.PI * t) * bow;
    const nx = from.x + (x - from.x) * t + arc * 0.3;
    const ny = from.y + (y - from.y) * t - arc;
    await page.mouse.move(nx, ny);
    await page.waitForTimeout(16);
  }
  page.__mouse = { x, y };
}

export async function moveTo(page, locator, ms = 750, dx = 0, dy = 0) {
  const box = await locator.boundingBox();
  if (!box) throw new Error(`no bounding box for ${locator}`);
  await humanMove(page, box.x + box.width / 2 + dx, box.y + box.height / 2 + dy, ms);
}

export async function humanClick(page, locator, ms = 750) {
  await moveTo(page, locator, ms);
  await page.waitForTimeout(180);
  await page.mouse.down();
  await page.waitForTimeout(90);
  await page.mouse.up();
}

/**
 * Smooth scroll by small main-thread scrollBy() steps on the scroll container under the mouse.
 * (Wheel events scroll on the compositor thread, and the screencast then catches half-repainted,
 * "torn" frames; main-thread steps keep every frame consistent.) Ends with a forced repaint so
 * the held final frame is clean.
 */
const SCROLL_STEP = ({ x, y, step }) => new Promise((resolve) => {
  let el = document.elementFromPoint(x, y);
  while (el && el !== document.documentElement) {
    const st = getComputedStyle(el);
    if (/(auto|scroll)/.test(st.overflowY) && el.scrollHeight > el.clientHeight + 4) break;
    el = el.parentElement;
  }
  (el && el !== document.documentElement ? el : (document.scrollingElement || document.documentElement)).scrollBy(0, step);
  requestAnimationFrame(() => requestAnimationFrame(resolve));   // let the new position paint
});

/** Real-time smooth scroll (use Recorder.scroll() while recording: it avoids torn frames). */
export async function smoothScroll(page, dy, ms = 1500) {
  const steps = Math.max(10, Math.round(ms / 16));
  let done = 0;
  for (let i = 1; i <= steps; i++) {
    const target = Math.round(dy * ease(i / steps));
    await page.evaluate(SCROLL_STEP, { x: page.__mouse.x, y: page.__mouse.y, step: target - done });
    done = target;
  }
  await repaint(page);
}

/** Force a fresh full frame (tiny cursor wiggle + a no-op style flip). */
export async function repaint(page) {
  await page.evaluate(() => new Promise((r) => {
    document.body.style.outline = "1px solid transparent";
    requestAnimationFrame(() => requestAnimationFrame(() => { document.body.style.outline = ""; r(); }));
  }));
  await page.mouse.move(page.__mouse.x + 1, page.__mouse.y);
  await page.mouse.move(page.__mouse.x, page.__mouse.y);
}

export class Recorder {
  constructor(page, name, outDir, tmpRoot) {
    this.page = page;
    this.name = name;
    this.outFile = join(outDir, `${name}.mp4`);
    this.tmp = join(tmpRoot, `frames_${name}`);
    this.frames = [];
    this.offset = 0;      // seconds of capture time removed from the timeline (virtual-time scrolls)
    this.paused = false;
  }

  async start() {
    rmSync(this.tmp, { recursive: true, force: true });
    mkdirSync(this.tmp, { recursive: true });
    this.cdp = await this.page.context().newCDPSession(this.page);
    this.cdp.on("Page.screencastFrame", async ({ data, metadata, sessionId }) => {
      if (!this.paused) this.push(Buffer.from(data, "base64"), metadata.timestamp - this.offset);
      try { await this.cdp.send("Page.screencastFrameAck", { sessionId }); } catch { /* stopping */ }
    });
    await this.startCast();
    // nudge a repaint so the first frame is captured immediately
    await this.page.mouse.move(this.page.__mouse.x + 1, this.page.__mouse.y);
    await this.page.mouse.move(this.page.__mouse.x, this.page.__mouse.y);
    this.t0 = Date.now() / 1000;
  }

  push(buf, ts) {
    const file = join(this.tmp, `f${String(this.frames.length).padStart(5, "0")}.jpg`);
    writeFileSync(file, buf);
    this.frames.push({ file, ts });
  }

  async startCast() {
    await this.cdp.send("Page.startScreencast", { format: "jpeg", quality: 92, maxWidth: W, maxHeight: H, everyNthFrame: 1 });
  }

  /**
   * Smooth scroll recorded in virtual time. The screencast tears while a scroller moves, so the
   * screencast is paused and every step is captured as a fully composed screenshot, stamped
   * exactly 1/30 s apart. Capture takes ~85 ms/frame; that extra wall time is cut from the
   * timeline, so playback is a smooth 30 fps scroll.
   */
  async scroll(dy, seconds = 1.6) {
    const page = this.page;
    this.paused = true;
    await this.cdp.send("Page.stopScreencast");
    const wallStart = Date.now() / 1000;
    const vStart = wallStart - this.offset;
    const n = Math.max(8, Math.round(seconds * 30));
    let done = 0;
    for (let i = 1; i <= n; i++) {
      const target = Math.round(dy * ease(i / n));
      await page.evaluate(SCROLL_STEP, { x: page.__mouse.x, y: page.__mouse.y, step: target - done });
      done = target;
      const { data } = await this.cdp.send("Page.captureScreenshot", { format: "jpeg", quality: 92 });
      this.push(Buffer.from(data, "base64"), vStart + i / 30);
    }
    this.offset += (Date.now() / 1000 - wallStart) - n / 30;
    this.paused = false;
    await this.startCast();
    await repaint(page);
  }

  /**
   * Hard cut to another part of the page. Canvas-heavy pages (Grafana, the Spark UI) render
   * corrupted frames mid-scroll in headless Chromium, so instead of recording the scroll the
   * capture pauses, the page jumps, everything re-renders, and capture resumes. The paused time
   * is removed from the timeline, so the clip shows a clean cut.
   */
  async jump(dy, settleMs = 1800) {
    const page = this.page;
    this.paused = true;
    await this.cdp.send("Page.stopScreencast");
    const wallStart = Date.now() / 1000;
    const vCut = wallStart - this.offset;           // timeline position of the cut
    await page.evaluate(SCROLL_STEP, { x: page.__mouse.x, y: page.__mouse.y, step: dy });
    await page.waitForTimeout(settleMs);
    // one fully composed frame exactly at the cut, then live capture again
    const { data } = await this.cdp.send("Page.captureScreenshot", { format: "jpeg", quality: 92 });
    this.push(Buffer.from(data, "base64"), vCut + 0.001);
    this.offset += Date.now() / 1000 - wallStart;  // drop the paused time from the timeline
    this.cuts = [...(this.cuts || []), vCut - this.t0];
    this.paused = false;
    await this.startCast();
    await repaint(page);
  }

  elapsed() { return Date.now() / 1000 - this.t0 - this.offset; }

  /** Keep recording until at least `seconds` have elapsed since start (wiggle-free hold). */
  async holdUntil(seconds) {
    const left = seconds - this.elapsed();
    if (left > 0) await this.page.waitForTimeout(left * 1000);
  }

  async stop() {
    const tEnd = Date.now() / 1000 - this.offset;
    await this.cdp.send("Page.stopScreencast");
    await new Promise((r) => setTimeout(r, 300));
    if (this.frames.length === 0) throw new Error("no frames captured");
    // concat list: each frame shown until the next frame's timestamp; the clip starts at the first frame
    const lines = ["ffconcat version 1.0"];
    this.frames.forEach((f, i) => {
      const next = i + 1 < this.frames.length ? this.frames[i + 1].ts : tEnd;
      const dur = Math.max(0.001, next - f.ts);
      lines.push(`file '${f.file.replace(/\\/g, "/")}'`, `duration ${dur.toFixed(4)}`);
    });
    lines.push(`file '${this.frames.at(-1).file.replace(/\\/g, "/")}'`);
    const list = join(this.tmp, "list.txt");
    writeFileSync(list, lines.join("\n"));
    execFileSync(FFMPEG, [
      "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", list,
      "-vf", `fps=30,scale=${W}:${H}:force_original_aspect_ratio=decrease,pad=${W}:${H}:(ow-iw)/2:(oh-ih)/2:color=black,scale=out_range=tv,format=yuv420p`,
      "-c:v", "libx264", "-preset", "slow", "-crf", "18", "-r", "30", "-color_range", "tv", "-movflags", "+faststart", this.outFile,
    ]);
    const seconds = tEnd - this.frames[0].ts;
    rmSync(this.tmp, { recursive: true, force: true });
    return { file: this.outFile, frames: this.frames.length, seconds };
  }
}
