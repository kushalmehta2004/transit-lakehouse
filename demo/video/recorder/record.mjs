// Record demo clips of the running stack.
//
//   node record.mjs <clip> [<clip> ...]     record clips into ../public/clips/<clip>.mp4
//   node record.mjs probe <clip>            run only the clip's setup and save a screenshot
//   clips: kafka spark minio dbt airflow grafana   (or "all")
//
// Each clip: setup() runs before recording (logins, waiting for data), act() runs while
// recording, then the clip is held until MIN_SECONDS.
import { chromium } from "playwright";
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  H, W, Recorder, ensureCursor, humanClick, humanMove, moveTo, preparePage,
} from "./recorder.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT = join(HERE, "..", "public", "clips");
const TMP = join(HERE, ".tmp");
const MIN_SECONDS = 16;

const CLIPS = {
  kafka: {
    async setup(page) {
      await page.goto("http://localhost:8080/topics/vehicle_positions.raw", { waitUntil: "networkidle" });
      await page.getByText("10/", { exact: false }).first().waitFor({ timeout: 30000 }); // message timestamps
      await page.waitForTimeout(2500);
    },
    async act(page, rec) {
      await page.waitForTimeout(1200);
      // reload the newest 50 messages: shows the topic is live
      await humanMove(page, 1729, 344, 1000);
      await page.waitForTimeout(250);
      await page.mouse.down(); await page.waitForTimeout(90); await page.mouse.up();
      await page.waitForTimeout(1800);
      const rows = page.locator("tbody tr");
      await moveTo(page, rows.nth(0), 900, -300, 0);
      await page.waitForTimeout(600);
      await humanMove(page, page.__mouse.x, page.__mouse.y + 120, 700);
      await page.waitForTimeout(500);
      // expand the second message to show its JSON payload
      await humanClick(page, rows.nth(1).locator("td").first(), 800);
      await page.waitForTimeout(1200);
      await humanMove(page, 900, 620, 900);
      await page.waitForTimeout(1200);
      await rec.scroll(280, 1.8);
      await page.waitForTimeout(1500);
    },
  },

  spark: {
    async setup(page) {
      // The Spark UI is narrow: zoom it to fill the frame from the first paint (applying it later
      // produces a half-zoomed frame).
      await page.context().addInitScript(() => {
        const z = () => { document.body.style.zoom = "1.3"; };
        if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", z); else z();
      });
      await page.goto("http://localhost:4041/StreamingQuery/", { waitUntil: "networkidle" });
      await page.locator("a[href*='statistics']").first().waitFor();
      await page.waitForTimeout(800);
    },
    async act(page, rec) {
      await page.waitForTimeout(1200);
      await humanClick(page, page.locator("a[href*='statistics']").first(), 900);
      await page.waitForURL(/statistics/);
      await page.waitForLoadState("networkidle");
      await ensureCursor(page);
      await page.waitForTimeout(1800);
      await humanMove(page, 760, 300, 900);              // input / process rate timelines
      await page.waitForTimeout(700);
      await humanMove(page, 860, 560, 1000);             // batch duration
      await page.waitForTimeout(1400);
      await rec.jump(1080);                              // cut: watermark gap + state rows
      await humanMove(page, 760, 360, 900);
      await page.waitForTimeout(800);
      await humanMove(page, 820, 620, 900);
      await page.waitForTimeout(1800);
    },
  },

  minio: {
    async setup(page) {
      await page.goto("http://localhost:9001/login", { waitUntil: "networkidle" });
      await page.fill("#accessKey", "minioadmin");
      await page.fill("#secretKey", "minioadmin");
      await page.keyboard.press("Enter");
      await page.waitForURL(/browser/, { timeout: 30000 });
      await page.goto("http://localhost:9001/browser/lakehouse", { waitUntil: "networkidle" });
      await page.getByText("warehouse", { exact: true }).first().waitFor();
      await page.waitForTimeout(1500);
    },
    async act(page, rec) {
      await page.waitForTimeout(1200);
      for (const folder of ["warehouse", "silver", "stop_delays", "data"]) {
        const target = page.getByText(folder, { exact: true }).first();
        await target.waitFor();
        await page.waitForTimeout(700);
        await humanClick(page, target, 850);
        await page.waitForTimeout(1500);
        await ensureCursor(page);
      }
      await page.getByText(/service_date=/).first().waitFor();
      await moveTo(page, page.getByText(/service_date=/).first(), 900);
      await page.waitForTimeout(800);
      await humanMove(page, page.__mouse.x, page.__mouse.y + 80, 800);
      await page.waitForTimeout(1500);
    },
  },

  dbt: {
    async setup(page) {
      await page.goto("http://localhost:8085/#!/overview?g_v=1", { waitUntil: "networkidle" });
      await page.getByText("Lineage Graph").first().waitFor({ timeout: 30000 });
      await page.waitForTimeout(3000);
    },
    async act(page, rec) {
      // node centres (lineage canvas, measured with `probe dbt` at 1920x1080)
      const N = JSON.parse(process.env.DBT_NODES || "null") || {
        silver: [316, 586], stg: [702, 586], hourly: [1121, 586], dim: [1121, 488], chronic: [1574, 537],
      };
      await page.waitForTimeout(1200);
      for (const [key, ms, hold] of [["silver", 1000, 1300], ["stg", 1100, 1100], ["hourly", 1100, 1300],
                                      ["dim", 800, 900], ["chronic", 1000, 1600]]) {
        await humanMove(page, N[key][0], N[key][1], ms);
        await page.waitForTimeout(hold);
      }
    },
  },

  airflow: {
    async setup(page) {
      await page.goto("http://localhost:8081/login/", { waitUntil: "networkidle" });
      await page.fill("#username", "admin");
      await page.fill("#password", "admin");
      await page.click("input[type=submit], button[type=submit]");
      await page.waitForLoadState("networkidle");
      await page.goto("http://localhost:8081/dags/transit_lakehouse_pipeline/grid", { waitUntil: "networkidle" });
      await page.getByText("dbt_run", { exact: true }).filter({ visible: true }).first().waitFor();
      await page.waitForTimeout(3500);
    },
    async act(page, rec) {
      const P = JSON.parse(process.env.AIRFLOW_POINTS || "null") || { run: [517, 462], cell: [517, 520] };
      await page.waitForTimeout(1200);
      await humanMove(page, P.run[0], P.run[1], 1000);      // hover a recent green run bar
      await page.waitForTimeout(1600);
      await humanMove(page, P.cell[0], P.cell[1], 700);
      await page.waitForTimeout(1300);
      await humanClick(page, page.getByRole("tab", { name: /Graph/ }).first(), 1000);
      await page.waitForTimeout(2500);
      await ensureCursor(page);
      const graph = page.getByText("ge_checkpoint", { exact: true }).filter({ visible: true }).last();
      await moveTo(page, page.getByText("dbt_run", { exact: true }).filter({ visible: true }).last(), 900);
      await page.waitForTimeout(700);
      await moveTo(page, graph, 1000);
      await page.waitForTimeout(700);
      await moveTo(page, page.getByText("alert_on_failure", { exact: true }).filter({ visible: true }).last(), 900);
      await page.waitForTimeout(1500);
    },
  },

  grafana: {
    async setup(page) {
      await page.goto(`http://localhost:3000/d/transit-lakehouse?orgId=1&kiosk&from=${process.env.GRAFANA_FROM || "now-10m"}&to=now`, { waitUntil: "networkidle" });
      await page.waitForTimeout(10000);
    },
    async act(page, rec) {
      await page.waitForTimeout(1800);
      await humanMove(page, 520, 330, 900);                 // over "Events / sec by topic"
      await page.waitForTimeout(400);
      await humanMove(page, 980, 340, 1600);                // sweep -> tooltip
      await page.waitForTimeout(1200);
      await humanMove(page, 960, 700, 800);
      await rec.jump(500);                                  // cut: streaming + latency rows
      await humanMove(page, 600, 300, 900);
      await humanMove(page, 860, 300, 1200);                // tooltip on Spark processing rate
      await page.waitForTimeout(1000);
      await humanMove(page, 820, 640, 900);                 // silver ingest -> landed (~16 min)
      await humanMove(page, 1040, 640, 1100);
      await page.waitForTimeout(1600);
    },
  },
};

async function main() {
  const args = process.argv.slice(2);
  const probe = args[0] === "probe";
  const names = (probe ? args.slice(1) : args);
  const list = names.includes("all") ? Object.keys(CLIPS) : names;
  if (!list.length || list.some((n) => !CLIPS[n])) {
    console.error(`usage: node record.mjs [probe] <${Object.keys(CLIPS).join("|")}|all> ...`);
    process.exit(2);
  }
  mkdirSync(OUT, { recursive: true });
  mkdirSync(TMP, { recursive: true });
  for (const name of list) {
    const browser = await chromium.launch({
      // draw only fully rasterized frames: no torn or half-painted frames while scrolling
      args: ["--run-all-compositor-stages-before-draw", "--disable-checker-imaging"],
    });
    const context = await browser.newContext({ viewport: { width: W, height: H }, deviceScaleFactor: 1 });
    const page = await preparePage(context);
    try {
      await CLIPS[name].setup(page);
      await ensureCursor(page);
      if (probe) {
        const shot = join(TMP, `probe_${name}.png`);
        await page.screenshot({ path: shot });
        console.log(`probe ${name}: ${shot}`);
        continue;
      }
      const rec = new Recorder(page, name, OUT, TMP);
      await rec.start();
      await CLIPS[name].act(page, rec);
      await rec.holdUntil(MIN_SECONDS);
      const res = await rec.stop();
      const cuts = (rec.cuts || []).map((c) => c.toFixed(1) + " s").join(", ");
      console.log(`${name}: ${res.seconds.toFixed(1)} s, ${res.frames} source frames${cuts ? `, cuts at ${cuts}` : ""} -> ${res.file}`);
    } finally {
      await browser.close();   // one browser at a time (RAM is tight)
    }
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
