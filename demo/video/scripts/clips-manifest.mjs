// Lists recorded UI clips in public/clips into src/data/clips.json, so the composition uses a
// clip when it exists and falls back to a still screenshot when it does not.
import { existsSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const dir = join(root, "public", "clips");
const clips = existsSync(dir)
  ? readdirSync(dir).filter((f) => f.endsWith(".mp4")).map((f) => f.replace(/\.mp4$/, "")).sort()
  : [];
writeFileSync(join(root, "src", "data", "clips.json"), JSON.stringify(clips, null, 1) + "\n");
console.log(`clips: ${clips.length ? clips.join(", ") : "(none yet, using still images)"}`);
