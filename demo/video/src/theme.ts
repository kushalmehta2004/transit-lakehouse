import { loadFont as loadPlex } from "@remotion/google-fonts/IBMPlexSans";
import { loadFont as loadMono } from "@remotion/google-fonts/JetBrainsMono";

const plex = loadPlex("normal", { weights: ["400", "500", "600", "700"], subsets: ["latin"] });
const mono = loadMono("normal", { weights: ["400", "600"], subsets: ["latin"] });

export const FONT = plex.fontFamily;
export const MONO = mono.fontFamily;

export const C = {
  bg: "#0e171d",
  panel: "#15222b",
  rule: "#2a3a45",
  ink: "#e6edf1",
  muted: "#9aabb6",
  teal: "#3cb4c4",
  amber: "#f0a54a", // lateness only
  good: "#4cc276",
};

export const FPS = 30;
export const W = 1920;
export const H = 1080;
