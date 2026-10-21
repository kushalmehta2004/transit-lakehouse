import voice from "./data/voice.json";
import { FPS } from "./theme";

export type Word = { text: string; startMs: number; endMs: number };
export type SceneVoice = { scene: number; file: string; durationMs: number; text: string; words: Word[] };

export const VOICE = voice as SceneVoice[];
export const PAD = 12; // frames of silence after each line
export const XFADE = 8; // frames two scenes overlap (cross-fade)

export const sceneFrames = VOICE.map((v) => Math.ceil((v.durationMs / 1000) * FPS) + PAD);

/** First frame of each scene on the global timeline (scenes overlap by XFADE). */
export const sceneStarts = sceneFrames.reduce<number[]>((acc, f, i) => {
  acc.push(i === 0 ? 0 : acc[i - 1] + sceneFrames[i - 1] - XFADE);
  return acc;
}, []);

export const TOTAL_FRAMES = sceneStarts[sceneStarts.length - 1] + sceneFrames[sceneFrames.length - 1];

/** Frame (relative to the scene) at which a word containing `needle` starts, or `fallback`. */
export const wordFrame = (scene: number, needle: string, fallback = 0): number => {
  const w = VOICE[scene - 1].words.find((x) => x.text.toLowerCase().includes(needle.toLowerCase()));
  return w ? Math.round((w.startMs / 1000) * FPS) : fallback;
};
