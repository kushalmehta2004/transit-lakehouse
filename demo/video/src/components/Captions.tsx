import React from "react";
import { useCurrentFrame } from "remotion";
import { C, FONT, FPS } from "../theme";
import { Word } from "../timing";

const MAX_CHARS = 58; // per caption page (renders as at most two lines)

/** Split a scene's words into evenly sized caption pages (no orphaned two-word last page). */
const paginate = (words: Word[]): Word[][] => {
  const total = words.reduce((n, w) => n + w.text.length + 1, 0);
  const target = total / Math.ceil(total / MAX_CHARS);
  const pages: Word[][] = [];
  let cur: Word[] = [];
  let len = 0;
  for (const w of words) {
    if (cur.length && len + (w.text.length + 1) / 2 > target) {
      pages.push(cur);
      cur = [];
      len = 0;
    }
    cur.push(w);
    len += w.text.length + 1;
  }
  if (cur.length) pages.push(cur);
  return pages;
};

/** Bottom-centre captions; the word being spoken is highlighted in teal. */
export const Captions: React.FC<{ words: Word[] }> = ({ words }) => {
  const ms = (useCurrentFrame() / FPS) * 1000;
  const pages = paginate(words);
  // show the page whose first word has started (keep the last page up until the scene ends)
  let page = pages[0];
  for (const p of pages) if (p[0].startMs <= ms) page = p;
  if (!page) return null;
  return (
    <div style={{ position: "absolute", left: 0, right: 0, bottom: 58, display: "flex", justifyContent: "center" }}>
      <div
        style={{
          maxWidth: 1240, padding: "12px 26px", borderRadius: 12, background: "rgba(10,17,22,0.84)",
          fontFamily: FONT, fontSize: 38, fontWeight: 500, lineHeight: 1.3, textAlign: "center", color: C.ink,
        }}
      >
        {page.map((w, i) => {
          const active = ms >= w.startMs && ms < (page[i + 1]?.startMs ?? w.endMs + 400);
          return (
            <span key={i} style={{ color: active ? C.teal : C.ink }}>
              {w.text}
              {i < page.length - 1 ? " " : ""}
            </span>
          );
        })}
      </div>
    </div>
  );
};
