import React from "react";
import { useCurrentFrame } from "remotion";
import { C, FONT } from "../theme";
import { sceneStarts, TOTAL_FRAMES } from "../timing";

const STOPS = ["Question", "Kafka", "Lakehouse", "Answer", "Airflow", "Observe", "Numbers", "GitHub"];

/** Bottom progress bar drawn as a transit line: one stop per scene, filled as scenes pass. */
export const RouteLine: React.FC = () => {
  const frame = useCurrentFrame();
  const left = 120;
  const width = 1680;
  const y = 1046;
  const progress = Math.min(1, frame / (TOTAL_FRAMES - 1));
  return (
    <div style={{ position: "absolute", left: 0, top: 0, width: 1920, height: 1080, pointerEvents: "none" }}>
      <div style={{ position: "absolute", left, top: y, width, height: 4, borderRadius: 2, background: C.rule }} />
      <div style={{ position: "absolute", left, top: y, width: width * progress, height: 4, borderRadius: 2, background: C.teal }} />
      {sceneStarts.map((s, i) => {
        const x = left + (width * s) / (TOTAL_FRAMES - 1);
        const reached = frame >= s;
        return (
          <div key={i}>
            <div
              style={{
                position: "absolute", left: x - 7, top: y - 5, width: 14, height: 14, borderRadius: 7,
                background: reached ? C.teal : C.bg, border: `3px solid ${reached ? C.teal : C.rule}`, boxSizing: "border-box",
              }}
            />
            <div
              style={{
                position: "absolute", left: x - 60, top: y + 12, width: 120, textAlign: "center",
                fontFamily: FONT, fontSize: 13, letterSpacing: 0.5, color: reached ? C.ink : C.muted,
              }}
            >
              {STOPS[i]}
            </div>
          </div>
        );
      })}
    </div>
  );
};
