import React from "react";
import { AbsoluteFill, interpolate, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { C, FONT, MONO } from "../theme";
import { wordFrame } from "../timing";

const QUESTION = "Which routes are chronically late, and when?";
const STACK = "Live MBTA data · Kafka → Spark → Iceberg → dbt → Airflow → Grafana";

export const Title: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const rise = spring({ frame, fps, config: { damping: 200 } });
  const typeStart = wordFrame(1, "which", 190);
  const chars = Math.floor(interpolate(frame, [typeStart, typeStart + 40], [0, QUESTION.length], {
    extrapolateLeft: "clamp", extrapolateRight: "clamp",
  }));
  const caret = frame >= typeStart - 12 && Math.floor(frame / 15) % 2 === 0 && chars < QUESTION.length;
  return (
    <AbsoluteFill style={{ background: C.bg, justifyContent: "center", alignItems: "center" }}>
      <div style={{ display: "grid", gap: 26, justifyItems: "center", transform: `translateY(${(1 - rise) * 30}px)`, opacity: rise }}>
        <div style={{ fontFamily: MONO, fontSize: 22, letterSpacing: 4, color: C.teal, textTransform: "uppercase" }}>
          Boston · GTFS-realtime · every 15 s
        </div>
        <div style={{ fontFamily: FONT, fontSize: 132, fontWeight: 700, color: C.ink, letterSpacing: -2 }}>
          Transit Lakehouse
        </div>
        <div style={{ fontFamily: FONT, fontSize: 28, color: C.muted }}>{STACK}</div>
        <div style={{ height: 6 }} />
        <div style={{ fontFamily: FONT, fontSize: 56, fontWeight: 600, color: C.amber, minHeight: 74 }}>
          {QUESTION.slice(0, chars)}
          <span style={{ opacity: caret ? 1 : 0, color: C.ink }}>|</span>
        </div>
      </div>
    </AbsoluteFill>
  );
};
