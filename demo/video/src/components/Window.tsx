import React from "react";
import { Img, interpolate, OffthreadVideo, staticFile, useCurrentFrame } from "remotion";
import clips from "../data/clips.json";
import { C, FONT, MONO } from "../theme";

// still screenshot used until the recorded clip exists
const FALLBACK: Record<string, string> = {
  kafka: "img/kafka-messages.png",
  spark: "img/spark-silver-statistics.png",
  minio: "img/minio-silver-partitions.png",
  dbt: "img/dbt-lineage.png",
  airflow: "img/airflow-pipeline-graph.png",
  grafana: "img/grafana-ingestion.png",
};

export const WIN = { x: 173, y: 74, w: 1574, h: 886 }; // 16:9 at 0.82 scale, clear of label + captions

/** A browser-like window holding a recorded UI clip (or its still fallback). */
export const Window: React.FC<{
  clip: keyof typeof FALLBACK;
  startFromSec?: number;
  url?: string;
}> = ({ clip, startFromSec = 1, url }) => {
  const hasClip = (clips as string[]).includes(clip);
  return (
    <Frame url={url}>
      {hasClip ? (
        <OffthreadVideo
          src={staticFile(`clips/${clip}.mp4`)}
          muted
          startFrom={Math.round(startFromSec * 30)}
          style={{ width: "100%", height: "100%", objectFit: "cover" }}
        />
      ) : (
        <StillPan src={FALLBACK[clip]} />
      )}
    </Frame>
  );
};

/** Window chrome: dark title bar with an address field, rounded body with a soft shadow. */
export const Frame: React.FC<{ url?: string; children: React.ReactNode }> = ({ url, children }) => (
  <div
    style={{
      position: "absolute", left: WIN.x, top: WIN.y, width: WIN.w, height: WIN.h, borderRadius: 14, overflow: "hidden",
      background: C.panel, boxShadow: "0 30px 80px rgba(0,0,0,0.55), 0 0 0 1px rgba(255,255,255,0.06)",
    }}
  >
    <div style={{ height: 38, display: "flex", alignItems: "center", gap: 8, padding: "0 16px", background: "#1b2a34" }}>
      {["#e0605a", "#e4b14a", "#5fbf6a"].map((c) => (
        <div key={c} style={{ width: 12, height: 12, borderRadius: 6, background: c, opacity: 0.85 }} />
      ))}
      {url && (
        <div
          style={{
            marginLeft: 18, padding: "4px 14px", borderRadius: 6, background: "#0f1a21", color: C.muted,
            fontFamily: MONO, fontSize: 15,
          }}
        >
          {url}
        </div>
      )}
    </div>
    <div style={{ position: "absolute", top: 38, left: 0, right: 0, bottom: 0, overflow: "hidden" }}>{children}</div>
  </div>
);

/** Slow downward pan over a screenshot, so stills don't feel frozen. */
const StillPan: React.FC<{ src: string }> = ({ src }) => {
  const frame = useCurrentFrame();
  const y = interpolate(frame, [0, 300], [0, -6], { extrapolateRight: "clamp" });
  return (
    <Img
      src={staticFile(src)}
      style={{ width: "100%", objectFit: "cover", objectPosition: "top", transform: `translateY(${y}%) scale(1.02)` }}
    />
  );
};

/** Small component label above the window, top-left. */
export const Label: React.FC<{ title: string; detail: string }> = ({ title, detail }) => (
  <div
    style={{
      position: "absolute", left: WIN.x, top: 26, display: "flex", alignItems: "baseline", gap: 12,
      fontFamily: FONT, color: C.ink,
    }}
  >
    <span style={{ fontSize: 24, fontWeight: 700 }}>{title}</span>
    <span style={{ fontSize: 20, color: C.muted }}>{detail}</span>
  </div>
);
