import React from "react";
import { AbsoluteFill, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { C, FONT, MONO } from "../theme";

export const End: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const p = spring({ frame, fps, config: { damping: 200 } });
  return (
    <AbsoluteFill style={{ background: C.bg, justifyContent: "center", alignItems: "center", fontFamily: FONT }}>
      <div style={{ display: "grid", gap: 28, justifyItems: "center", opacity: p, transform: `scale(${0.96 + 0.04 * p})` }}>
        <div style={{ fontSize: 112, fontWeight: 700, color: C.ink, letterSpacing: -2 }}>Transit Lakehouse</div>
        <div style={{ fontFamily: MONO, fontSize: 40, color: C.teal }}>github.com/kushalmehta2004/transit-lakehouse</div>
        <div style={{ fontSize: 26, color: C.muted }}>
          Redpanda · Spark Structured Streaming · Iceberg · dbt · Great Expectations · Airflow · Prometheus · Grafana · Kubernetes
        </div>
      </div>
    </AbsoluteFill>
  );
};
