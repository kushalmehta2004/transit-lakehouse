import React from "react";
import { AbsoluteFill, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { C, FONT, MONO } from "../theme";

// pod table from k8s/results/deploy-final.txt (kind cluster, all Running)
const PODS = `$ kubectl get pods -n transit-lakehouse
NAME                            READY   STATUS      RESTARTS   AGE
iceberg-catalog-db-0            1/1     Running     0          27m
minio-54d7c4b66b-7qsgq          1/1     Running     0          27m
minio-init-cfj45                0/1     Completed   0          26m
poller-769d67b4dc-wb2gl         1/1     Running     0          3m53s
redpanda-0                      1/1     Running     0          27m
spark-bronze-6bb6cc7d8b-gh9kg   1/1     Running     0          26m
spark-silver-6fd68f775-d4ltd    1/1     Running     0          26m
spark-thrift-6f5fbf68f6-kpvnv   1/1     Running     0          26m
spark-thrift-6f5fbf68f6-n9tzc   1/1     Running     0          3m4s

horizontalpodautoscaler/spark-thrift   cpu: 1%/70%   min 1   max 2   replicas 2`;

const STATS = [
  { value: "10,000", unit: "events / s", label: "sustained in load test" },
  { value: "9.9 s", unit: "p99", label: "ingest → lakehouse" },
  { value: "39", unit: "gates", label: "dbt + Great Expectations" },
  { value: "99.6%", unit: "≤ 60 s", label: "arrivals vs GPS" },
];

export const Numbers: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  return (
    <AbsoluteFill style={{ background: C.bg, fontFamily: FONT, color: C.ink }}>
      <pre style={{ position: "absolute", left: 173, top: 92, margin: 0, fontFamily: MONO, fontSize: 19, lineHeight: 1.5, color: C.muted, opacity: 0.55 }}>
        {PODS}
      </pre>
      <div style={{ position: "absolute", left: 173, top: 26, display: "flex", gap: 12, alignItems: "baseline" }}>
        <span style={{ fontSize: 24, fontWeight: 700 }}>Measured</span>
        <span style={{ fontSize: 20, color: C.muted }}>load test · latency · quality · Kubernetes (kind)</span>
      </div>
      <div style={{ position: "absolute", left: 173, right: 173, top: 620, display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 28 }}>
        {STATS.map((s, i) => {
          const p = spring({ frame: frame - 4 - i * 7, fps, config: { damping: 200 } });
          return (
            <div key={s.value} style={{ background: C.panel, border: `1px solid ${C.rule}`, borderRadius: 16, padding: "34px 30px", opacity: p, transform: `translateY(${(1 - p) * 40}px)`, display: "grid", gap: 10, boxShadow: "0 20px 50px rgba(0,0,0,0.45)" }}>
              <div style={{ fontSize: 72, fontWeight: 700, letterSpacing: -1, fontVariantNumeric: "tabular-nums" }}>{s.value}</div>
              <div style={{ fontFamily: MONO, fontSize: 22, color: C.teal }}>{s.unit}</div>
              <div style={{ fontSize: 24, color: C.muted }}>{s.label}</div>
            </div>
          );
        })}
      </div>
    </AbsoluteFill>
  );
};
