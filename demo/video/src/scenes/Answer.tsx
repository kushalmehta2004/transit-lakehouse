import React from "react";
import { AbsoluteFill, interpolate, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { C, FONT, MONO } from "../theme";

// real output of gold.chronically_late_routes / gold.route_lateness_by_hour (docs/demo/query-outputs.txt)
const ROUTES = [
  { route: "37", late: 14.1, over5: 78, worst: "5 PM" },
  { route: "131", late: 9.0, over5: 70, worst: "6 PM" },
  { route: "217", late: 8.6, over5: 37, worst: "4 PM" },
  { route: "36", late: 8.0, over5: 55, worst: "1 AM" },
  { route: "505", late: 7.9, over5: 55, worst: "5 PM" },
];
const HOURS = ["12p", "1p", "2p", "3p", "4p", "5p", "6p", "7p", "8p", "9p", "10p", "11p", "12a", "1a"];
const MINUTES = [3.3, 3.2, 3.6, 4.2, 4.5, 4.3, 5.4, 4.0, 3.3, 2.8, 2.1, 2.2, 1.5, 2.9];
const PEAK = 6; // 6 PM
const AXIS_MAX = 6; // minutes

export const Answer: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const fadeIn = interpolate(frame, [0, 12], [0, 1], { extrapolateRight: "clamp" });
  const chartH = 380;
  return (
    <AbsoluteFill style={{ background: C.bg, opacity: fadeIn, fontFamily: FONT, color: C.ink }}>
      <div style={{ position: "absolute", left: 173, top: 26, display: "flex", gap: 12, alignItems: "baseline" }}>
        <span style={{ fontSize: 24, fontWeight: 700 }}>Gold</span>
        <span style={{ fontSize: 20, color: C.muted }}>chronically_late_routes · route_lateness_by_hour</span>
      </div>

      {/* table */}
      <div style={{ position: "absolute", left: 173, top: 210, width: 720 }}>
        <div style={{ fontSize: 30, fontWeight: 600, marginBottom: 22 }}>Most chronically late routes</div>
        <div style={{ display: "grid", gridTemplateColumns: "120px 1fr 1fr 1fr", rowGap: 0 }}>
          {["Route", "Avg late", "> 5 min", "Worst hour"].map((h) => (
            <div key={h} style={{ fontFamily: MONO, fontSize: 17, letterSpacing: 1.5, color: C.muted, textTransform: "uppercase", paddingBottom: 12, borderBottom: `1px solid ${C.rule}` }}>{h}</div>
          ))}
          {ROUTES.map((r, i) => {
            const s = spring({ frame: frame - 6 - i * 5, fps, config: { damping: 200 } });
            const cell: React.CSSProperties = {
              padding: "16px 0", borderBottom: `1px solid ${C.rule}`, fontSize: 30, fontVariantNumeric: "tabular-nums",
              opacity: s, transform: `translateX(${(1 - s) * 24}px)`,
            };
            return (
              <React.Fragment key={r.route}>
                <div style={cell}>
                  <span style={{ display: "inline-block", minWidth: 76, textAlign: "center", padding: "4px 10px", borderRadius: 6, background: C.amber, color: C.bg, fontWeight: 700 }}>{r.route}</span>
                </div>
                <div style={{ ...cell, fontWeight: 600 }}>{r.late.toFixed(1)} min</div>
                <div style={cell}>{r.over5}%</div>
                <div style={{ ...cell, color: C.muted }}>{r.worst}</div>
              </React.Fragment>
            );
          })}
        </div>
      </div>

      {/* bar chart: to scale, 0..6 min */}
      <div style={{ position: "absolute", left: 1000, top: 210, width: 747 }}>
        <div style={{ fontSize: 30, fontWeight: 600, marginBottom: 6 }}>Buses: minutes late by hour</div>
        <div style={{ fontSize: 20, color: C.muted, marginBottom: 30 }}>Latest in the evening rush</div>
        <div style={{ position: "relative", height: chartH, marginLeft: 44 }}>
          {[0, 2, 4, 6].map((t) => (
            <div key={t} style={{ position: "absolute", left: 0, right: 0, bottom: (t / AXIS_MAX) * chartH, borderTop: `1px solid ${C.rule}` }}>
              <span style={{ position: "absolute", left: -40, top: -12, fontFamily: MONO, fontSize: 16, color: C.muted }}>{t}</span>
            </div>
          ))}
          <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "flex-end", gap: 10 }}>
            {MINUTES.map((m, i) => {
              const grow = spring({ frame: frame - 14 - i * 2, fps, config: { damping: 200 } });
              const peak = i === PEAK;
              return (
                <div key={i} style={{ flex: 1, position: "relative", height: (m / AXIS_MAX) * chartH * grow, background: peak ? C.amber : C.teal, borderRadius: "4px 4px 0 0" }}>
                  {(peak || i === 12) && (
                    <span style={{ position: "absolute", bottom: "100%", left: "50%", transform: "translateX(-50%)", marginBottom: 6, fontFamily: MONO, fontSize: 20, fontWeight: 600, color: peak ? C.amber : C.muted, opacity: grow }}>
                      {m.toFixed(1)}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        </div>
        <div style={{ display: "flex", gap: 10, marginLeft: 44, marginTop: 10 }}>
          {HOURS.map((h, i) => (
            <div key={h} style={{ flex: 1, textAlign: "center", fontFamily: MONO, fontSize: 15, color: i === PEAK ? C.amber : C.muted }}>{h}</div>
          ))}
        </div>
      </div>
    </AbsoluteFill>
  );
};
