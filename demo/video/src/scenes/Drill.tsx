import React from "react";
import { AbsoluteFill, Img, interpolate, staticFile, useCurrentFrame } from "remotion";
import { Frame, Label } from "../components/Window";
import { C } from "../theme";

/**
 * The real drill screenshot (poller paused 8 min). Slow zoom toward the top tiles:
 * "Events / sec: No data", "Bronze lag 8.47 mins", "Kafka consumer lag 0", "Alerts firing 2".
 */
export const Drill: React.FC<{ durationInFrames: number }> = ({ durationInFrames }) => {
  const frame = useCurrentFrame();
  const scale = interpolate(frame, [0, durationInFrames], [1, 1.06], { extrapolateRight: "clamp" });
  return (
    <AbsoluteFill style={{ background: C.bg }}>
      <Label title="Drill" detail="poller paused for 8 minutes → BronzeLagHigh fires" />
      <Frame url="localhost:3000 · Transit lakehouse pipeline">
        <Img
          src={staticFile("img/grafana-drill-poller-paused.png")}
          style={{ width: "100%", transformOrigin: "50% 0%", transform: `scale(${scale})` }}
        />
      </Frame>
    </AbsoluteFill>
  );
};
