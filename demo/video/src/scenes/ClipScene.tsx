import React from "react";
import { AbsoluteFill, Sequence } from "remotion";
import { Label, Window } from "../components/Window";
import { C } from "../theme";

export type Shot = {
  clip: "kafka" | "spark" | "minio" | "dbt" | "airflow" | "grafana";
  title: string;
  detail: string;
  url: string;
  startFromSec?: number;
};

/** One or more UI clips played back to back inside the window, splitting the scene by `share`. */
export const ClipScene: React.FC<{ shots: Shot[]; shares?: number[]; durationInFrames: number; chip?: string }> = ({
  shots, shares, durationInFrames, chip,
}) => {
  const parts = shares ?? shots.map(() => 1 / shots.length);
  let from = 0;
  return (
    <AbsoluteFill style={{ background: C.bg }}>
      {shots.map((s, i) => {
        const len = i === shots.length - 1 ? durationInFrames - from : Math.round(durationInFrames * parts[i]);
        const seq = (
          <Sequence key={s.clip} from={from} durationInFrames={len} layout="none">
            <Label title={s.title} detail={s.detail} />
            <Window clip={s.clip} url={s.url} startFromSec={s.startFromSec} />
          </Sequence>
        );
        from += len;
        return seq;
      })}
      {chip && <Chip text={chip} />}
    </AbsoluteFill>
  );
};

export const Chip: React.FC<{ text: string }> = ({ text }) => (
  <div
    style={{
      position: "absolute", right: 173, top: 22, padding: "8px 16px", borderRadius: 999,
      background: "rgba(60,180,196,0.16)", border: `1px solid ${C.teal}`, color: C.teal,
      fontFamily: "inherit", fontSize: 20, fontWeight: 600,
    }}
  >
    {text}
  </div>
);
