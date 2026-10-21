import React from "react";
import { AbsoluteFill, Audio, interpolate, Sequence, staticFile, useCurrentFrame } from "remotion";
import { Captions } from "./components/Captions";
import { RouteLine } from "./components/RouteLine";
import { Answer } from "./scenes/Answer";
import { ClipScene } from "./scenes/ClipScene";
import { Drill } from "./scenes/Drill";
import { End } from "./scenes/End";
import { Numbers } from "./scenes/Numbers";
import { Title } from "./scenes/Title";
import { C, FONT } from "./theme";
import { sceneFrames, sceneStarts, VOICE, XFADE } from "./timing";

/**
 * Fades a scene in over the cross-fade window, and drifts it in slowly (+2.5% over the scene)
 * so title cards, stat cards and paused UI never sit perfectly still while the voice runs.
 */
const FadeIn: React.FC<{ children: React.ReactNode; first: boolean; len: number }> = ({ children, first, len }) => {
  const frame = useCurrentFrame();
  const opacity = first ? 1 : interpolate(frame, [0, XFADE], [0, 1], { extrapolateRight: "clamp" });
  const scale = interpolate(frame, [0, len], [1, 1.025], { extrapolateRight: "clamp" });
  return <AbsoluteFill style={{ opacity, transform: `scale(${scale})` }}>{children}</AbsoluteFill>;
};

const sceneBody = (n: number, len: number): React.ReactNode => {
  switch (n) {
    case 1:
      return <Title />;
    case 2:
      return (
        <ClipScene durationInFrames={len} shots={[
          { clip: "kafka", title: "Kafka", detail: "Redpanda console · vehicle_positions.raw", url: "localhost:8080/topics/vehicle_positions.raw", startFromSec: 2.6 },
        ]} chip="~150 events / s" />
      );
    case 3:
      return (
        <ClipScene durationInFrames={len} shares={[0.6, 0.4]} shots={[
          { clip: "spark", title: "Spark Structured Streaming", detail: "silver query · watermark + state", url: "localhost:4041/StreamingQuery", startFromSec: 4.6 },
          { clip: "minio", title: "Apache Iceberg on MinIO", detail: "silver.stop_delays partitions", url: "localhost:9001/browser/lakehouse", startFromSec: 17 },
        ]} chip="15-min watermark" />
      );
    case 4: {
      const dbtLen = Math.round(len * 0.35);
      return (
        <AbsoluteFill>
          <Sequence durationInFrames={dbtLen} layout="none">
            <ClipScene durationInFrames={dbtLen} shots={[
              { clip: "dbt", title: "dbt", detail: "lineage: silver → gold", url: "localhost:8085/#!/overview", startFromSec: 1.2 },
            ]} />
          </Sequence>
          <Sequence from={dbtLen} layout="none"><Answer /></Sequence>
        </AbsoluteFill>
      );
    }
    case 5:
      return (
        <ClipScene durationInFrames={len} shots={[
          { clip: "airflow", title: "Airflow", detail: "transit_lakehouse_pipeline · every 15 min", url: "localhost:8081/dags/transit_lakehouse_pipeline/grid", startFromSec: 9 },
        ]} chip="24 dbt tests + 15 Great Expectations checks" />
      );
    case 6: {
      const grafLen = Math.round(len * 0.55);
      return (
        <AbsoluteFill>
          <Sequence durationInFrames={grafLen} layout="none">
            <ClipScene durationInFrames={grafLen} shots={[
              { clip: "grafana", title: "Grafana", detail: "throughput · lag · freshness · DAG health", url: "localhost:3000/d/transit-lakehouse" },
            ]} />
          </Sequence>
          <Sequence from={grafLen} layout="none"><Drill durationInFrames={len - grafLen} /></Sequence>
        </AbsoluteFill>
      );
    }
    case 7:
      return <Numbers />;
    default:
      return <End />;
  }
};

export const TransitDemo: React.FC = () => (
  <AbsoluteFill style={{ background: C.bg, fontFamily: FONT }}>
    {VOICE.map((v, i) => (
      <Sequence key={v.scene} from={sceneStarts[i]} durationInFrames={sceneFrames[i]} name={`scene ${v.scene}`}>
        <FadeIn first={i === 0} len={sceneFrames[i]}>{sceneBody(v.scene, sceneFrames[i])}</FadeIn>
        <Audio src={staticFile(v.file)} />
        {v.scene !== 1 && v.scene !== 8 && <Captions words={v.words} />}
      </Sequence>
    ))}
    <RouteLine />
  </AbsoluteFill>
);
