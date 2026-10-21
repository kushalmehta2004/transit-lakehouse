import React from "react";
import { Composition } from "remotion";
import { FPS, H, W } from "./theme";
import { TOTAL_FRAMES } from "./timing";
import { TransitDemo } from "./Video";

export const RemotionRoot: React.FC = () => (
  <Composition id="TransitDemo" component={TransitDemo} durationInFrames={TOTAL_FRAMES} fps={FPS} width={W} height={H} />
);
