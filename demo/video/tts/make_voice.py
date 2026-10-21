"""Generate the per-scene voiceover + word timings for the demo video.

    python demo/video/tts/make_voice.py

Writes demo/video/public/audio/scene-<n>.mp3 and demo/video/src/data/voice.json:
    [{scene, file, durationMs, onsetMs, words: [{text, startMs, endMs}]}]
Durations are measured by decoding the MP3 with ffmpeg (imageio-ffmpeg), and the speech onset
with silencedetect, so captions line up with the audio Remotion actually plays.
"""
import asyncio
import json
import re
import subprocess
import sys
from pathlib import Path

import edge_tts
import imageio_ffmpeg

VOICE = "en-US-AndrewNeural"
RATE = "+3%"
ROOT = Path(__file__).resolve().parents[1]
AUDIO = ROOT / "public" / "audio"
DATA = ROOT / "src" / "data"

SCENES = [
    "Every fifteen seconds, Boston's transit agency publishes where its buses and trains are. "
    "This project turns that live feed into an answer: which routes are chronically late, and when?",
    "A poller streams every vehicle and prediction into Kafka: about a hundred and fifty events a second.",
    "Spark Structured Streaming lands it in Apache Iceberg, and joins each prediction to the timetable, "
    "with a fifteen-minute watermark so late data still counts.",
    "dbt builds the gold layer. Route thirty-seven runs fourteen minutes late on average, "
    "and buses are latest in the evening rush.",
    "Airflow refreshes it every fifteen minutes, behind thirty-nine data-quality gates "
    "that turn the pipeline red on bad data.",
    "Grafana tracks throughput, lag and freshness. Pause the poller, and the lag alert fires within minutes.",
    "Load-tested at ten thousand events a second, nine-second p99 latency, and deployed once to Kubernetes.",
    "Transit Lakehouse. The code is on GitHub.",
]
FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()


async def synth(text: str, path: Path) -> list[dict]:
    words = []
    comm = edge_tts.Communicate(text, VOICE, rate=RATE, boundary="WordBoundary")
    with open(path, "wb") as f:
        async for chunk in comm.stream():
            if chunk["type"] == "audio":
                f.write(chunk["data"])
            elif chunk["type"] == "WordBoundary":
                start = chunk["offset"] / 10_000  # 100-ns ticks -> ms
                words.append({"text": chunk["text"], "startMs": round(start),
                              "endMs": round(start + chunk["duration"] / 10_000)})
    return words


def probe(path: Path) -> tuple[int, int]:
    """(duration ms, speech onset ms) by decoding with ffmpeg."""
    out = subprocess.run([FFMPEG, "-hide_banner", "-i", str(path), "-af", "silencedetect=n=-40dB:d=0.05",
                          "-f", "null", "-"], capture_output=True, text=True).stderr
    t = re.findall(r"time=(\d+):(\d+):([\d.]+)", out)[-1]
    duration = round((int(t[0]) * 3600 + int(t[1]) * 60 + float(t[2])) * 1000)
    m = re.search(r"silence_start: (-?[\d.]+)\s.*?silence_end: ([\d.]+)", out, re.S)
    onset = round(float(m.group(2)) * 1000) if m and float(m.group(1)) <= 0.01 else 0
    return duration, onset


async def main() -> None:
    AUDIO.mkdir(parents=True, exist_ok=True)
    DATA.mkdir(parents=True, exist_ok=True)
    scenes = []
    for i, text in enumerate(SCENES, start=1):
        path = AUDIO / f"scene-{i}.mp3"
        words = await synth(text, path)
        duration, onset = probe(path)
        scenes.append({"scene": i, "file": f"audio/{path.name}", "durationMs": duration, "onsetMs": onset,
                       "text": text, "words": words})
        first = words[0]["startMs"] if words else 0
        print(f"scene {i}: {duration/1000:5.2f}s  onset {onset}ms  first word {first}ms  {len(words)} words")
    (DATA / "voice.json").write_text(json.dumps(scenes, indent=1))
    total = sum(s["durationMs"] for s in scenes) / 1000
    print(f"total speech {total:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
