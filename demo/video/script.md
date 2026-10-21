# Transit Lakehouse: 60-second demo video

**Format:** 1920×1080, 30 fps, H.264 MP4, about 60 s long.

**Voice:** edge-tts `en-US-AndrewNeural`. Captions are synced from its word timings.

**Look:** dark UI, with IBM Plex Sans for text and JetBrains Mono for data. The accent is teal (#3cb4c4), and amber (#f0a54a) is used only for lateness and delay. A thin progress "route line" with a stop dot per scene runs along the bottom edge.

Each scene runs as long as its narration, plus about 0.4 s of padding. Each clip is recorded at 15 s or more, so it always covers its scene.

| # | Scene | Narration (exact text sent to TTS) | Visual | Source |
|---|---|---|---|---|
| 1 | Title | "Every fifteen seconds, Boston's transit agency publishes where its buses and trains are. This project turns that live feed into an answer: which routes are chronically late, and when?" | Title card "Transit Lakehouse", then the question types in. Subtitle: "Live MBTA data · Kafka → Spark → Iceberg → dbt → Airflow → Grafana" | Remotion |
| 2 | Kafka | "A poller streams every vehicle and prediction into Kafka: about a hundred and fifty events a second." | Redpanda console, `vehicle_positions.raw` → Messages. The cursor opens one message, showing its route key and JSON | clip `kafka.mp4` |
| 3 | Lakehouse | "Spark Structured Streaming lands it in Apache Iceberg, and joins each prediction to the timetable, with a fifteen-minute watermark so late data still counts." | Spark silver streaming statistics (input rate, watermark gap, state rows), then MinIO: warehouse → silver → stop_delays → data partitions | clips `spark.mp4`, `minio.mp4` (split the scene about 60/40) |
| 4 | Answer | "dbt builds the gold layer. Route thirty-seven runs fourteen minutes late on average, and buses are latest in the evening rush." | The dbt lineage graph (silver → gold) for about 35%, then an animated Remotion table of the top 5 chronically-late routes plus a bar chart of bus lateness by hour, with the 6 PM bar (5.4 min) in amber | clip `dbt.mp4` + Remotion data (below) |
| 5 | Airflow | "Airflow refreshes it every fifteen minutes, behind thirty-nine data-quality gates that turn the pipeline red on bad data." | Airflow grid → graph of `transit_lakehouse_pipeline`, with the cursor hovering a green run. Overlay chip: "24 dbt tests + 15 Great Expectations checks" | clip `airflow.mp4` |
| 6 | Observe | "Grafana tracks throughput, lag and freshness. Pause the poller, and the lag alert fires within minutes." | The live Grafana dashboard scrolling for about 55%, then the real drill screenshot `docs/demo/img/grafana-drill-poller-paused.png` with a slow zoom on "Bronze lag 8.47 mins" and "Alerts firing 2" | clip `grafana.mp4` + image |
| 7 | Numbers | "Load-tested at ten thousand events a second, nine-second p99 latency, and deployed once to Kubernetes." | Stat cards animate in: **10,000 ev/s** sustained · **9.9 s** p99 ingest → lakehouse · **39** quality gates · **99.6%** within 60 s of GPS. Behind them, dimmed `kubectl get pods` text (all Running) from `k8s/results/deploy-final.txt` | Remotion |
| 8 | End | "Transit Lakehouse. The code is on GitHub." | End card: "Transit Lakehouse", github.com/kushalmehta2004/transit-lakehouse | Remotion |

## Data for scene 4 (real, from `docs/demo/query-outputs.txt`)

**Top chronically-late routes** (route · avg late · share over 5 min · worst hour):
- 37 · 14.1 min · 78% · 5 PM
- 131 · 9.0 min · 70% · 6 PM
- 217 · 8.6 min · 37% · 4 PM
- 36 · 8.0 min · 55% · 1 AM
- 505 · 7.9 min · 55% · 5 PM

**Bus average minutes late, by hour:**

| Hour | 12p | 1p | 2p | 3p | 4p | 5p | **6p** | 7p | 8p | 9p | 10p | 11p | 12a | 1a |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Minutes late | 3.3 | 3.2 | 3.6 | 4.2 | 4.5 | 4.3 | **5.4** (peak) | 4.0 | 3.3 | 2.8 | 2.1 | 2.2 | 1.5 | 2.9 |
