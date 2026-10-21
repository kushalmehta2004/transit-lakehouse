# Demo clip recorder

This records real-UI clips of the running stack for the demo video. Clips go to `../public/clips/<clip>.mp4`, at 1920×1080, constant 30 fps, H.264 yuv420p (CRF 18).

```bash
npm install && npx playwright install chromium      # one-time; uses Playwright's bundled Chromium
node record.mjs all                                 # or: node record.mjs kafka spark minio dbt airflow grafana
node record.mjs probe dbt                           # run a clip's setup only, save .tmp/probe_dbt.png
```

**Prerequisites:**
- The compose stack must be up and settled, about 20 min after a cold start, so the silver latency panels are clean.
- The `dbt` clip also needs the dbt docs server:
  ```bash
  docker compose run --rm dbt dbt docs generate
  docker compose run -d --name dbt-docs -p 8085:8085 dbt dbt docs serve --host 0.0.0.0 --port 8085 --no-browser
  ```
  Remove it afterwards with `docker rm -f dbt-docs`.
- ffmpeg comes from the `imageio-ffmpeg` Python package.

## How it works (`recorder.mjs`)

- **Capture:** Chrome DevTools screencast (`Page.startScreencast`, JPEG q92) runs on headless Chromium. Every frame is kept with its timestamp. The screencast only emits a frame when the page repaints, so ffmpeg's concat demuxer holds each frame until the next one, then `fps=30` makes the video constant frame rate. Static moments still fill real time.
- **Cursor:** an SVG arrow with a teal click ripple is injected on every navigation (an init script) and follows real mouse events. `humanMove()` makes eased, slightly curved moves of 600–1100 ms. Scrollbars are hidden.
- **Scrolling** has two modes, because headless Chromium's screencast tears while a scroller moves:
  - **`rec.scroll(dy, seconds)`** records the scroll in *virtual time*. It pauses the screencast, scrolls in small main-thread steps, captures each step as a fully composed screenshot stamped exactly 1/30 s apart, and cuts the extra capture time from the timeline. Use it for normal DOM pages (Kafka).
  - **`rec.jump(dy)`** makes a hard cut. Canvas-heavy pages (Grafana's uPlot panels, the Spark UI's d3 charts) render corrupted frames mid-scroll, and even screenshots catch it, so the recorder pauses, jumps, waits ~1.8 s for a clean render, and resumes. The paused time is removed. The cut times are printed after each recording.
- Chromium runs with `--run-all-compositor-stages-before-draw`, so it draws only fully rasterized frames.
