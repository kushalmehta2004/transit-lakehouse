#!/usr/bin/env bash
# Ramp the replay load and record one JSON line per step in scripts/results/<label>_steps.jsonl.
#   bash scripts/load_ramp.sh <label> "500 1000 2000 5000 10000 20000" [seconds_per_step]
# Stops ramping once a step "breaks": consumer lag grows faster than BREAK_SLOPE msg/s.
set -euo pipefail
cd "$(dirname "$0")/.."
LABEL=$1; RATES=$2; SECONDS_PER_STEP=${3:-180}; BREAK_SLOPE=${BREAK_SLOPE:-200}
OUT=scripts/results; mkdir -p "$OUT"
STATS="$OUT/${LABEL}_docker_stats.csv"
echo "ts,name,cpu,mem" > "$STATS"
( while true; do docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null \
    | sed "s/^/$(date +%s),/"; sleep 5; done ) >> "$STATS" &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null || true' EXIT

for rate in $RATES; do
  run="${LABEL}-${rate}"
  result=$(MSYS_NO_PATHCONV=1 docker compose run --rm --no-deps -v "$(pwd -W 2>/dev/null || pwd)/scripts:/scripts" \
           poller python /scripts/replay.py --rate "$rate" --seconds "$SECONDS_PER_STEP" --run-id "$run" 2>/dev/null | tail -1)
  start=$(echo "$result" | python -c "import json,sys; print(json.load(sys.stdin)['start'])")
  end=$(echo "$result" | python -c "import json,sys; print(json.load(sys.stdin)['end'])")
  metrics=$(python scripts/measure.py "$start" "$end" "$STATS")
  line=$(python -c "import json,sys; a=json.loads(sys.argv[1]); a.update(json.loads(sys.argv[2])); print(json.dumps(a))" "$result" "$metrics")
  echo "$line" | tee -a "$OUT/${LABEL}_steps.jsonl"
  slope=$(echo "$metrics" | python -c "import json,sys; print(json.load(sys.stdin)['lag_slope_msg_per_s'])")
  if python -c "import sys; sys.exit(0 if float('$slope') > $BREAK_SLOPE else 1)"; then
    echo ">>> step $rate broke: consumer lag growing at $slope msg/s"; break
  fi
done
