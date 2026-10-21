"""Load generator: republish historical raw events at a controlled rate.

Reads already-ingested messages from vehicle_positions.raw / trip_updates.raw (from the
earliest retained offset, looping back once it reaches the offsets that existed at start) and
produces them to the SAME topics, keyed by route_id, with:
    ingest_time  restamped to now   (so ingest -> bronze latency is measurable)
    source       "replay:<run_id>"  (so replayed rows can be measured and deleted from bronze)
event_time is left untouched, so silver drops these rows as late (outside its watermark).
That is intended: the load test targets the ingest -> Kafka -> bronze path.

Rate control: each worker process owns a disjoint set of source partitions and a token bucket
for rate / workers. JSON is not re-parsed; the two fields are rewritten with a byte regex,
so one process can push well over 10k msg/s.

    python replay.py --rate 5000 --seconds 180 --run-id ramp1-5000 [--workers 4]
Prints one JSON line with the achieved rate at the end.
"""
import argparse
import json
import multiprocessing as mp
import os
import re
import time
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, Producer, TopicPartition

TOPICS = ["vehicle_positions.raw", "trip_updates.raw"]
INGEST_RE = re.compile(rb'"ingest_time":"[^"]*"')
SOURCE_RE = re.compile(rb'"source":"[^"]*"')


def now_iso() -> bytes:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z").encode()


def worker(idx: int, parts: list[tuple[str, int, int, int]], rate: float, seconds: float, run_id: str,
           bootstrap: str, out: mp.Queue) -> None:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"replay-{run_id}-{idx}",
                         "enable.auto.commit": False, "fetch.max.bytes": 50_000_000})
    consumer.assign([TopicPartition(t, p, low) for t, p, low, _ in parts])
    stop_at = {(t, p): high for t, p, _, high in parts}
    low_of = {(t, p): low for t, p, low, _ in parts}
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "compression.type": "zstd",
                         "batch.size": 1_000_000, "queue.buffering.max.messages": 500_000,
                         "partitioner": "murmur2_random", "acks": "1"})
    source = b'"source":"replay:' + run_id.encode() + b'"'
    sent, tokens, last = 0, 0.0, time.monotonic()
    started = last
    while time.monotonic() - started < seconds:
        now = time.monotonic()
        tokens = min(tokens + (now - last) * rate, rate * 0.5 + 1)  # at most 0.5 s of burst
        last = now
        if tokens < 1:
            time.sleep(min(0.01, (1 - tokens) / rate))
            continue
        msgs = consumer.consume(num_messages=min(int(tokens), 5000), timeout=0.5)
        stamp = b'"ingest_time":"' + now_iso() + b'"'
        for m in msgs:
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError(m.error())
                continue
            key = (m.topic(), m.partition())
            if m.offset() >= stop_at[key] - 1:  # reached the pre-test end: loop to the beginning
                consumer.seek(TopicPartition(m.topic(), m.partition(), low_of[key]))
            value = SOURCE_RE.sub(source, INGEST_RE.sub(stamp, m.value(), count=1), count=1)
            while True:
                try:
                    producer.produce(m.topic(), key=m.key(), value=value)
                    break
                except BufferError:
                    producer.poll(0.05)
            sent += 1
        tokens -= len(msgs)
        producer.poll(0)
    producer.flush(60)
    consumer.close()
    out.put(sent)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, required=True, help="target messages/second (all topics)")
    ap.add_argument("--seconds", type=float, default=180)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--workers", type=int, default=max(2, min(6, os.cpu_count() or 1)))
    ap.add_argument("--vp-share", type=float, default=0.3, help="fraction of the rate for vehicle_positions")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    args = ap.parse_args()

    probe = Consumer({"bootstrap.servers": args.bootstrap, "group.id": "replay-probe"})
    parts = []
    for topic in TOPICS:
        for p in probe.list_topics(topic, timeout=10).topics[topic].partitions:
            low, high = probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high - low > 1000:
                parts.append((topic, p, max(low, OFFSET_BEGINNING if low < 0 else low), high))
    probe.close()
    # One consumer fed by both topics drains whichever has the bigger fetch backlog, so give
    # each topic its own workers and its share of the rate (live mix: ~30% vehicle positions).
    shares = {"vehicle_positions.raw": args.vp_share, "trip_updates.raw": 1 - args.vp_share}
    plan = []  # (partitions, rate)
    for topic, share in shares.items():
        topic_parts = [p for p in parts if p[0] == topic]
        n = max(1, round(args.workers * share)) if topic_parts else 0
        n = min(n, len(topic_parts))
        plan += [(topic_parts[i::n], args.rate * share / n) for i in range(n)]
    out: mp.Queue = mp.Queue()
    t0 = time.time()
    procs = [mp.Process(target=worker, args=(i, shard, rate, args.seconds, args.run_id, args.bootstrap, out))
             for i, (shard, rate) in enumerate(plan)]
    workers = len(procs)
    for p in procs:
        p.start()
    sent = sum(out.get() for _ in procs)
    for p in procs:
        p.join()
    t1 = time.time()
    print(json.dumps({"run_id": args.run_id, "target_rate": args.rate, "workers": workers, "sent": sent,
                      "start": t0, "end": t1, "achieved_rate": round(sent / (t1 - t0), 1)}))


if __name__ == "__main__":
    main()
