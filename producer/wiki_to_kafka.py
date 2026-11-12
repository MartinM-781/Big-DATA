"""Wikimedia EventStreams (recentchange, SSE) -> Kafka.

Every event is forwarded unmodified (raw JSON) to the Kafka topic; filtering
happens in Spark. The producer reconnects with ``Last-Event-ID`` so that a
dropped HTTP connection does not lose events.

Late-data simulation (optional, for the watermark demo):
``--late-fraction 0.02 --late-delay 900`` holds back 2 % of the events for
15 minutes before sending them, *without* touching their event time, so that
Spark sees genuinely late records.
"""

import argparse
import heapq
import json
import os
import random
import sys
import time

import requests
from kafka import KafkaProducer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
CONTACT = os.environ.get("WIKI_CONTACT", "educational project")
USER_AGENT = f"ECE-BigData-IMDb-Streaming/1.0 ({CONTACT})"


def sse_events(last_id=None):
    """Yield (event_id, data) tuples from the SSE stream, forever."""
    while True:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            with requests.get(STREAM_URL, headers=headers, stream=True,
                              timeout=(10, 60)) as resp:
                resp.raise_for_status()
                event_id, data = None, []
                for raw in resp.iter_lines(decode_unicode=True):
                    if raw is None:
                        continue
                    if raw == "":
                        if data:
                            yield event_id, "\n".join(data)
                            last_id = event_id or last_id
                        event_id, data = None, []
                    elif raw.startswith("id:"):
                        event_id = raw[3:].strip()
                    elif raw.startswith("data:"):
                        data.append(raw[5:].lstrip())
        except (requests.RequestException, ValueError) as exc:
            print(f"[sse] connection lost ({exc!r}); reconnecting in 3 s",
                  flush=True)
            time.sleep(3)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap",
                        default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    parser.add_argument("--topic",
                        default=os.environ.get("KAFKA_TOPIC",
                                               "wiki.recentchange"))
    parser.add_argument("--duration", type=int, default=0,
                        help="stop after N seconds (0 = run forever)")
    parser.add_argument("--late-fraction", type=float, default=float(
        os.environ.get("LATE_FRACTION", "0")))
    parser.add_argument("--late-delay", type=int, default=int(
        os.environ.get("LATE_DELAY", "900")))
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap, acks="all", linger_ms=50,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"))
    held = []                      # min-heap of (release_time, key, value)
    sent = delayed = 0
    start = time.time()
    for _event_id, data in sse_events():
        now = time.time()
        try:
            event = json.loads(data)
        except ValueError:
            continue
        if event.get("meta", {}).get("domain") == "canary":
            continue                # Wikimedia monitoring events
        key = f"{event.get('wiki')}|{event.get('title')}"
        if args.late_fraction and random.random() < args.late_fraction:
            heapq.heappush(held, (now + args.late_delay, key, data))
            delayed += 1
        else:
            producer.send(args.topic, key=key, value=data)
            sent += 1
        while held and held[0][0] <= now:
            _, k, v = heapq.heappop(held)
            producer.send(args.topic, key=k, value=v)
            sent += 1
        if sent and sent % 2000 == 0:
            print(f"[producer] sent={sent:,} held={len(held):,} "
                  f"delayed_total={delayed:,}", flush=True)
        if args.duration and now - start > args.duration:
            break
    for _, k, v in held:            # flush whatever is still held back
        producer.send(args.topic, key=k, value=v)
    producer.flush()
    print(f"[producer] done: sent={sent + len(held):,}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
