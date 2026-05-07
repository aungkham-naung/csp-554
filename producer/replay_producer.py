import json
import os
import time
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "REPLACE_WITH_MSK_BOOTSTRAP:9092",
)
KAFKA_TOPIC = "crypto-trades"
DATA_FILE = "data/captured_trades.jsonl"
SPEED = float(os.getenv("REPLAY_SPEED", "2.0"))

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "enable.idempotence": True,
    "linger.ms": 5,
    "batch.size": 16384,
})


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def replay():
    with open(DATA_FILE, "r") as f:
        lines = f.readlines()

    print(f"Loaded {len(lines)} records from {DATA_FILE}")
    print(f"Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"Speed: {SPEED}x  (1.0 = real-time, higher = faster)\n")

    prev_time = None
    sent = 0
    start = time.time()

    for line in lines:
        record = json.loads(line.strip())
        original_time = record["trade_time"]

        if prev_time is not None:
            delay = (original_time - prev_time) / 1000.0 / SPEED
            if 0 < delay < 5:
                time.sleep(delay)

        # Remap trade_time to "now" so Spark watermarks behave as if live.
        record["trade_time"] = int(time.time() * 1000)
        prev_time = original_time

        producer.produce(
            topic=KAFKA_TOPIC,
            key=record["symbol"].encode("utf-8"),
            value=json.dumps(record).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        sent += 1

        if sent % 500 == 0:
            producer.flush()
            elapsed = time.time() - start
            rate = sent / elapsed if elapsed > 0 else 0
            print(f"Replayed {sent}/{len(lines)} ({rate:.0f} msg/s)")

    producer.flush()
    elapsed = time.time() - start
    print(f"\nDone. {sent} records produced in {elapsed:.1f}s ({sent/elapsed:.0f} msg/s)")


if __name__ == "__main__":
    if "REPLACE_WITH_MSK_BOOTSTRAP" in KAFKA_BOOTSTRAP:
        raise SystemExit(
            "Set KAFKA_BOOTSTRAP env var or edit replay_producer.py:KAFKA_BOOTSTRAP"
        )
    replay()
