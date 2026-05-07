import json
import os
import time
import boto3

REGION = os.getenv("AWS_REGION", "us-east-2")
STREAM_NAME = os.getenv("KINESIS_STREAM", "crypto-trades-kinesis")
DATA_FILE = os.getenv("DATA_FILE", "captured_trades.jsonl")
SPEED = float(os.getenv("REPLAY_SPEED", "20.0"))
BATCH_SIZE = 500  # Kinesis put_records hard limit

client = boto3.client("kinesis", region_name=REGION)


def flush_batch(batch):
    if not batch:
        return 0
    resp = client.put_records(StreamName=STREAM_NAME, Records=batch)
    failed = resp.get("FailedRecordCount", 0)
    if failed:
        print(f"  WARN: {failed}/{len(batch)} records failed (will retry next batch)")
    return len(batch) - failed


def replay():
    with open(DATA_FILE, "r") as f:
        lines = f.readlines()

    print(f"Loaded {len(lines)} records from {DATA_FILE}")
    print(f"Stream: {STREAM_NAME}  Region: {REGION}")
    print(f"Speed: {SPEED}x  (1.0 = real-time)")
    print(f"Batch size: {BATCH_SIZE}\n")

    prev_time = None
    sent = 0
    batch = []
    start = time.time()

    for line in lines:
        record = json.loads(line.strip())
        original_time = record["trade_time"]

        if prev_time is not None:
            delay = (original_time - prev_time) / 1000.0 / SPEED
            if 0 < delay < 5:
                time.sleep(delay)

        # Remap trade_time so Spark watermarks behave as if live.
        record["trade_time"] = int(time.time() * 1000)
        prev_time = original_time

        batch.append({
            "Data": json.dumps(record).encode("utf-8"),
            "PartitionKey": record["symbol"],
        })

        if len(batch) >= BATCH_SIZE:
            sent += flush_batch(batch)
            batch = []
            elapsed = time.time() - start
            rate = sent / elapsed if elapsed > 0 else 0
            print(f"Replayed {sent}/{len(lines)} ({rate:.0f} msg/s)")

    sent += flush_batch(batch)

    elapsed = time.time() - start
    print(f"\nDone. {sent} records produced in {elapsed:.1f}s ({sent/elapsed:.0f} msg/s)")


if __name__ == "__main__":
    replay()
