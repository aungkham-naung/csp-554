import os
import sys
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "b-1.cryptokafka.9zha68.c4.kafka.us-east-2.amazonaws.com:9092,"
    "b-2.cryptokafka.9zha68.c4.kafka.us-east-2.amazonaws.com:9092",
)
TOPIC = "crypto-trades"
PARTITIONS = 3
REPLICATION = 2

admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

print(f"Bootstrap: {KAFKA_BOOTSTRAP}")
print(f"Creating topic '{TOPIC}' (partitions={PARTITIONS}, replication={REPLICATION})...")

new_topic = NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=REPLICATION)
fut = admin.create_topics([new_topic])

for name, f in fut.items():
    try:
        f.result()
        print(f"OK: created topic '{name}'")
    except Exception as e:
        if "already exists" in str(e).lower() or "TopicExistsError" in type(e).__name__:
            print(f"OK: topic '{name}' already exists")
        else:
            print(f"FAIL: {name} -> {e}")
            sys.exit(1)

print("\nListing topics:")
md = admin.list_topics(timeout=10)
for t in sorted(md.topics):
    if not t.startswith("__"):
        print(f"  - {t} ({len(md.topics[t].partitions)} partitions)")
