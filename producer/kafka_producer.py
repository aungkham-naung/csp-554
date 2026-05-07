import json
import os
import time
import websocket
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "REPLACE_WITH_MSK_BOOTSTRAP:9092",
)
KAFKA_TOPIC = "crypto-trades"

PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD"]
WS_URL = "wss://ws-feed.exchange.coinbase.com"

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "crypto-ws-producer",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,
    "batch.size": 16384,
})

msg_count = 0


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def to_symbol(product_id):
    return product_id.replace("-", "")


def iso_to_ms(iso_str):
    if iso_str.endswith("Z"):
        iso_str = iso_str[:-1] + "+00:00"
    return int(datetime.fromisoformat(iso_str).timestamp() * 1000)


def on_message(ws, message):
    global msg_count
    msg = json.loads(message)
    if msg.get("type") != "match":
        if msg.get("type") in ("subscriptions", "error"):
            print(f"[{msg['type']}] {message[:200]}")
        return

    record = {
        "symbol": to_symbol(msg["product_id"]),
        "price": float(msg["price"]),
        "quantity": float(msg["size"]),
        "trade_time": iso_to_ms(msg["time"]),
        "buyer_maker": msg["side"] == "sell",
        "trade_id": msg["trade_id"],
    }

    producer.produce(
        topic=KAFKA_TOPIC,
        key=record["symbol"].encode("utf-8"),
        value=json.dumps(record).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)

    msg_count += 1
    if msg_count % 100 == 0:
        print(f"Produced {msg_count} messages")
        producer.flush()


def on_open(ws):
    print(f"Connected. Producing to Kafka topic: {KAFKA_TOPIC}")
    print(f"Bootstrap: {KAFKA_BOOTSTRAP}")
    sub = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["matches"],
    }
    ws.send(json.dumps(sub))


def on_error(ws, error):
    print(f"WS Error: {error}")


def on_close(ws, status_code, msg):
    print(f"\nClosed (code={status_code}). Total produced: {msg_count}")
    producer.flush()


if __name__ == "__main__":
    if "REPLACE_WITH_MSK_BOOTSTRAP" in KAFKA_BOOTSTRAP:
        raise SystemExit(
            "Set KAFKA_BOOTSTRAP env var or edit kafka_producer.py:KAFKA_BOOTSTRAP"
        )
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    try:
        ws.run_forever(ping_interval=30, ping_timeout=10)
    except KeyboardInterrupt:
        producer.flush()
        print(f"\nTotal produced: {msg_count}")
