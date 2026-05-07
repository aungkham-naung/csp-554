import json
import os
import time
import websocket
from datetime import datetime, timezone

PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD"]
WS_URL = "wss://ws-feed.exchange.coinbase.com"
OUTPUT_FILE = "data/captured_trades.jsonl"

count = 0


def to_symbol(product_id):
    return product_id.replace("-", "")


def iso_to_ms(iso_str):
    if iso_str.endswith("Z"):
        iso_str = iso_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)


def on_message(ws, message):
    global count
    msg = json.loads(message)
    msg_type = msg.get("type")

    if msg_type != "match":
        if msg_type in ("subscriptions", "error"):
            print(f"[{msg_type}] {message[:200]}")
        return

    record = {
        "symbol": to_symbol(msg["product_id"]),
        "price": float(msg["price"]),
        "quantity": float(msg["size"]),
        "trade_time": iso_to_ms(msg["time"]),
        "buyer_maker": msg["side"] == "sell",
        "trade_id": msg["trade_id"],
        "capture_time": int(time.time() * 1000),
    }

    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")

    count += 1
    if count <= 5 or count % 100 == 0:
        ts = datetime.fromtimestamp(record["trade_time"] / 1000).strftime("%H:%M:%S")
        print(f"[{ts}] captured {count} | {record['symbol']} ${record['price']:.2f} x {record['quantity']}")


def on_open(ws):
    print(f"Connected. Subscribing to: {', '.join(PRODUCTS)}")
    sub = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["matches"],
    }
    ws.send(json.dumps(sub))
    print(f"Sent subscribe. Writing to: {OUTPUT_FILE}\n")


def on_error(ws, error):
    print(f"WS Error: {error}")


def on_close(ws, status_code, msg):
    print(f"\nClosed (code={status_code}, msg={msg}). Total captured: {count}")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
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
        print(f"\nStopped. Total captured: {count}")
