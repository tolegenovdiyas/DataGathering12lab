import json
import time
import os
from kafka import KafkaProducer
from websocket import WebSocketApp


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto_trades"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# new: max messages (0 or unset = unlimited)
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "100"))
message_count = 0
stop_requested = False  # new: stop the outer loop when True

# Global producer variable
producer = None


def create_kafka_producer():
    """
    Create a Kafka producer with JSON serialization.
    Returns None on failure.
    """
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
        )
        return p
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        return None


def _on_send_success(record_metadata):
    print(f"Message sent to Kafka topic={record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}")


def _on_send_error(exc):
    print(f"Failed to send message to Kafka: {exc}")


def on_message(ws, message):
    """
    Called when a new WebSocket message is received.
    Parses Binance trade event and sends it to Kafka.
    """
    global producer, message_count, stop_requested

    # increment counter and enforce limit (0 = unlimited)
    message_count += 1
    if 0 < MAX_MESSAGES <= message_count:
        print(f"Message limit reached ({MAX_MESSAGES}), closing websocket.")
        stop_requested = True
        try:
            ws.close()
        except Exception:
            pass
        return

    try:
        data = json.loads(message)
    except Exception as e:
        print(f"Invalid JSON from websocket: {e}")
        return

    # Validate required numeric fields
    price_raw = data.get("p")
    qty_raw = data.get("q")
    if price_raw is None or qty_raw is None:
        print(f"Skipping message without price/quantity: {data}")
        return

    try:
        price = float(price_raw)
        quantity = float(qty_raw)
    except (TypeError, ValueError) as e:
        print(f"Failed to parse numeric fields: {e} -- {data}")
        return

    trade_event = {
        "event_type": data.get("e"),
        "event_time": data.get("E"),
        "symbol": data.get("s"),
        "trade_id": data.get("t"),
        "price": price,
        "quantity": quantity,
        "buyer_order_id": data.get("b"),
        "seller_order_id": data.get("a"),
        "trade_time": data.get("T"),
        "is_buyer_maker": data.get("m"),
    }

    if not producer:
        print(f"No Kafka producer available; dropping message: {trade_event}")
        return

    try:
        future = producer.send(KAFKA_TOPIC, trade_event)
        future.add_callback(_on_send_success)
        future.add_errback(_on_send_error)
        print(f"Queued to Kafka: {trade_event['symbol']} @ {trade_event['price']}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code}, {close_msg}")


def on_open(ws):
    print("WebSocket connection opened - listening for BTC/USDT trades...")


if __name__ == "__main__":
    # Create global producer
    producer = create_kafka_producer()
    if producer:
        print(f"Kafka producer created, connected to {KAFKA_BOOTSTRAP_SERVERS}")
    else:
        print(f"Kafka producer not available. Messages will be dropped until re-created.")

    # Create WebSocket connection
    ws_app = WebSocketApp(
        BINANCE_WS_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )

    # Run forever with auto-reconnect
    print("Starting WebSocket connection...")
    try:
        while True:
            try:
                ws_app.run_forever(ping_interval=30, ping_timeout=10)
            except KeyboardInterrupt:
                print("\nInterrupted by user - shutting down gracefully...")
                ws_app.keep_running = False
                ws_app.close()
                break
            except Exception as e:
                print(f"WebSocket crashed, retrying in 5 seconds: {e}")
                time.sleep(5)

            # stop reconnect loop when we reached the message limit
            if stop_requested:
                print("Stop requested after reaching message limit; exiting main loop.")
                break
    finally:
        if producer:
            try:
                producer.flush(timeout=10)
                producer.close()
                print("Producer flushed and closed.")
            except Exception as e:
                print(f"Error closing producer: {e}")
        print("Shutdown complete.")