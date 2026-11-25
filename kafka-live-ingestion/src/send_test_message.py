"""Send fake price messages to Kafka WITHOUT calling the Yahoo API.

Use it to test Kafka and the consumer when the market is closed, or so a
teammate (the Spark cleaning task) can get sample documents in MongoDB.

  python src/send_test_message.py --count 5
  python src/send_test_message.py --symbol HRHO.CA --count 3
  python src/send_test_message.py --bad        # also sends one broken message (dead-letter test)
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

import messages
from config import load_settings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--symbol", default="COMI.CA")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--bad", action="store_true", help="also send one invalid message")
    args = parser.parse_args()

    settings = load_settings()
    producer = Producer({"bootstrap.servers": settings.bootstrap_servers, "acks": "all"})

    start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start -= timedelta(minutes=start.minute % 5 + 5 * args.count)  # whole 5-minute candles in the past
    price = 95.0
    for i in range(args.count):
        price += random.uniform(-0.3, 0.3)
        row = {"Open": price, "High": price + 0.2, "Low": price - 0.2,
               "Close": price + 0.05, "Volume": random.randint(1_000, 50_000)}
        message = messages.build_message(args.symbol, start + timedelta(minutes=5 * i), row,
                                         interval=settings.candle_interval, source="test_fake")
        producer.produce(settings.topic, key=message["symbol"].encode(), value=messages.encode(message))
    if args.bad:
        producer.produce(settings.topic, key=b"BAD", value=b"this is not json")
    producer.flush(15)
    print(f"sent {args.count} test message(s){' + 1 bad' if args.bad else ''} to topic {settings.topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
