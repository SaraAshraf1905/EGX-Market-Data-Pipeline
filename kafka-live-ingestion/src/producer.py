"""PRODUCER: Yahoo Finance API  ->  Kafka topic.

For every symbol in config/symbols.txt it fetches the latest candle and sends
ONE message to Kafka (key = symbol). It does not touch MongoDB.

Examples
  python src/producer.py                          # keep running, poll every POLL_INTERVAL_SECONDS
  python src/producer.py --once                   # one round, then exit (used by Airflow)
  python src/producer.py --once --ignore-market-hours   # test when the market is closed
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

import messages
from config import Settings, load_settings

log = logging.getLogger("producer")

CAIRO = ZoneInfo("Africa/Cairo")
# EGX trades Sunday-Thursday. Python weekday(): Monday=0 ... Sunday=6.
TRADING_WEEKDAYS = {6, 0, 1, 2, 3}
MARKET_OPEN, MARKET_CLOSE = dtime(10, 0), dtime(14, 30)  # Cairo time

_running = True


def _stop(signum, frame):  # Ctrl+C
    global _running
    _running = False
    log.info("stop requested, finishing the current round...")


def is_market_open(now: datetime | None = None) -> bool:
    now = now.astimezone(CAIRO) if now else datetime.now(CAIRO)
    return now.weekday() in TRADING_WEEKDAYS and MARKET_OPEN <= now.time() <= MARKET_CLOSE


def read_symbols(path: str) -> list[str]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    symbols = [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    if not symbols:
        raise SystemExit(f"No symbols found in {path}")
    return symbols


def fetch_latest_bar(symbol: str, interval: str, retries: int = 3):
    """Return (bar_time, row_dict) of the newest candle, or None if Yahoo has nothing."""
    import yfinance as yf  # imported here so the module can be imported without it

    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            # period="5d" (not "1d"): if today has no candles yet (market closed,
            # weekend, holiday) we still get the most recent trading session.
            df = yf.Ticker(symbol).history(
                period="5d", interval=interval, auto_adjust=False, actions=False
            )
            if df is None or df.empty:
                return None
            return df.index[-1].to_pydatetime(), df.iloc[-1].to_dict()
        except Exception as exc:  # network error, rate limit ...
            log.warning("%s: attempt %d/%d failed: %s", symbol, attempt, retries, exc)
            time.sleep(delay)
            delay *= 2  # wait 1s, 2s, 4s
    return None


def make_producer(settings: Settings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": "egx-live-producer",
            "acks": "all",                # wait until Kafka has stored the message
            "enable.idempotence": True,   # a retry can never create a duplicate in Kafka
            "linger.ms": 50,
        }
    )


def run_round(producer: Producer, settings: Settings, symbols: list[str]) -> int:
    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            log.error("delivery failed for key=%s: %s", msg.key(), err)
        else:
            stats["delivered"] += 1
            log.debug("delivered %s [partition %d] offset %d", msg.key(), msg.partition(), msg.offset())

    skipped = 0
    for symbol in symbols:
        bar = fetch_latest_bar(symbol, settings.candle_interval)
        if bar is None:
            log.warning("%s: no data from Yahoo, skipped", symbol)
            skipped += 1
            continue
        bar_time, row = bar
        message = messages.build_message(symbol, bar_time, row, interval=settings.candle_interval)
        producer.produce(
            topic=settings.topic,
            key=message["symbol"].encode("utf-8"),
            value=messages.encode(message),
            on_delivery=on_delivery,
        )
        producer.poll(0)  # lets the client report finished deliveries
    producer.flush(30)    # wait for all messages to be confirmed
    log.info("round finished: delivered=%d failed=%d skipped=%d",
             stats["delivered"], stats["failed"], skipped)
    return stats["failed"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--once", action="store_true", help="run one round and exit")
    parser.add_argument("--ignore-market-hours", action="store_true",
                        help="fetch even when the EGX is closed (for testing)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = load_settings()
    symbols = read_symbols(settings.symbols_file)
    producer = make_producer(settings)
    signal.signal(signal.SIGINT, _stop)
    log.info("producer started: %d symbols -> topic %s on %s",
             len(symbols), settings.topic, settings.bootstrap_servers)

    failures = 0
    while _running:
        if args.ignore_market_hours or is_market_open():
            failures = run_round(producer, settings, symbols)
        else:
            log.info("EGX is closed (Sun-Thu 10:00-14:30 Cairo time), nothing to fetch")
        if args.once:
            break
        for _ in range(settings.poll_interval_seconds):  # sleep in 1s steps so Ctrl+C is quick
            if not _running:
                break
            time.sleep(1)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
