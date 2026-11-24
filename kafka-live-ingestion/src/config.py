"""Settings for the Kafka live-ingestion scripts.

Every value comes from an environment variable (or the .env file), with a
sensible default for local development. Nothing secret is hard-coded here.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

try:  # python-dotenv is optional: without it, plain environment variables are used.
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    topic: str
    consumer_group: str
    mongo_uri: str
    mongo_db: str
    mongo_collection: str
    mongo_dead_letter_collection: str
    symbols_file: str
    candle_interval: str
    poll_interval_seconds: int


def load_settings() -> Settings:
    return Settings(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "prod.yahoo.egx_prices.v1"),
        consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "mongo-writer"),
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "market_raw"),
        mongo_collection=os.getenv("MONGO_COLLECTION", "live_prices"),
        mongo_dead_letter_collection=os.getenv(
            "MONGO_DEAD_LETTER_COLLECTION", "live_prices_dead_letter"
        ),
        symbols_file=os.getenv("SYMBOLS_FILE", "config/symbols.txt"),
        candle_interval=os.getenv("CANDLE_INTERVAL", "1d"),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "3600")),
    )
