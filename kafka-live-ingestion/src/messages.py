"""The message format (schema) that travels through Kafka, and helpers for it.

This module is plain Python (no Kafka, no MongoDB, no Yahoo) so it can be unit
tested anywhere:  python -m unittest discover -s tests

Message = one price candle of one stock, as JSON:
{
  "schema_version": 1,
  "source": "yahoo_egx_live",
  "symbol": "COMI.CA",
  "interval": "5m",
  "bar_time": "2026-11-15T10:05:00+02:00",   # candle start, Cairo time as sent by Yahoo
  "bar_time_utc": "2026-11-15T08:05:00Z",    # same instant in UTC
  "open": 95.1, "high": 95.4, "low": 94.9, "close": 95.3, "volume": 120450,
  "fetched_at_utc": "2026-11-15T08:07:12Z"
}
Raw layer rule: prices are stored exactly as the API gave them. Business
checks (close > 0, high >= low ...) belong to the Spark cleaning step, not here.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping

SCHEMA_VERSION = 1
DEFAULT_SOURCE = "yahoo_egx_live"

# Fields that must exist for a message to be accepted into MongoDB.
REQUIRED_FIELDS = (
    "schema_version",
    "source",
    "symbol",
    "interval",
    "bar_time_utc",
    "close",
    "fetched_at_utc",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _number(value: Any, as_int: bool = False) -> float | int | None:
    """Convert to a number; missing values (None / NaN) become None."""
    if value is None:
        return None
    try:
        if value != value:  # NaN is the only value not equal to itself
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if as_int else number


def build_message(
    symbol: str,
    bar_time: datetime,
    row: Mapping[str, Any],
    *,
    interval: str,
    source: str = DEFAULT_SOURCE,
    fetched_at: str | None = None,
) -> dict:
    """Create one message from a Yahoo candle. bar_time must be timezone-aware."""
    if bar_time.tzinfo is None:
        raise ValueError("bar_time must be timezone-aware")
    return {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "symbol": symbol.strip().upper(),
        "interval": interval,
        "bar_time": bar_time.isoformat(),
        "bar_time_utc": bar_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "open": _number(row.get("Open")),
        "high": _number(row.get("High")),
        "low": _number(row.get("Low")),
        "close": _number(row.get("Close")),
        "volume": _number(row.get("Volume"), as_int=True),
        "fetched_at_utc": fetched_at or utc_now_iso(),
    }


def make_doc_id(message: Mapping[str, Any]) -> str:
    """Unique id of a candle: symbol + interval + start time.

    Used as the MongoDB _id, so receiving the same candle twice (or receiving a
    newer version of a candle that was still in progress) overwrites the old
    document instead of creating a duplicate.
    """
    return f"{message['symbol']}|{message['interval']}|{message['bar_time_utc']}"


def validate_message(message: Any) -> list[str]:
    """Return a list of problems (empty list = message is acceptable)."""
    if not isinstance(message, dict):
        return ["message is not a JSON object"]
    errors = [f"missing field: {name}" for name in REQUIRED_FIELDS if message.get(name) in (None, "")]
    if "symbol" in message and not isinstance(message["symbol"], str):
        errors.append("symbol must be a string")
    return errors


def encode(message: Mapping[str, Any]) -> bytes:
    return json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def decode(payload: bytes) -> Any:
    """Bytes from Kafka -> Python object. Raises ValueError if it is not JSON."""
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"payload is not valid JSON: {exc}") from exc
