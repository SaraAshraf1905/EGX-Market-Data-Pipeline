# 3. Design decisions

Each decision has a reason, so it can be explained to the instructor.

## 1. Topic: `prod.yahoo.egx_prices.v1`

Pattern `<env>.<source>.<data>.<version>`, lowercase with dots. The `v1` allows a future incompatible message format (`v2`) to live next to the old one. One topic for all stocks (not one per stock): simpler, and the key keeps stocks apart.

## 2. Partitions = 3, replication factor = 1, retention = 7 days

- **3 partitions:** allows up to 3 consumers in the same group to read in parallel. Not needed for the current volume, but cheap.
- **Replication 1:** we run a single broker, so a higher value is impossible. A real production cluster would use 3.
- **7 days:** if the consumer or MongoDB is down for a weekend, nothing is lost. MongoDB is the permanent store, Kafka is only the buffer.

## 3. Message key = stock symbol

Kafka keeps order only inside a partition, and the same key always goes to the same partition. So the candles of `COMI.CA` are always read in the order they were sent.

## 4. Message schema (version 1)

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | int | Format version (1) |
| `source` | string | `yahoo_egx_live` (`test_fake` for test messages) |
| `symbol` | string | Yahoo ticker, upper case, e.g. `COMI.CA` |
| `interval` | string | Candle size, `1d` (daily) in this project |
| `bar_time` | string | Candle start with offset, as Yahoo gives it (Cairo, `+02:00`) |
| `bar_time_utc` | string | Same instant in UTC (`...Z`) |
| `open`, `high`, `low`, `close` | number or null | Prices, **as received** (no rounding) |
| `volume` | int or null | Traded volume |
| `fetched_at_utc` | string | When the producer got it |

The MongoDB document is this message plus:

| Field | Meaning |
|---|---|
| `_id` | `symbol\|interval\|bar_time_utc`, e.g. `COMI.CA\|1d\|2026-11-11T22:00:00Z` |
| `ingested_at` | When the consumer stored it (UTC datetime). Task 2 uses it to read only new data |
| `kafka` | `{topic, partition, offset}` – where the message came from, for traceability |

**Why raw values?** Bronze must keep the data exactly as it arrived. Rounding, rejecting bad rows, removing duplicates is the job of the Spark cleaning step (silver). The consumer only rejects messages that are *structurally* broken (not JSON, missing required fields) and puts them in the dead-letter collection.

## 5. Delivery guarantee: at-least-once + idempotent writes = no duplicates

- **Producer:** `acks=all` (wait for Kafka to store it) and `enable.idempotence=true` (a network retry cannot duplicate a message inside Kafka).
- **Consumer:** `enable.auto.commit=false`. We commit the offset **after** MongoDB confirmed the write. If anything fails before that, the message is read again.
- **Duplicates:** a message may therefore arrive twice. The `_id` is built from the candle identity and we use `replace_one(..., upsert=True)`, so the second arrival overwrites the first. Result: exactly one document per candle, which holds the latest values (useful because Yahoo reports a still-open candle with changing values).

## 6. MongoDB failure handling

If a write fails, the consumer logs it, waits 5 seconds, moves back to that offset (`seek`) and tries again. It never commits a message that was not stored.

## 7. Dead-letter collection

`live_prices_dead_letter` stores messages that cannot be accepted, with the reason and the Kafka position. The consumer keeps running; someone can inspect the collection later. This avoids one bad message blocking the pipeline forever.

## 8. Candle size, polling interval and market hours

**Candle size = 1 day (`CANDLE_INTERVAL=1d`).** We tested Yahoo with `src/check_yahoo.py`: for EGX stocks (COMI.CA, HRHO.CA) only daily candles exist; `5m`, `15m` and `1h` fail inside yfinance (`KeyError: 'tradingPeriods'`). Gold (`GC=F`) and USD/EGP (`EGP=X`) also offer intraday candles, but we use daily for all symbols so the whole project has one grain, the same as `fact_daily_price`. Per-symbol candle sizes would be possible but add code and a second grain to clean.

**Polling:** every 3600 seconds, only Sunday–Thursday 10:00–14:30 (Cairo time). The producer asks Yahoo for the last 5 days and sends the newest candle, so it also works when today has no candle yet. While a day is still open, Yahoo may return the same day with changing values; the consumer overwrites the same document (same `_id`). Yahoo's EGX daily data can be delayed by about a day. Official holidays are not checked (a holiday simply returns no new candle). `--ignore-market-hours` exists for testing, `--once` for Airflow.

**Dates:** a daily candle's time is midnight in the market's own time zone (`+02:00` for EGX, `-05:00` for gold futures, `+00:00` for USD/EGP). The trading date is therefore the **first 10 characters of `bar_time`**, not the UTC date (an EGX candle for 12 Nov has `bar_time_utc` 11 Nov 22:00Z).

## 9. Why `confluent-kafka` and not `kafka-python`

`confluent-kafka` is actively maintained, ships ready-made installers for Windows/macOS/Linux and recent Python versions, and supports idempotent producers and manual commits. `kafka-python` has had compatibility problems with newer Python versions.

## 10. Timestamps

All stored times are UTC (`bar_time_utc`, `fetched_at_utc`, `ingested_at`). The original Cairo time is kept in `bar_time`. The historical silver data uses the *local Cairo date*; Task 2 must derive the date the same way (first 10 characters of `bar_time`) so live and historical rows join without a one-day shift.

## 11. Known limitations (to mention honestly)

- One broker, no authentication, no TLS: a development setup.
- Yahoo is an unofficial source: it can rate-limit or return nothing; the producer retries 3 times, then skips the symbol for this round.
- Live data is daily, like the historical data, so no intraday reduction is needed. The latest day may change until the market closes (and Yahoo may publish EGX days late).
- The Kafka/Mongo/Yahoo integration was not run by the author of this document; unit tests cover the message logic only. Record your runs in `docs/evidence/`.
