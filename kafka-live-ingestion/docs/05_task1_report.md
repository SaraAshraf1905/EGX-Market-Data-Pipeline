# 5. Task 1 report: Kafka live ingestion

**Date:** 2026-11-15
**Team members:** [name 1] (Kafka, producer), [name 2] (consumer, MongoDB)
**Status:** completed and tested on the team's machines (Windows, Docker Desktop, Python 3.11)

## 1. Goal

Bring live prices from the Yahoo Finance API into MongoDB as raw (bronze) data, with Apache Kafka as the transport between the API and the database, so that no price is lost when MongoDB is slow or stopped.

## 2. What was built

| Component | File | Result |
|---|---|---|
| Kafka broker (KRaft, 1 broker) + topic `prod.yahoo.egx_prices.v1` (3 partitions, 7-day retention) | `docker-compose.yml` | running, healthy |
| Producer: Yahoo API -> Kafka | `src/producer.py` | one message per symbol per round |
| Consumer: Kafka -> MongoDB `market_raw.live_prices` | `src/consumer.py` | stores with upsert, manual commit, dead-letter |
| Message schema and helpers | `src/messages.py` | 10 unit tests pass |
| Helper tools | `src/send_test_message.py`, `src/check_mongo.py`, `src/check_yahoo.py` | used for testing |

Architecture: `Yahoo API -> producer -> Kafka topic -> consumer -> MongoDB (bronze)`. See `README.md`.

## 3. Findings during implementation

1. **Yahoo has no intraday candles for EGX stocks.** `src/check_yahoo.py` showed that for COMI.CA and HRHO.CA only `1d` works; `5m`, `15m`, `1h` fail inside yfinance (`KeyError: 'tradingPeriods'`). Gold (`GC=F`) and USD/EGP (`EGP=X`) do have intraday data.
2. **Decision:** use daily candles (`CANDLE_INTERVAL=1d`) for all 38 symbols, so live data has the same grain as the historical data and as `fact_daily_price`.
3. Yahoo's EGX daily data can be published about a day late (the newest candle on Sunday evening was Thursday's).

## 4. Test results

| Test | How | Result |
|---|---|---|
| Unit tests | `python -m unittest discover -s tests -v` | 10 tests, OK |
| Kafka up and topic created | `docker compose ps -a`, `kafka-topics.sh --describe` | healthy; 3 partitions |
| Producer | `python src/producer.py --once --ignore-market-hours` | `delivered=38 failed=0 skipped=0` |
| Consumer -> MongoDB | `python src/consumer.py` | 38 candles stored (`stored=39` counts one earlier 5m test document, later deleted) |
| No duplicates | the same candles were read again by the consumer | document count did not increase (same `_id` is overwritten) |
| Dead letter | `python src/send_test_message.py --count 1 --bad` then consumer | 1 document in `live_prices_dead_letter`, consumer kept running |
| Fully caught up | `kafka-consumer-groups.sh --describe --group mongo-writer` | LAG 0 on all partitions |

[Optional, if done: **MongoDB down test** (guide step 12): consumer logged `MongoDB write failed ... Retrying` and stored the messages after MongoDB was restarted.]

Evidence (screenshots and outputs) is in `docs/evidence/`.

## 5. Hand-over to Task 2 (Spark cleaning)

| Item | Value |
|---|---|
| MongoDB database / collection | `market_raw` / `live_prices` |
| Bad messages | `market_raw` / `live_prices_dead_letter` (not for cleaning) |
| Unique id (`_id`) | `symbol\|interval\|bar_time_utc` |
| Fields | `schema_version`, `source`, `symbol`, `interval` (`1d`), `bar_time`, `bar_time_utc`, `open`, `high`, `low`, `close`, `volume`, `fetched_at_utc`, `ingested_at`, `kafka` |
| Read only new data | filter `ingested_at` greater than the last run |
| Symbols | 35 EGX stocks (`.CA`), `GC=F` gold, `SI=F` silver, `EGP=X` USD/EGP |
| **Trading date rule** | take the **first 10 characters of `bar_time`** (local market date), not the UTC date. An EGX candle for 12 Nov has `bar_time_utc` 11 Nov 22:00Z |
| Values | raw, unrounded, may contain nulls (no business rules applied in bronze) |
| Symbol names | `.CA` suffix as in Yahoo; Task 2 applies the same alias map as the historical cleaning |

## 6. Hand-over to Airflow

- Ingestion task: `python src/producer.py --once` (fetches once and exits).
- The consumer runs continuously (`python src/consumer.py`), or as a scheduled short run with `--max-messages`.
- Both need the Kafka container running (`docker compose up -d`).

## 7. Limitations

- Single broker, no authentication or TLS (development setup).
- Yahoo is unofficial: it can rate-limit or return no data; the producer retries 3 times and skips the symbol for that round.
- EGX holidays are not checked; a holiday just returns no new candle.
- No candle for the current day until Yahoo publishes it (about a day late for EGX).
