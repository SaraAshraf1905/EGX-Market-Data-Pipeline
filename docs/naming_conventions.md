# Naming Conventions

One set of rules for every layer of the EGX Market Data Pipeline, so the Spark jobs, SQL Server tables,
dbt models, Kafka, Airflow and Power BI all fit together.

## 1. Data layer (SQL Server, dbt, Parquet)
Everything is `snake_case`. Never use `camelCase` or `PascalCase` for tables or columns.

### Schemas and tables (medallion layers)
| Layer | Schema / prefix | Format | Examples in this project |
|---|---|---|---|
| Bronze (raw) | MongoDB collections | `<source>_<entity>` | `api_raw_responses`, `live_prices` |
| Silver (cleaned) | schema `stg`, Parquet in `data/silver/` | `<entity>` | `stg.stock_prices`, `stg.egx30_daily` |
| Gold (star schema) | schema `gold` | `dim_<entity>` or `fact_<entity>` | `gold.dim_symbol`, `gold.fact_daily_price` |
| Operations | schema `ops` | `<entity>` | `ops.pipeline_runs` |
| dbt staging | schema `dbt_staging` | `stg_<entity>` | `stg_prices` |
| dbt metrics | schema `metrics` | `metric_<measure>` | `metric_daily_return`, `metric_drawdown` |

### Columns
- **Surrogate and foreign keys:** suffix `_key` (`symbol_key`, `date_key`, `source_key`).
- **Dates:** `full_date` for a calendar date; `date_key` is an integer `yyyymmdd`.
- **Timestamps:** suffix `_at` or `_utc` (`loaded_at`, `fetched_at_utc`). Stored in UTC.
- **Booleans and flags:** prefix `is_` (`is_traded`, `is_weekend`, `is_market_holiday`).
- **Prices and volumes:** `open_price`, `close_price`, `adj_close_price`, `volume`.
- **Metrics:** the name says the measure: `daily_return`, `volatility_annualized`, `rebased_100`.

### Data rules
- Prices are `DECIMAL` (for example `DECIMAL(18,4)`), never `FLOAT`.
- The trading date is the first 10 characters of `bar_time` (market timezone), not the UTC date.
- Symbols are stored without the Yahoo suffix: `COMI`, not `COMI.CA`.
- The primary key of the daily fact is `(symbol_key, date_key, source_key)`, and loads use MERGE so they can be re-run safely.

## 2. Infrastructure and orchestration

### Kafka
- Topic format: `<env>.<source>.<entity>.<version>`. Example: `prod.yahoo.egx_prices.v1`.
- Message key is the symbol. Consumer group: `mongo-writer`.

### Airflow
- DAG id format: `<frequency>_<domain>_<action>`. Example: `daily_egx_ingest_and_metrics`.
- Task ids start with a verb: `check_services`, `extract_api_prices`, `consume_to_mongo`,
  `spark_clean`, `merge_to_gold`, `run_dbt_models`, `test_dbt_models`, `export_metrics_csv`.

### MongoDB
- Databases and collections are `snake_case`: `egx_raw_db.api_raw_responses`, `market_raw.live_prices`.
- Document `_id` for live candles is `symbol|interval|bar_time_utc`.

## 3. Python and PySpark
- Follow PEP 8.
- Variables, functions: `snake_case` (`fetch_latest_bar`).
- Classes: `PascalCase` (`YahooBatchExtractor`).
- Constants and environment variables: `UPPER_SNAKE_CASE` (`MONGO_URI`, `KAFKA_BOOTSTRAP_SERVERS`).
- File and folder names: `snake_case` (`transform.py`, `incremental_load/`).
- Secrets are never in code or Git: use `.env` (ignored), documented in `.env.example`.

## 4. Git
- Branches: `feature/<short-description>`, `bugfix/<short-description>`, `docs/<short-description>`.
  Example: `feature/airflow-dag`.
- Commit messages: short, imperative mood. "Add dbt metrics", not "Added dbt metrics".
- Never commit `.env`, `.venv`, `data/`, `exports/` or `profiles.yml`.
