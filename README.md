# EGX Market Data Pipeline

Protecting Egyptian savings: EGX stocks and the EGX30 index, with gold, silver and USD/EGP planned.
Python → Kafka → MongoDB (bronze) → PySpark (silver) → SQL Server star schema (gold) → dbt metrics → Power BI, orchestrated by Airflow.

## Folders
| Folder | Purpose |
|---|---|
| `kafka-live-ingestion/` | Kafka broker (Docker), producer (Yahoo → Kafka), consumer (Kafka → Mongo) |
| `src/ingestion/` | Batch API extractor and Mongo helper |
| `src/transformation/Transform.py` | Spark: Mongo (API + Kafka docs) → silver parquet |
| `src/Incremental_Load/load_merged.py` | silver → `gold.fact_daily_price` (MERGE, idempotent) |
| `sql/` | DDL (`ddl/`) and ELT (`elt/`) scripts for `EGX_Warehouse` |
| `dbt-transformations/` | dbt metrics (daily return, drawdown, volatility, rebased 100…) |
| `src/export/` | Writes metrics CSVs for Power BI |
| `dags/` | Reserved for the Airflow DAG (not built yet) |
| `docs/` | Architecture and runbooks |

## Run it (manually, on Windows)
1. `copy .env.example .env` and fill in the passwords (never commit `.env`).
2. Kafka + Mongo: see `kafka-live-ingestion/README.md` (producer and consumer).
3. Spark clean: `python src/transformation/Transform.py`
4. Load gold: `python src/Incremental_Load/load_merged.py`
5. Metrics: `cd dbt-transformations`, then `dbt run` and `dbt test`.
6. Power BI CSVs: `python src/export/export_metrics_csv.py` (writes to `exports/`), then Refresh in Power BI.

Airflow orchestration is the next step and is not implemented yet.

## Known limits
- Gold, silver and USD/EGP are not in `gold.dim_symbol` yet, so only `.CA` stocks and EGX30 flow through.
- Inflation data was dropped from the project scope.
