# dbt metrics layer (EGX_Warehouse)

Reads the gold star schema and creates metric views in schema `metrics`.
NOTE: MetricFlow / dbt Semantic Layer does not support SQL Server, so each metric is a normal dbt SQL model.
Status: written but not yet run against a real database. Expect to fix small column-name issues on first run.

## Metrics
| Model | Meaning |
|---|---|
| metric_daily_return | close today / close previous traded day - 1 |
| metric_rebased_100 | price rebased to 100 at first date |
| metric_volatility_30d | annualized stdev of daily returns, 30-row window |
| metric_drawdown | % below running peak |
| metric_monthly_return | month-end to month-end return |
| metric_compare_rebased | all symbols rebased to 100 from one common start date |
| metric_symbol_list | symbol + company name for Power BI slicers |
| metric_savings_protection | OPTIONAL gold-in-EGP and USD/EGP (needs gold and FX loaded first; `enable_gold_fx: true`) |

## Setup (Windows PowerShell)
1. Install "ODBC Driver 18 for SQL Server" (or 17; edit `driver` in profiles).
2. `python -m venv .venv` then `.venv\Scripts\Activate.ps1`, then `pip install -r requirements.txt`
3. Enable a SQL login on your SQL Server (mixed mode) and create a user with read on `gold` and create-view rights.
4. Copy `profiles.yml.example` to `$HOME\.dbt\profiles.yml` (never commit it).
5. Set env vars in the same terminal:
   `$env:DBT_SQL_USER="dbt_user"; $env:DBT_SQL_PASSWORD="..."; $env:DBT_SQL_SERVER="localhost"`
6. Check symbols: `SELECT symbol, company_name, asset_class FROM gold.dim_symbol;` Symbol names are already set from your backup (EGX30 etc.).
7. `dbt debug` (must end with "All checks passed"), then `dbt run`, `dbt test`.
8. `dbt docs generate` then `dbt docs serve` for the lineage graph (screenshot for the report).
9. Connect Power BI to the views in schema `metrics`.
