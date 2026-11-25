"""Write every dbt metrics table to a CSV file that Power BI reads.
File and column names must stay the same, otherwise Power BI 'Refresh' breaks.
"""
import os
from pathlib import Path

import pandas as pd
import pyodbc

OUT = Path(os.getenv("EXPORT_DIR", Path(__file__).resolve().parents[2] / "exports"))
TABLES = [
    "metric_symbol_list",
    "metric_compare_rebased",
    "metric_daily_return",
    "metric_drawdown",
    "metric_volatility_30d",
    "metric_monthly_return",
    "metric_rebased_100",
]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.getenv('SQLSERVER_HOST', 'localhost')},{os.getenv('SQLSERVER_PORT', '1433')};"
        f"DATABASE={os.getenv('SQLSERVER_DB', 'EGX_Warehouse')};"
        f"UID={os.environ['DBT_SQL_USER']};PWD={os.environ['DBT_SQL_PASSWORD']};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    for table in TABLES:
        df = pd.read_sql(f"SELECT * FROM metrics.{table}", conn)
        df.to_csv(OUT / f"{table}.csv", index=False)
        print(f"{table}: {len(df)} rows -> {OUT / (table + '.csv')}")
    conn.close()


if __name__ == "__main__":
    main()
