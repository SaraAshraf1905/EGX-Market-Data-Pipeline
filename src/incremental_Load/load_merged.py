import os, sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
if os.name == "nt":  # Windows only; inside Docker (Linux) these are not needed
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", r"C:\hadoop")
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

import traceback
from datetime import datetime
import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, quarter, month, dayofweek, dayofmonth, weekofyear,
    last_day, date_format, when, lit
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PATH = os.getenv("SILVER_PATH", str(PROJECT_ROOT / "data" / "silver" / "fact_daily_price"))

# Connection settings come from environment variables (see .env.example).
# Without SQL_USER the Windows login is used (works on your PC, NOT inside Docker).
_server = os.getenv("SQLSERVER_HOST", "localhost")
_port = os.getenv("SQLSERVER_PORT", "1433")
_db = os.getenv("SQLSERVER_DB", "EGX_Warehouse")
_user = os.getenv("SQL_USER", "")
_auth = (f"UID={_user};PWD={os.getenv('SQL_PASSWORD', '')};" if _user else "Trusted_Connection=yes;")
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={_server},{_port};"
    f"DATABASE={_db};"
    + _auth +
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

SOURCE_CONFIG = {
    "yahoo_finance_live_batch": {
        "Source_Name": "Yahoo Finance Live Batch",
        "Source_Type": "API / Live Batch",
        "Provider": "Yahoo Finance",
        "Landing_Path": "MongoDB Staging",
        "Licence_Note": "Public Free Tier",
    },
}

EXCHANGE = ("EGX", "Egyptian Exchange", "EG", "EGP", "Africa/Cairo", "Friday,Saturday", ".CA")



def merge(cur, table, key_cols, cols, rows, update_cols=None):
    """
    update_cols=None -> update all non-key columns
    update_cols=[]   -> INSERT only (no WHEN MATCHED clause)
    update_cols=[..] -> update only those columns
    """
    if not rows:
        return
    on = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    upd_cols = update_cols if update_cols is not None else [c for c in cols if c not in key_cols]
    matched = ""
    if upd_cols:
        upd = ", ".join(f"{c} = s.{c}" for c in upd_cols)
        matched = f"WHEN MATCHED THEN UPDATE SET {upd}"
    placeholders = ",".join("?" for _ in cols)
    sql = f"""
    MERGE {table} AS t
    USING (VALUES ({placeholders})) AS s ({",".join(cols)})
    ON {on}
    {matched}
    WHEN NOT MATCHED THEN INSERT ({",".join(cols)})
        VALUES ({",".join("s." + c for c in cols)});
    """
    cur.executemany(sql, rows)


# Read Silver
spark = SparkSession.builder \
    .appName("EGX_Load") \
    .master("local[*]") \
    .getOrCreate()

fact = spark.read.parquet(SILVER_PATH)
print("Silver rows:", fact.count())

dates_df = fact.select("Date_Key", "Full_Date").distinct() \
    .withColumn("Year", year("Full_Date")) \
    .withColumn("Quarter", quarter("Full_Date")) \
    .withColumn("Month", month("Full_Date")) \
    .withColumn("Month_Name", date_format("Full_Date", "MMMM")) \
    .withColumn("Year_Month", date_format("Full_Date", "yyyy-MM")) \
    .withColumn("Week_Of_Year", weekofyear("Full_Date")) \
    .withColumn("Day_Of_Month", dayofmonth("Full_Date")) \
    .withColumn("Day_Of_Week", dayofweek("Full_Date")) \
    .withColumn("Day_Name", date_format("Full_Date", "EEEE")) \
    .withColumn("Is_Weekend", when(col("Day_Of_Week").isin([6, 7]), True).otherwise(False)) \
    .withColumn("Is_Month_End", when(col("Full_Date") == last_day("Full_Date"), True).otherwise(False)) \
    .withColumn("Is_Market_Holiday", lit(False)) \
    .withColumn("Is_EGX_Trading_Day", when(col("Is_Weekend"), False).otherwise(True))

symbols_rows_spark = fact.select("Symbol_Natural").distinct().collect()
source_rows_spark = fact.select("Source_Name").distinct().collect()
dates_rows_spark = dates_df.collect()
fact_rows_spark = fact.collect()

# Load to SQL Server
conn = pyodbc.connect(CONN_STR)
cur = conn.cursor()
exit_code = 0

run_id = cur.execute("""
    INSERT INTO ops.pipeline_runs (step_name, status, started_at)
    OUTPUT INSERTED.run_id
    VALUES ('load_silver_to_gold', 'running', SYSDATETIME())
""").fetchone()[0]
conn.commit()

try:
    # dim_exchange
    merge(cur, "gold.dim_exchange", ["exchange_code"],
          ["exchange_code", "exchange_name", "country_code", "currency_code",
           "timezone", "weekend_days", "yahoo_suffix"],
          [EXCHANGE])
    conn.commit()
    egx_key = cur.execute(
        "SELECT exchange_key FROM gold.dim_exchange WHERE exchange_code = 'EGX'").fetchone()[0]

    # dim_source
    source_rows = []
    for r in source_rows_spark:
        cfg = SOURCE_CONFIG.get(r.Source_Name)
        if cfg is None:
            raise ValueError(f"مصدر مش موجود في SOURCE_CONFIG: {r.Source_Name}")
        source_rows.append((r.Source_Name, cfg["Source_Name"], cfg["Source_Type"],
                            cfg["Provider"], cfg["Landing_Path"], cfg["Licence_Note"], True))
    merge(cur, "gold.dim_source", ["source_code"],
          ["source_code", "source_name", "source_type", "provider",
           "landing_path", "licence_note", "is_active"], source_rows)
    conn.commit()

    #  dim_date (insert only: calendar data never changes)
    date_cols = ["date_key", "full_date", "year", "quarter", "month", "month_name", "year_month",
                 "week_of_year", "day_of_month", "day_of_week", "day_name",
                 "is_weekend", "is_month_end", "is_market_holiday", "is_egx_trading_day"]
    date_rows = [(
        r.Date_Key, r.Full_Date, r.Year, r.Quarter, r.Month, r.Month_Name,
        r.Year_Month, r.Week_Of_Year, r.Day_Of_Month, r.Day_Of_Week, r.Day_Name,
        bool(r.Is_Weekend) if r.Is_Weekend is not None else False,
        bool(r.Is_Month_End) if r.Is_Month_End is not None else False,
        bool(r.Is_Market_Holiday) if r.Is_Market_Holiday is not None else False,
        bool(r.Is_EGX_Trading_Day) if r.Is_EGX_Trading_Day is not None else True,
    ) for r in dates_rows_spark]
    merge(cur, "gold.dim_date", ["date_key"], date_cols, date_rows, update_cols=[])
    conn.commit()

    # dim_symbol (keep existing metadata, refresh only price-data flag + timestamp)
    now = datetime.now()
    symbol_rows = [(r.Symbol_Natural, r.Symbol_Natural + ".CA", None, None,
                    "Equities", "Stock", "Ordinary", "EGP", True, egx_key, None,
                    "Yahoo Finance", False, False, True, now)
                   for r in symbols_rows_spark]
    symbol_cols = ["symbol", "yahoo_ticker", "isin", "company_name", "asset_class",
                   "instrument_type", "share_class", "currency_code", "is_primary_listing",
                   "exchange_key", "logo_id", "metadata_provider", "found_by_isin",
                   "has_metadata", "has_price_data", "updated_at"]
    merge(cur, "gold.dim_symbol", ["symbol"], symbol_cols, symbol_rows,
          update_cols=["has_price_data", "updated_at"])
    conn.commit()

    # fact_daily_price
    symbol_map = dict(cur.execute("SELECT symbol, symbol_key FROM gold.dim_symbol").fetchall())
    source_map = dict(cur.execute("SELECT source_code, source_key FROM gold.dim_source").fetchall())

    fact_cols = ["symbol_key", "date_key", "source_key", "open_price", "high_price", "low_price",
                 "close_price", "adj_close_price", "volume", "trade_value", "dividend_amount",
                 "stock_split_ratio", "capital_gains", "is_traded", "dq_flags", "fetched_at_utc",
                 "source_file", "mongo_doc_id", "run_id", "loaded_at"]
    fact_rows = []
    for r in fact_rows_spark:
        # Safety fallbacks only: Transform.py already fills these values.
        vol = int(r.Volume) if r.Volume is not None else 0
        t_val = (r.Trade_Value if r.Trade_Value is not None
                 else (float(r.Close_Price) * vol if r.Close_Price is not None else 0))
        fact_rows.append((
            symbol_map[r.Symbol_Natural],
            int(r.Date_Key),
            source_map[r.Source_Name],
            r.Open_Price,
            r.High_Price,
            r.Low_Price,
            r.Close_Price,
            r.Adj_Close_Price,
            vol,
            t_val,
            r.Dividend_Amount if r.Dividend_Amount is not None else 0,
            r.Stock_Split_Ratio if r.Stock_Split_Ratio is not None else 0,
            r.Capital_Gains if r.Capital_Gains is not None else 0,
            bool(r.Is_Traded) if r.Is_Traded is not None else (vol > 0),
            r.DQ_Flags if r.DQ_Flags else "OK",
            r.Fetched_At,
            r.Source_File,
            r.Mongo_Document_Id,
            run_id,
            r.Loaded_At if r.Loaded_At is not None else now,
        ))
    merge(cur, "gold.fact_daily_price", ["symbol_key", "date_key", "source_key"],
          fact_cols, fact_rows)
    conn.commit()

    # log success
    cur.execute("""
        UPDATE ops.pipeline_runs
        SET status = 'success', rows_read = ?, rows_written = ?,
            message = 'loaded', finished_at = SYSDATETIME()
        WHERE run_id = ?""", len(fact_rows), len(fact_rows), run_id)
    conn.commit()
    print(f"run {run_id}: success, fact rows = {len(fact_rows)}")

except Exception as e:
    exit_code = 1
    traceback.print_exc()
    conn.rollback()
    cur.execute("""
        UPDATE ops.pipeline_runs
        SET status = 'failed', message = ?, finished_at = SYSDATETIME()
        WHERE run_id = ?""", str(e)[:500], run_id)
    conn.commit()

finally:
    conn.close()
    spark.stop()
    print("Finished.", flush=True)
    # Force-exit so Spark on Windows can't hang the process,
    # but keep the real exit code so Airflow sees failures.
    os._exit(exit_code)
