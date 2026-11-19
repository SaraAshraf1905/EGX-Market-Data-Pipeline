import os, sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"


import json
from bson import json_util
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, explode, to_date, date_format, lit, when,
    current_timestamp, concat_ws, coalesce
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PATH = str(PROJECT_ROOT / "data" / "silver" / "fact_daily_price")

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "egx_raw_db"             
COLL_NAME = "api_raw_responses"    



client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
coll = client[DB_NAME][COLL_NAME]

docs = list(coll.find({"processed": {"$ne": True}}))
print("Documents to process:", len(docs))

if not docs:
    print("No new documents. Nothing to do.")
    client.close()
    raise SystemExit(0)


ids = [d["_id"] for d in docs]

json_lines = []
for d in docs:
    d["_id"] = str(d["_id"])
    
    if "ingested_at" in d and hasattr(d["ingested_at"], "isoformat"):
        d["ingested_at"] = d["ingested_at"].isoformat()
    json_lines.append(json.dumps(json.loads(json_util.dumps(d))))


spark = SparkSession.builder \
    .appName("EGX_Transform") \
    .master("local[*]") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

df = spark.read.json(spark.sparkContext.parallelize(json_lines))


exploded_df = df.withColumn("record", explode(col("raw_payload")))

available_columns = exploded_df.select("record.*").columns

if "Adj Close" in available_columns:
    adj_close_expr = when((col("record.Adj Close").isNull()) | (col("record.Adj Close") == 0),
                          col("record.Close")).otherwise(col("record.Adj Close"))
else:
    adj_close_expr = col("record.Close")

dividends_expr = col("record.Dividends").cast("decimal(18,6)") if "Dividends" in available_columns else lit(0.0).cast("decimal(18,6)")
splits_expr = col("record.Stock Splits").cast("decimal(12,6)") if "Stock Splits" in available_columns else lit(0.0).cast("decimal(12,6)")
cap_gains_expr = col("record.Capital Gains").cast("decimal(18,6)") if "Capital Gains" in available_columns else lit(0.0).cast("decimal(18,6)")

base = exploded_df \
    .withColumn("Symbol_Natural", regexp_replace(col("symbol"), "\\.CA$", "")) \
    .withColumn("Date_Key", date_format(to_date(col("record.Date"), "yyyy-MM-dd"), "yyyyMMdd").cast("int")) \
    .withColumn("Full_Date", to_date(col("record.Date"), "yyyy-MM-dd")) \
    .withColumn("Open_Price", col("record.Open").cast("decimal(18,4)")) \
    .withColumn("High_Price", col("record.High").cast("decimal(18,4)")) \
    .withColumn("Low_Price", col("record.Low").cast("decimal(18,4)")) \
    .withColumn("Close_Price", col("record.Close").cast("decimal(18,4)")) \
    .withColumn("Adj_Close_Price", adj_close_expr.cast("decimal(18,4)")) \
    .withColumn("Volume", coalesce(col("record.Volume").cast("bigint"), lit(0))) \
    .withColumn("Trade_Value", (col("Close_Price") * col("Volume")).cast("decimal(22,3)")) \
    .withColumn("Dividend_Amount", coalesce(dividends_expr, lit(0.0).cast("decimal(18,6)"))) \
    .withColumn("Stock_Split_Ratio", coalesce(splits_expr, lit(0.0).cast("decimal(12,6)"))) \
    .withColumn("Capital_Gains", coalesce(cap_gains_expr, lit(0.0).cast("decimal(18,6)"))) \
    .withColumn("Is_Traded", when(col("Volume") > 0, True).otherwise(False)) \
    .withColumn("Fetched_At", col("ingested_at").cast("timestamp")) \
    .withColumn("Source_Name", col("source")) \
    .withColumn("Source_File", lit("mongo_live_api_snapshot.json")) \
    .withColumn("Mongo_Document_Id", col("_id").cast("string")) \
    .withColumn("Run_Id", col("airflow_run_id").cast("string")) \
    .withColumn("Loaded_At", current_timestamp())

dq_expr = concat_ws(";",
    when(col("Open_Price").isNull(), "NULL_OPEN"),
    when(col("High_Price").isNull(), "NULL_HIGH"),
    when(col("Low_Price").isNull(), "NULL_LOW"),
    when(col("Close_Price").isNull(), "NULL_CLOSE"),
    when((col("Open_Price") < 0) | (col("High_Price") < 0) | (col("Low_Price") < 0) | (col("Close_Price") < 0), "NEG_PRICE"),
    when(col("High_Price") < col("Low_Price"), "HIGH_LT_LOW"),
    when((col("Close_Price") > col("High_Price")) | (col("Close_Price") < col("Low_Price")), "CLOSE_OUT_OF_RANGE"),
    when(col("Volume") < 0, "NEG_VOLUME"),
)

fact_df = base \
    .withColumn("DQ_Flags", when(dq_expr == "", "OK").otherwise(dq_expr)) \
    .dropDuplicates(["Symbol_Natural", "Date_Key"]) \
    .select(
        "Symbol_Natural", "Date_Key", "Full_Date",
        "Open_Price", "High_Price", "Low_Price", "Close_Price", "Adj_Close_Price",
        "Volume", "Trade_Value", "Dividend_Amount", "Stock_Split_Ratio", "Capital_Gains",
        "Is_Traded", "DQ_Flags", "Fetched_At", "Source_Name", "Source_File",
        "Mongo_Document_Id", "Run_Id", "Loaded_At")


fact_df.write.mode("overwrite") \
    .partitionBy("Full_Date") \
    .parquet(SILVER_PATH)

coll.update_many({"_id": {"$in": ids}}, {"$set": {"processed": True}})

print("Done. Silver rows:", fact_df.count())
client.close()
spark.stop()