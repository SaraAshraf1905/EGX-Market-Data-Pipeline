# 6. Sharing the loaded data with teammates

Every teammate runs their **own** MongoDB on their own computer. Code goes through GitHub; data does **not** (it is in `.gitignore`).

## Option A – export and import a file (start here)

**Who has the data** (the person who ran the consumer):

```powershell
python src/export_mongo.py
```

This creates `data/export/live_prices.jsonl` (one JSON document per line, about 38 lines for one daily round). Upload this file to the team's shared Google Drive folder. **Never commit it to GitHub** (`data/` is ignored).

**Each teammate:**

1. Clone the repository and set up the project (`README.md` Quick start, steps up to `pip install -r requirements.txt`; Kafka is not needed for this).
2. Make sure their own MongoDB is running (Compass can connect to it).
3. Download the file from Drive, then:

```powershell
python src/import_mongo.py "C:\path\to\live_prices.jsonl" --dry-run   # checks the file, writes nothing
python src/import_mongo.py "C:\path\to\live_prices.jsonl"
python src/check_mongo.py
```

Importing twice is safe: documents are matched by `_id` and overwritten, so there are no duplicates. Dates (`ingested_at`) are restored as real MongoDB dates.

## Option B – everyone runs the pipeline themselves

Follow the README on each machine (Kafka + producer + consumer). Each person gets their own, fresh copy of the live data. Use this when the data must be up to date. The numbers will differ slightly between people if they run at different times, so for the Spark cleaning task agree to use one exported file (Option A) so everyone tests on the same rows.

## Option C – one shared MongoDB (not recommended)

Possible only on the same network, with your computer always on, with MongoDB opened to the network and password protected. It exposes the database and breaks when your computer sleeps. Do not use it for this project.

## What Task 2 does after the import

Spark reads `market_raw.live_prices` from their own MongoDB, applies the cleaning rules, and loads the result into SQL Server `silver`. See section 5 of `docs/05_task1_report.md` (fields, and the rule to take the trading date from the first 10 characters of `bar_time`).
