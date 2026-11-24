# 2. Step-by-step guide

Commands are written for **Windows PowerShell**. On macOS/Linux replace `copy` with `cp`, and activate the environment with `source .venv/bin/activate`.

Run every command from the project folder (`kafka-live-ingestion`).

---

## Step 0 – Prerequisites

| Tool | Check with | Notes |
|---|---|---|
| Docker Desktop | `docker --version` and `docker compose version` | Must be **running** (whale icon in the system tray) |
| Python 3.10+ | `python --version` | |
| Git | `git --version` | |

---

## Step 1 – Get the project and create your `.env`

```powershell
git clone <your-repo-url>
cd <repo>\kafka-live-ingestion
copy .env.example .env
```

`.env` holds your local settings. It is in `.gitignore` and must never be committed. For this task the defaults work without changes.

---

## Step 2 – Start Kafka

```powershell
docker compose up -d
```

What happens: Docker downloads `apache/kafka:3.7.0` (first time only), starts the broker `egx-kafka`, waits until it is healthy, then starts `egx-kafka-init`, which creates the topic and exits.

Check:

```powershell
docker compose ps -a
```

Expected: `egx-kafka` is `Up ... (healthy)`; `egx-kafka-init` is `Exited (0)`. (Exit code 0 means it finished correctly.)

Check the topic exists:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --topic prod.yahoo.egx_prices.v1
```

Expected: `PartitionCount: 3`, `ReplicationFactor: 1`, `retention.ms=604800000`.

If something fails, read the logs: `docker compose logs kafka`.

---

## Step 3 (optional but recommended) – Kafka UI

A web page to *see* topics and messages.

```powershell
docker compose --profile ui up -d
```

Open <http://localhost:8090> -> Topics -> `prod.yahoo.egx_prices.v1` -> Messages.

---

## Step 4 – Python environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If PowerShell blocks the activation script, run once: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

Run the unit tests (no Docker needed):

```powershell
python -m unittest discover -s tests -v
```

Expected: `Ran 10 tests ... OK`.

---

## Step 5 – Test Kafka alone (no API, no MongoDB)

Send 5 fake messages:

```powershell
python src/send_test_message.py --count 5
```

Expected: `sent 5 test message(s) to topic prod.yahoo.egx_prices.v1`.

Read them back with Kafka's own console tool:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic prod.yahoo.egx_prices.v1 --from-beginning --max-messages 5 --property print.key=true --property print.partition=true
```

Expected: 5 lines like `Partition:1  COMI.CA  {"schema_version":1,"source":"test_fake","symbol":"COMI.CA",...}`.

**This proves the broker, the topic and the producer side work.** Member 1's part is validated here.

---

## Step 6 – Start MongoDB

Skip this step if you already run MongoDB on port 27017 (for example from the starter repo); the scripts will use it as it is.

```powershell
docker compose --profile mongo up -d
```

Check: `docker compose ps` shows `egx-mongo` as `Up`.

---

## Step 7 – Run the consumer (Kafka -> MongoDB)

```powershell
python src/consumer.py --max-messages 5
```

Expected log:

```
consumer started: topic prod.yahoo.egx_prices.v1 -> market_raw.live_prices
stored COMI.CA offset=0 (stored=1, dead_letter=0)
...
stored COMI.CA offset=4 (stored=5, dead_letter=0)
```

It stops after 5 messages. Without `--max-messages` it runs until you press **Ctrl+C**.

Check MongoDB:

```powershell
python src/check_mongo.py
```

Expected: `documents : 5`, `dead letters : 0`, and the last documents printed. This script also creates the indexes.

Alternative with the Mongo shell:

```powershell
docker compose exec mongo mongosh market_raw --eval "db.live_prices.countDocuments({})"
```

(If MongoDB runs from another compose project, use that container's name.)

---

## Step 8 – Prove idempotency (no duplicates)

Run the consumer again from the start of the topic by using a new group name:

```powershell
$env:KAFKA_CONSUMER_GROUP="test-replay"
python src/consumer.py --max-messages 5
Remove-Item Env:KAFKA_CONSUMER_GROUP
python src/check_mongo.py
```

Expected: `documents` is **still 5**. The same candles were read again but overwritten (same `_id`), not duplicated.

---

## Step 9 – Prove the dead-letter handling

```powershell
python src/send_test_message.py --count 1 --bad
python src/consumer.py --max-messages 2
python src/check_mongo.py
```

Expected: one extra document in `live_prices`, and `dead letters : 1`. The consumer did not crash.

---

## Step 10 – Run the real producer (Yahoo API -> Kafka)

```powershell
python src/producer.py --once --ignore-market-hours
```

Expected log:

```
producer started: 5 symbols -> topic prod.yahoo.egx_prices.v1 on localhost:9092
round finished: delivered=5 failed=0 skipped=0
```

`--ignore-market-hours` is needed when the EGX is closed (it trades Sunday–Thursday, 10:00–14:30 Cairo time); without it the producer prints "EGX is closed" and does nothing. `skipped` greater than 0 means Yahoo returned no data for that symbol (see troubleshooting). Note: Yahoo has no intraday candles for EGX stocks, so `.env` uses `CANDLE_INTERVAL=1d` (daily candles).

Then store them: `python src/consumer.py --max-messages 5` and `python src/check_mongo.py`.

---

## Step 11 – Run everything continuously

Open two terminals (both with the virtual environment activated):

- Terminal 1: `python src/consumer.py`
- Terminal 2: `python src/producer.py` (fetches every `POLL_INTERVAL_SECONDS`, default 3600 s = 1 hour, only during market hours)

Stop each with **Ctrl+C**. The consumer finishes the current message and closes cleanly.

Note: while the day's candle is still open, Yahoo returns it with changing values. The producer may send the same candle several times with updated values; the consumer overwrites the same document, so MongoDB always holds the latest version of each candle.

---

## Step 12 – Prove "no message is lost when MongoDB is down"

1. Start the consumer: `python src/consumer.py`
2. Stop MongoDB: `docker compose stop mongo`
3. Send messages: `python src/send_test_message.py --count 3 --symbol HRHO.CA`
4. The consumer logs `MongoDB write failed ... Retrying in 5s...` repeatedly.
5. Start MongoDB: `docker compose start mongo`
6. The consumer stores the 3 messages. Check with `python src/check_mongo.py`.

---

## Step 13 – Save evidence for the repository and the instructor

Save in `docs/evidence/`:

- screenshot of `docker compose ps -a`
- output of the topic `--describe` command (step 2)
- screenshot of Kafka UI showing messages (step 3)
- consumer log and `check_mongo.py` output (steps 7, 8, 9)
- the unit test output (step 4)

Then commit:

```powershell
git checkout -b feature/kafka-live-ingestion
git add kafka-live-ingestion
git commit -m "Add Kafka live ingestion: producer, consumer, docker compose, docs"
git push -u origin feature/kafka-live-ingestion
```

Open a pull request and describe it in three bullets: what it builds, how to run it (Quick start), how to test it (unit tests + steps 5–9).

---

## Step 14 – Stop and reset

| Goal | Command |
|---|---|
| Stop, keep data | `docker compose --profile mongo --profile ui down` |
| Stop and delete all Kafka/Mongo data | `docker compose --profile mongo --profile ui down -v` |

Warning: `-v` also deletes the MongoDB volume of this compose file. Do not use it if that MongoDB holds your historical raw data.

---

## Connection to the next task

Task 2 (Spark cleaning) reads from the MongoDB collection `market_raw.live_prices`. Each document has `ingested_at` (when it arrived), so Task 2 can read only the documents newer than its last run. Agree with that pair on the field names (see `03_design_decisions.md`, section 4).

Airflow runs the producer with `python src/producer.py --once` as the ingestion task; the consumer runs continuously.
