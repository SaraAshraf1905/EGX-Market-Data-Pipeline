# 4. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `error during connect ... docker daemon is not running` | Docker Desktop is not running | Start Docker Desktop, wait for it to say "running", retry |
| `Bind for 0.0.0.0:9092 failed: port is already allocated` | Another Kafka (or program) uses 9092 | `docker ps` to find it, stop it, or change the left number in `ports:` and in `KAFKA_ADVERTISED_LISTENERS` / `.env` |
| `Bind ... 27017 ... already allocated` | MongoDB is already running | Do not start the `mongo` profile; use the existing MongoDB (`MONGO_URI` in `.env`) |
| `egx-kafka` stays `unhealthy` / restarts | Wrong cluster id, old data in the volume, or not enough memory | `docker compose logs kafka`; if it mentions "cluster id", run `docker compose down -v` and start again; give Docker Desktop at least 2 GB RAM |
| `kafka-init` exited with code 1 | Broker was not ready or topic name invalid | `docker compose logs kafka-init`; run `docker compose up -d` again |
| Python: `Failed to resolve 'kafka:29092'` or the script hangs after connecting | The script uses the Docker address | In `.env` use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` |
| `KafkaError ... UNKNOWN_TOPIC_OR_PART` | Topic does not exist (auto creation is off on purpose) | `docker compose up -d` (runs `kafka-init`), or check the topic name in `.env` |
| `ModuleNotFoundError: confluent_kafka` / `pymongo` | Virtual environment not active or packages not installed | `.venv\Scripts\Activate.ps1` then `pip install -r requirements.txt` |
| `ZoneInfoNotFoundError: Africa/Cairo` | No time zone database on Windows | `pip install tzdata` |
| Producer prints "EGX is closed" | Outside Sun–Thu 10:00–14:30 Cairo time | Add `--ignore-market-hours` to test |
| Producer: `no data from Yahoo, skipped` | Wrong ticker, rate limit, or no intraday data for that ticker | Run `python src/check_yahoo.py COMI.CA` to see which candle sizes exist (EGX stocks: only `1d`, so use `CANDLE_INTERVAL=1d`); wait a few minutes if rate-limited; remove bad tickers from `config/symbols.txt` |
| Consumer: `ServerSelectionTimeoutError` | MongoDB not running or wrong `MONGO_URI` | `docker compose ps`; start MongoDB; check `.env` |
| Consumer prints nothing | No new messages, or the group already read everything | Send test messages; to read again use a new group: `$env:KAFKA_CONSUMER_GROUP="another-name"` |
| Documents appear twice | Different `_id`s for the same candle | Check that `bar_time_utc` is identical; the `_id` is `symbol\|interval\|bar_time_utc` |
| `dead letters` increases | Messages that are not JSON or miss required fields | Inspect: `db.live_prices_dead_letter.find()`; the `error` field says why |

## Yahoo returns no data for every symbol ("possibly delisted; no price data found")

The tickers are usually fine. Most likely Yahoo has **no intraday candles** for them. Test which candle sizes exist:

```powershell
python src/check_yahoo.py
```

(It tests several tickers and candle sizes and does not stop at the first error. A `KeyError: 'tradingPeriods'` from yfinance for `5m` also means Yahoo has no intraday data for that ticker.)

- If `1d` returns rows but `5m` returns 0: put `CANDLE_INTERVAL=1d` in `.env` (daily candles).
- If everything returns 0: wait a few minutes (rate limit) and try again, or check your internet / proxy.

## Useful commands

```powershell
docker compose ps -a                          # state of containers
docker compose logs -f kafka                  # live Kafka log
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --describe --group mongo-writer
```

The last command shows, per partition, the **current offset** (what the consumer committed), the **log end offset** (newest message) and **LAG** (messages waiting). `LAG 0` means the consumer is fully caught up.
