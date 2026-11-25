# 1. Kafka concepts, from zero

You only need these ten terms for this task.

## Why Kafka exists in this project

Without Kafka, the script that downloads prices would write directly into MongoDB. Then:

- if MongoDB is slow or stopped, prices are lost or the script crashes;
- the download script and the storage script cannot be developed, tested or restarted independently.

With Kafka between them, the download script only has to hand messages to Kafka, and the storage script reads them when it is ready. Kafka keeps the messages on disk until they are read.

**Kafka is not** the data source (that is the API), **not** the database (that is MongoDB / SQL Server), and **not** the cleaning tool (that is Spark). It only transports and temporarily stores messages.

## The terms

| Term | Meaning in this project |
|---|---|
| **Broker** | The Kafka server. We run one in Docker (`egx-kafka`). |
| **Message** (event) | One JSON text: the latest daily candle of one stock. |
| **Topic** | A named stream of messages. Ours: `prod.yahoo.egx_prices.v1`. All price messages go in it. |
| **Producer** | A program that sends messages to a topic. Ours: `src/producer.py`. |
| **Consumer** | A program that reads messages from a topic. Ours: `src/consumer.py`. |
| **Key** | A label on each message. We use the stock symbol (`COMI.CA`). Messages with the same key always go to the same partition, so the prices of one stock stay in order. |
| **Partition** | A topic is split into parts so that several consumers can read in parallel. Ours has 3. Order is guaranteed only inside one partition (that is why the key matters). |
| **Offset** | The position number of a message inside a partition (0, 1, 2, ...). Kafka never renumbers it. |
| **Consumer group** | A name (`mongo-writer`) for a consumer. Kafka remembers, per group, the last offset that was finished ("committed"). |
| **Commit** | Telling Kafka "I finished up to this offset". After a restart the consumer continues from the committed offset. |

## How a message travels (one stock, one round)

1. Producer asks Yahoo for the latest `COMI.CA` candle.
2. Producer builds the JSON message and sends it with key `COMI.CA`.
3. Kafka hashes the key, picks a partition (for example 1), appends the message there and assigns the next offset (for example 41).
4. Kafka confirms to the producer (`acks=all`).
5. Consumer polls the topic, receives the message (partition 1, offset 41).
6. Consumer writes it into MongoDB and waits for MongoDB to confirm.
7. Only then the consumer commits offset 41 in Kafka.

If the consumer crashes between steps 6 and 7, it will read message 41 again after restart. This is called **at-least-once delivery**. To make a repeated message harmless, each MongoDB document has a unique `_id` and we use *upsert* (insert or overwrite). See `03_design_decisions.md`.

## Retention

Kafka deletes messages after a configured time. Ours: **7 days** (`retention.ms=604800000`). Within 7 days the consumer can be stopped and restarted without losing data. MongoDB is the permanent raw store.

## KRaft mode

Older Kafka needed a second program (ZooKeeper) to manage the cluster. Since Kafka 3.x "KRaft mode" lets the broker manage itself. Our compose file uses KRaft, so there is just one container.

## Two addresses: `kafka:29092` and `localhost:9092`

The broker listens on two ports:

- `localhost:9092` – for programs running **on your computer** (our Python scripts).
- `kafka:29092` – for programs running **inside Docker** (Kafka UI, the topic creation container).

Kafka tells clients which address to reconnect to ("advertised listener"). A wrong value here is the most common reason a client connects but then hangs. See `04_troubleshooting.md`.
