"""CONSUMER: Kafka topic  ->  MongoDB (raw / bronze).

Reads messages from the topic and stores each one as a document in MongoDB.

Safety rules
  1. A message is "committed" (marked as done in Kafka) ONLY after MongoDB
     confirmed the write. If MongoDB is down, the message is retried, never lost.
  2. The MongoDB _id is symbol|interval|bar_time_utc and we use upsert, so a
     message delivered twice does not create a duplicate document.
  3. A message that is not valid JSON or lacks required fields goes to the
     dead-letter collection instead of crashing the consumer.
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, TopicPartition
from pymongo import MongoClient
from pymongo.errors import PyMongoError

import messages
from config import load_settings

log = logging.getLogger("consumer")
_running = True


def _stop(signum, frame):
    global _running
    _running = False
    log.info("stop requested...")


def store_message(db, settings, msg) -> str:
    """Write one Kafka message to MongoDB. Returns 'stored' or 'dead_letter'."""
    kafka_info = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
    }
    now = datetime.now(timezone.utc)
    try:
        payload = messages.decode(msg.value())
        problems = messages.validate_message(payload)
        if problems:
            raise ValueError("; ".join(problems))
    except ValueError as exc:
        db[settings.mongo_dead_letter_collection].insert_one(
            {
                "raw_value": msg.value().decode("utf-8", errors="replace"),
                "error": str(exc),
                "kafka": kafka_info,
                "ingested_at": now,
            }
        )
        log.warning("bad message sent to dead letter (offset %d): %s", msg.offset(), exc)
        return "dead_letter"

    doc = dict(payload)
    doc["_id"] = messages.make_doc_id(payload)
    doc["ingested_at"] = now
    doc["kafka"] = kafka_info
    db[settings.mongo_collection].replace_one({"_id": doc["_id"]}, doc, upsert=True)
    return "stored"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--max-messages", type=int, default=0,
                        help="stop after N messages (0 = run until Ctrl+C)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = load_settings()
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")  # fail immediately if MongoDB is not reachable
    db = client[settings.mongo_db]

    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group,
            "client.id": "mongo-writer-1",
            "auto.offset.reset": "earliest",   # a new group starts from the oldest message
            "enable.auto.commit": False,       # we commit ourselves, after MongoDB confirms
        }
    )
    consumer.subscribe([settings.topic])
    signal.signal(signal.SIGINT, _stop)
    log.info("consumer started: topic %s -> %s.%s",
             settings.topic, settings.mongo_db, settings.mongo_collection)

    counts = {"stored": 0, "dead_letter": 0}
    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue
            try:
                result = store_message(db, settings, msg)
            except PyMongoError as exc:
                log.error("MongoDB write failed (%s). Retrying in 5s...", exc)
                time.sleep(5)
                # go back to this message so it is read again
                consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                continue
            consumer.commit(message=msg, asynchronous=False)
            counts[result] += 1
            log.info("%s %s offset=%d (stored=%d, dead_letter=%d)",
                     result, (msg.key() or b"").decode(), msg.offset(),
                     counts["stored"], counts["dead_letter"])
            if args.max_messages and sum(counts.values()) >= args.max_messages:
                break
    finally:
        consumer.close()
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
