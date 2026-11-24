"""Import a JSON Lines file made by export_mongo.py into YOUR MongoDB.

Safe to run more than once: documents are matched by _id and overwritten, so
no duplicates are created.

  python src/import_mongo.py path\to\live_prices.jsonl
  python src/import_mongo.py path\to\live_prices.jsonl --dry-run   # only count, write nothing
"""
from __future__ import annotations

import argparse
from pathlib import Path

from bson import json_util
from pymongo import MongoClient, ReplaceOne

from config import load_settings

BATCH_SIZE = 500


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("file", help="the .jsonl file to import")
    parser.add_argument("--dry-run", action="store_true", help="count the documents, write nothing")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.is_file():
        raise SystemExit(f"file not found: {path}")

    settings = load_settings()
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    collection = client[settings.mongo_db][settings.mongo_collection]

    total, batch = 0, []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            doc = json_util.loads(line)
            total += 1
            if not args.dry_run:
                batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
                if len(batch) >= BATCH_SIZE:
                    collection.bulk_write(batch, ordered=False)
                    batch.clear()
    if batch:
        collection.bulk_write(batch, ordered=False)

    action = "would import" if args.dry_run else "imported"
    print(f"{action} {total} documents into {settings.mongo_db}.{settings.mongo_collection}")
    if not args.dry_run:
        print("documents now in the collection:", collection.count_documents({}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
