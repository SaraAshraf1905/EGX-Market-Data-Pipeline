"""Check what the consumer stored in MongoDB (and create useful indexes).

  python src/check_mongo.py
"""
from __future__ import annotations

from pymongo import ASCENDING, MongoClient

from config import load_settings


def main() -> int:
    settings = load_settings()
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    col = client[settings.mongo_db][settings.mongo_collection]
    dead = client[settings.mongo_db][settings.mongo_dead_letter_collection]

    # Indexes: _id (symbol|interval|bar_time_utc) is already unique by default.
    col.create_index([("ingested_at", ASCENDING)], name="ix_ingested_at")
    col.create_index([("symbol", ASCENDING), ("bar_time_utc", ASCENDING)], name="ix_symbol_bar_time")

    print(f"database={settings.mongo_db} collection={settings.mongo_collection}")
    print("documents      :", col.count_documents({}))
    print("dead letters   :", dead.count_documents({}))
    print("\ndocuments per symbol:")
    for row in col.aggregate([{"$group": {"_id": "$symbol", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]):
        print(f"  {row['_id']:<10} {row['n']}")
    print("\nlast 3 documents:")
    for doc in col.find().sort("ingested_at", -1).limit(3):
        print(" ", doc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
