"""Export the live price collection to a JSON Lines file (one document per line).

Use it to share the loaded data with teammates (upload the file to the team's
Google Drive; do NOT commit it to GitHub).

  python src/export_mongo.py                       # writes data/export/live_prices.jsonl
  python src/export_mongo.py --out my_folder       # another folder
"""
from __future__ import annotations

import argparse
from pathlib import Path

from bson import json_util
from pymongo import MongoClient

from config import load_settings


def export_collection(collection, path: Path) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for doc in collection.find().sort("_id", 1):
            # json_util keeps dates and other MongoDB types, so the import restores them
            handle.write(json_util.dumps(doc, ensure_ascii=False) + "\n")
            count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--out", default="data/export", help="output folder (default data/export)")
    args = parser.parse_args()

    settings = load_settings()
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    path = out_dir / f"{settings.mongo_collection}.jsonl"
    count = export_collection(client[settings.mongo_db][settings.mongo_collection], path)
    print(f"exported {count} documents -> {path}  ({path.stat().st_size / 1024:.1f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
