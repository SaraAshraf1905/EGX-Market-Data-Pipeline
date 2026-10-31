from datetime import datetime, timezone
from pymongo import MongoClient, errors

class MongoRawLoader:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="egx_raw_db"):
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[db_name]
        
    def create_indexes(self, collection_name="api_raw_responses"):
        collection = self.db[collection_name]
        collection.create_index([
            ("source", 1),
            ("symbol", 1),
            ("session_date", 1)
        ], unique=True)
        print(f"Indexes ensured on collection: {collection_name}")

    def load_api_response(self, source: str, symbol: str, session_date: str, 
                          raw_json_data: dict | list, mode: str = "incremental", 
                          run_id: str = None, collection_name: str = "api_raw_responses"):
        collection = self.db[collection_name]
        
        document = {
            "source": source,                        
            "symbol": symbol,                        
            "session_date": session_date,            
            "extraction_mode": mode,                 
            "airflow_run_id": run_id,                
            "ingested_at": datetime.now(timezone.utc), 
            "record_count": len(raw_json_data) if isinstance(raw_json_data, list) else 1,
            "raw_payload": raw_json_data             
        }

        try:
            filter_query = {
                "source": source,
                "symbol": symbol,
                "session_date": session_date
            }
            
            result = collection.update_one(
                filter_query,
                {"$set": document},
                upsert=True
            )
            
            if result.upserted_id:
                print(f"[INSERTED] New raw document for {symbol} on {session_date}")
            else:
                print(f"[UPDATED] Overwrote existing raw document for {symbol} on {session_date}")
                
            return True

        except errors.PyMongoError as e:
            print(f"[ERROR] Failed to load data into MongoDB for {symbol}: {e}")
            raise e

    def close(self):
        self.client.close()