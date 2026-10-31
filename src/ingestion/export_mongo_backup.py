import os
import json
from datetime import datetime
from pymongo import MongoClient

def export_mongo_to_json():
    # 1. الاتصال بقاعدة البيانات
    client = MongoClient("mongodb://localhost:27017/")
    db = client["egx_raw_db"]
    collection = db["api_raw_responses"]
    
    print("Fetching documents from MongoDB...")
    documents = list(collection.find({}))
    
    # تحويل الـ ObjectId والـ datetime إلى صيغة نصية عشان الـ JSON يقبلهم بدون إيرور
    for doc in documents:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        
        # المرور على كل حقل في الدكشنري وتحويل أي تاريخ لـ String
        for key, value in doc.items():
            if isinstance(value, datetime):
                doc[key] = value.isoformat()
            # لو الـ raw_json_data جوه تاريخ أو صيغة معينة (احتياطياً)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        for sub_k, sub_v in item.items():
                            if isinstance(sub_v, datetime):
                                item[sub_k] = sub_v.isoformat()

    # 2. تحديد مسار الحفظ في فولدر المشروع
    # بما إننا جوه فولدر src/ingestion، هنطلع لفوق للمشروع الرئيسي ونحفظ في data/raw
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw"))
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "mongo_live_api_snapshot.json")
    
    # 3. حفظ البيانات في ملف JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=4)
        
    print(f"[SUCCESS] Exported {len(documents)} documents successfully to {output_file}!")
    
    client.close()

if __name__ == "__main__":
    export_mongo_to_json()