import json
from pymongo import MongoClient


client = MongoClient("mongodb://mongo:27017/")
db = client["etl_source"]

data_dir = "/opt/airflow/data"


def load_json(filename):
    with open(f"{data_dir}/{filename}", "r", encoding="utf-8") as f:
        return json.load(f)


collections = {
    "user_sessions": "user_sessions.json",
    "event_logs": "event_logs.json",
    "support_tickets": "support_tickets.json",
    "user_recommendations": "user_recommendations.json",
    "moderation_queue": "moderation_queue.json"
}


for collection_name, filename in collections.items():
    data = load_json(filename)
    collection = db[collection_name]
    collection.delete_many({})
    if data:
        collection.insert_many(data)
    print(f"Loaded {len(data)} documents into {collection_name}")


print("MongoDB loading completed")
