import json
from datetime import datetime

import psycopg2
from pymongo import MongoClient


mongo_client = MongoClient("mongodb://mongo:27017/")
mongo_db = mongo_client["etl_source"]

pg_conn = psycopg2.connect(
    host="postgres",
    port=5432,
    database="etl_project",
    user="airflow",
    password="airflow"
)
pg_conn.autocommit = True
cursor = pg_conn.cursor()


def to_datetime(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


with open("/opt/airflow/scripts/create_tables.sql", "r", encoding="utf-8") as f:
    cursor.execute(f.read())


cursor.execute("DELETE FROM support_messages")
cursor.execute("DELETE FROM support_tickets")
cursor.execute("DELETE FROM user_sessions")
cursor.execute("DELETE FROM event_logs")
cursor.execute("DELETE FROM user_recommendations")
cursor.execute("DELETE FROM moderation_queue")


user_sessions = list(mongo_db["user_sessions"].find())
for row in user_sessions:
    cursor.execute(
        """
        INSERT INTO user_sessions (
            session_id, user_id, start_time, end_time, pages_visited, device, actions
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row["session_id"],
            row["user_id"],
            to_datetime(row["start_time"]),
            to_datetime(row["end_time"]),
            row["pages_visited"],
            row["device"],
            row["actions"]
        )
    )


event_logs = list(mongo_db["event_logs"].find())
for row in event_logs:
    cursor.execute(
        """
        INSERT INTO event_logs (
            event_id, timestamp, event_type, details
        )
        VALUES (%s, %s, %s, %s)
        """,
        (
            row["event_id"],
            to_datetime(row["timestamp"]),
            row["event_type"],
            json.dumps(row["details"])
        )
    )


support_tickets = list(mongo_db["support_tickets"].find())
for row in support_tickets:
    cursor.execute(
        """
        INSERT INTO support_tickets (
            ticket_id, user_id, status, issue_type, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            row["ticket_id"],
            row["user_id"],
            row["status"],
            row["issue_type"],
            to_datetime(row["created_at"]),
            to_datetime(row["updated_at"])
        )
    )

    for message in row["messages"]:
        cursor.execute(
            """
            INSERT INTO support_messages (
                ticket_id, sender, message, timestamp
            )
            VALUES (%s, %s, %s, %s)
            """,
            (
                row["ticket_id"],
                message["sender"],
                message["message"],
                to_datetime(message["timestamp"])
            )
        )


user_recommendations = list(mongo_db["user_recommendations"].find())
for row in user_recommendations:
    cursor.execute(
        """
        INSERT INTO user_recommendations (
            user_id, recommended_products, last_updated
        )
        VALUES (%s, %s, %s)
        """,
        (
            row["user_id"],
            row["recommended_products"],
            to_datetime(row["last_updated"])
        )
    )


moderation_queue = list(mongo_db["moderation_queue"].find())
for row in moderation_queue:
    cursor.execute(
        """
        INSERT INTO moderation_queue (
            review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row["review_id"],
            row["user_id"],
            row["product_id"],
            row["review_text"],
            row["rating"],
            row["moderation_status"],
            row["flags"],
            to_datetime(row["submitted_at"])
        )
    )


cursor.close()
pg_conn.close()
mongo_client.close()

print("ETL from MongoDB to PostgreSQL completed")
