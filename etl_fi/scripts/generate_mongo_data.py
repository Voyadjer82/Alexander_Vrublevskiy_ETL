import json
import os
import random
from datetime import datetime, timedelta


data_dir = "/opt/airflow/data"
os.makedirs(data_dir, exist_ok=True)

random.seed(42)


def random_datetime(start, end):
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def dt_to_str(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


start_date = datetime(2024, 1, 1, 8, 0, 0)
end_date = datetime(2024, 1, 10, 20, 0, 0)

users = [f"user_{i}" for i in range(1, 21)]
products = [f"prod_{i}" for i in range(1, 31)]
pages = ["/home", "/catalog", "/product", "/cart", "/profile", "/support"]
actions_list = ["login", "view_product", "add_to_cart", "logout", "search", "checkout"]
event_types = ["click", "view", "login", "logout", "purchase"]
issue_types = ["payment", "delivery", "account", "refund"]
ticket_statuses = ["open", "closed", "in_progress"]
moderation_statuses = ["pending", "approved", "rejected"]
flags_list = ["contains_images", "spam", "offensive", "duplicate"]


user_sessions = []
for i in range(1, 101):
    start_time = random_datetime(start_date, end_date)
    end_time = start_time + timedelta(minutes=random.randint(5, 90))

    session = {
        "session_id": f"sess_{i:03d}",
        "user_id": random.choice(users),
        "start_time": dt_to_str(start_time),
        "end_time": dt_to_str(end_time),
        "pages_visited": random.sample(pages, random.randint(2, 5)),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "actions": random.sample(actions_list, random.randint(2, 5))
    }
    user_sessions.append(session)


event_logs = []
for i in range(1, 121):
    event_time = random_datetime(start_date, end_date)

    event = {
        "event_id": f"evt_{i:03d}",
        "timestamp": dt_to_str(event_time),
        "event_type": random.choice(event_types),
        "details": {
            "user_id": random.choice(users),
            "page": random.choice(pages),
            "product_id": random.choice(products)
        }
    }
    event_logs.append(event)


support_tickets = []
for i in range(1, 41):
    created_at = random_datetime(start_date, end_date)
    updated_at = created_at + timedelta(hours=random.randint(1, 48))

    messages = []
    messages_count = random.randint(2, 4)

    current_time = created_at
    for j in range(messages_count):
        sender = "user" if j % 2 == 0 else "support"
        current_time = current_time + timedelta(minutes=random.randint(10, 180))
        messages.append(
            {
                "sender": sender,
                "message": f"Message {j + 1} for ticket {i}",
                "timestamp": dt_to_str(current_time)
            }
        )

    ticket = {
        "ticket_id": f"ticket_{i:03d}",
        "user_id": random.choice(users),
        "status": random.choice(ticket_statuses),
        "issue_type": random.choice(issue_types),
        "messages": messages,
        "created_at": dt_to_str(created_at),
        "updated_at": dt_to_str(updated_at)
    }
    support_tickets.append(ticket)


user_recommendations = []
for user in users:
    rec = {
        "user_id": user,
        "recommended_products": random.sample(products, 3),
        "last_updated": dt_to_str(random_datetime(start_date, end_date))
    }
    user_recommendations.append(rec)


moderation_queue = []
for i in range(1, 61):
    review = {
        "review_id": f"rev_{i:03d}",
        "user_id": random.choice(users),
        "product_id": random.choice(products),
        "review_text": f"Review text {i}",
        "rating": random.randint(1, 5),
        "moderation_status": random.choice(moderation_statuses),
        "flags": random.sample(flags_list, random.randint(1, 2)),
        "submitted_at": dt_to_str(random_datetime(start_date, end_date))
    }
    moderation_queue.append(review)


with open(os.path.join(data_dir, "user_sessions.json"), "w", encoding="utf-8") as f:
    json.dump(user_sessions, f, ensure_ascii=False, indent=2)

with open(os.path.join(data_dir, "event_logs.json"), "w", encoding="utf-8") as f:
    json.dump(event_logs, f, ensure_ascii=False, indent=2)

with open(os.path.join(data_dir, "support_tickets.json"), "w", encoding="utf-8") as f:
    json.dump(support_tickets, f, ensure_ascii=False, indent=2)

with open(os.path.join(data_dir, "user_recommendations.json"), "w", encoding="utf-8") as f:
    json.dump(user_recommendations, f, ensure_ascii=False, indent=2)

with open(os.path.join(data_dir, "moderation_queue.json"), "w", encoding="utf-8") as f:
    json.dump(moderation_queue, f, ensure_ascii=False, indent=2)

print("Data generated successfully")
