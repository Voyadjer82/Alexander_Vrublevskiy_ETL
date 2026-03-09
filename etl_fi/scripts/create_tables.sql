CREATE TABLE IF NOT EXISTS user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    pages_visited TEXT[],
    device TEXT,
    actions TEXT[]
);

CREATE TABLE IF NOT EXISTS event_logs (
    event_id TEXT PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    event_type TEXT NOT NULL,
    details TEXT
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS support_messages (
    id SERIAL PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    sender TEXT NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (ticket_id) REFERENCES support_tickets(ticket_id)
);

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id TEXT PRIMARY KEY,
    recommended_products TEXT[],
    last_updated TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS moderation_queue (
    review_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    review_text TEXT NOT NULL,
    rating INTEGER NOT NULL,
    moderation_status TEXT NOT NULL,
    flags TEXT[],
    submitted_at TIMESTAMP NOT NULL
);
