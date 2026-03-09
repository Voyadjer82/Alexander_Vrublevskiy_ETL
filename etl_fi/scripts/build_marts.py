import psycopg2


conn = psycopg2.connect(
    host="postgres",
    port=5432,
    database="etl_project",
    user="airflow",
    password="airflow"
)
conn.autocommit = True
cursor = conn.cursor()


cursor.execute("DROP TABLE IF EXISTS mart_user_activity")
cursor.execute("DROP TABLE IF EXISTS mart_support_stats")


cursor.execute(
    """
    CREATE TABLE mart_user_activity AS
    SELECT
        user_id,
        COUNT(*) AS sessions_count,
        AVG(EXTRACT(EPOCH FROM (end_time - start_time))) AS avg_session_duration,
        COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_sessions,
        COUNT(*) FILTER (WHERE device = 'desktop') AS desktop_sessions,
        COUNT(*) FILTER (WHERE device = 'tablet') AS tablet_sessions
    FROM user_sessions
    GROUP BY user_id
    """
)

cursor.execute(
    """
    CREATE TABLE mart_support_stats AS
    SELECT
        issue_type,
        status,
        COUNT(*) AS tickets_count,
        AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 60.0) AS avg_resolution_minutes
    FROM support_tickets
    GROUP BY issue_type, status
    """
)


cursor.close()
conn.close()

print("Marts built successfully")
