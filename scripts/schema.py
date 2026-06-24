from common.db import get_connection


def create_raw_schema():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")


def create_tracker_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.ingestion_tracker (
                    id          SERIAL PRIMARY KEY,
                    s3_key      TEXT NOT NULL UNIQUE,
                    status      TEXT NOT NULL DEFAULT 'loaded',
                    row_count   INTEGER,
                    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    error_msg   TEXT
                )
            """)


def create_document_events_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.document_events (
                    id          SERIAL PRIMARY KEY,
                    document_id TEXT NOT NULL UNIQUE,
                    tenant_id   TEXT,
                    user_id     TEXT,
                    source      TEXT,
                    report_type TEXT,
                    s3_path     TEXT,
                    uploaded_at TIMESTAMPTZ,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)


def create_raw_data_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.raw_data (
                    ingestion_id    BIGINT IDENTITY(1,1),
                    s3_key          VARCHAR(1024),
                    ingested_at     TIMESTAMPTZ DEFAULT SYSDATE
                )
            """)


def init():
    create_raw_schema()
    create_tracker_table()
    create_document_events_table()
    create_raw_data_table()
    print("Schema initialised.")


if __name__ == "__main__":
    init()
