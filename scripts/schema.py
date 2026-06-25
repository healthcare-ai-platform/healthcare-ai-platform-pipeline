from common.db import get_connection
from common.warehouse import get_warehouse_session


# ── PostgreSQL (OLTP) — pipeline operational metadata ─────────────────────────

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


# ── Snowflake (warehouse) — analytics tables ──────────────────────────────────

def create_raw_schema():
    with get_warehouse_session() as session:
        session.sql("CREATE SCHEMA IF NOT EXISTS RAW").collect()


def create_raw_data_table():
    with get_warehouse_session() as session:
        session.sql("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_DATA (
                ingestion_id    BIGINT AUTOINCREMENT,
                s3_key          VARCHAR(1024),
                ingested_at     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()


def create_ocr_extractions_table():
    """One row per document — patient demographics + report metadata."""
    with get_warehouse_session() as session:
        session.sql("""
            CREATE TABLE IF NOT EXISTS RAW.OCR_EXTRACTIONS (
                document_id             VARCHAR(36)     NOT NULL,
                tenant_id               VARCHAR(256)    NOT NULL,
                report_type             VARCHAR(128),
                patient_name            VARCHAR(512),
                patient_external_id     VARCHAR(256),
                patient_dob             VARCHAR(10),
                patient_gender          VARCHAR(16),
                report_date             VARCHAR(10),
                doctor                  VARCHAR(512),
                facility                VARCHAR(512),
                extraction_status       VARCHAR(32),
                extraction_confidence   FLOAT,
                extracted_at            TIMESTAMP_TZ
            )
        """).collect()


def create_ocr_results_table():
    """One row per test result — normalised lab values from each document."""
    with get_warehouse_session() as session:
        session.sql("""
            CREATE TABLE IF NOT EXISTS RAW.OCR_RESULTS (
                document_id     VARCHAR(36)     NOT NULL,
                tenant_id       VARCHAR(256)    NOT NULL,
                test_name       VARCHAR(512),
                value           VARCHAR(64),
                unit            VARCHAR(64),
                reference_range VARCHAR(128),
                flag            VARCHAR(16),
                extracted_at    TIMESTAMP_TZ
            )
        """).collect()


def init():
    # PostgreSQL — pipeline metadata tables
    create_tracker_table()
    create_document_events_table()

    # Snowflake — warehouse analytics tables
    create_raw_schema()
    create_raw_data_table()
    create_ocr_extractions_table()
    create_ocr_results_table()
    print("Schema initialised.")


if __name__ == "__main__":
    init()
