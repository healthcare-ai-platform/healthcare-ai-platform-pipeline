import os

from dotenv import load_dotenv

load_dotenv()  # must run before local imports so env vars are set when modules load

from common.db import get_connection
from common.s3 import S3_BUCKET
from common.warehouse import get_warehouse_session


# ── PostgreSQL (OLTP) — pipeline operational metadata ─────────────────────────

def create_postgres_raw_schema():
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


# ── Snowflake (warehouse) — analytics tables ──────────────────────────────────

def create_warehouse():
    with get_warehouse_session() as session:
        session.sql("""
            CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
                WAREHOUSE_SIZE    = 'X-SMALL'
                AUTO_SUSPEND      = 60
                AUTO_RESUME       = TRUE
                INITIALLY_SUSPENDED = TRUE
        """).collect()


def create_database():
    with get_warehouse_session() as session:
        session.sql("CREATE DATABASE IF NOT EXISTS HEALTHCARE").collect()


def create_raw_schema():
    with get_warehouse_session() as session:
        session.sql("CREATE SCHEMA IF NOT EXISTS RAW").collect()


def create_stage():
    """
    External stage required by Snowpipe — always created.
    Uses a storage integration when available (preferred); falls back to inline
    AWS credentials otherwise.
    """
    with get_warehouse_session() as session:
        integration = os.getenv("SNOWFLAKE_STORAGE_INTEGRATION", "")
        if integration:
            session.sql(f"""
                CREATE STAGE IF NOT EXISTS HEALTHCARE.RAW.S3_STAGE
                    URL                 = 's3://{S3_BUCKET}/'
                    STORAGE_INTEGRATION = {integration}
                    FILE_FORMAT         = (TYPE = PARQUET)
            """).collect()
        else:
            key_id = os.environ["AWS_ACCESS_KEY_ID"]
            secret = os.environ["AWS_SECRET_ACCESS_KEY"]
            session.sql(f"""
                CREATE STAGE IF NOT EXISTS HEALTHCARE.RAW.S3_STAGE
                    URL         = 's3://{S3_BUCKET}/'
                    CREDENTIALS = (AWS_KEY_ID='{key_id}' AWS_SECRET_KEY='{secret}')
                    FILE_FORMAT = (TYPE = PARQUET)
            """).collect()


def create_pipes():
    """Snowpipe definitions — one pipe per OCR output table."""
    with get_warehouse_session() as session:
        session.sql("""
            CREATE PIPE IF NOT EXISTS HEALTHCARE.RAW.OCR_EXTRACTIONS_PIPE
                COMMENT = 'Snowpipe: silver Parquet → RAW.OCR_EXTRACTIONS'
            AS
            COPY INTO HEALTHCARE.RAW.OCR_EXTRACTIONS
            FROM @HEALTHCARE.RAW.S3_STAGE/processed/ocr_extractions/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """).collect()

        session.sql("""
            CREATE PIPE IF NOT EXISTS HEALTHCARE.RAW.OCR_RESULTS_PIPE
                COMMENT = 'Snowpipe: silver Parquet → RAW.OCR_RESULTS'
            AS
            COPY INTO HEALTHCARE.RAW.OCR_RESULTS
            FROM @HEALTHCARE.RAW.S3_STAGE/processed/ocr_results/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """).collect()


def create_raw_data_table():
    with get_warehouse_session() as session:
        session.sql("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_DATA (
                ingestion_id    BIGINT AUTOINCREMENT,
                s3_key          VARCHAR(1024),
                ingested_at     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()


def _enable_schema_evolution(session, table: str) -> None:
    """
    Let Snowpipe (MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE, see create_pipes())
    add new columns automatically instead of silently dropping any column in the
    incoming Parquet file that doesn't already exist on the table.

    Runs unconditionally, even right after CREATE TABLE IF NOT EXISTS, because
    that CREATE is a no-op on a table that already exists — this ALTER is what
    actually applies the setting to tables created before this was added.
    """
    role = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
    session.sql(f"ALTER TABLE {table} SET ENABLE_SCHEMA_EVOLUTION = TRUE").collect()
    session.sql(f"GRANT EVOLVE SCHEMA ON TABLE {table} TO ROLE {role}").collect()


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
            ENABLE_SCHEMA_EVOLUTION = TRUE
        """).collect()
        _enable_schema_evolution(session, "RAW.OCR_EXTRACTIONS")


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
            ENABLE_SCHEMA_EVOLUTION = TRUE
        """).collect()
        _enable_schema_evolution(session, "RAW.OCR_RESULTS")


def verify():
    """Print a quick health-check of the Snowflake environment."""
    with get_warehouse_session() as session:
        checks = {
            "warehouse":  "SHOW WAREHOUSES",
            "database":   "SHOW DATABASES",
            "schema":     "SHOW SCHEMAS IN DATABASE HEALTHCARE",
            "tables":     "SHOW TABLES IN SCHEMA HEALTHCARE.RAW",
        }
        for label, sql in checks.items():
            rows = session.sql(sql).collect()
            names = [r["name"] for r in rows]
            print(f"  {label:10s}: {names}")


def init():
    # PostgreSQL — pipeline metadata tables
    create_postgres_raw_schema()
    create_tracker_table()
    create_document_events_table()

    # Snowflake — order matters: WH → DB → schema → tables → stage → pipes
    create_warehouse()
    create_database()
    create_raw_schema()
    create_raw_data_table()
    create_ocr_extractions_table()
    create_ocr_results_table()
    create_stage()   # stage must exist before pipes
    create_pipes()   # pipes reference stage + tables
    print("Schema initialised.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        verify()
    else:
        init()
