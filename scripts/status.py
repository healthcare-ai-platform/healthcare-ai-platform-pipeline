"""
One-shot pipeline health check — Kafka consumer lag, recent document statuses,
and Snowflake row counts, all in one place.

Run from pipeline root:
    python -m scripts.status
"""

import subprocess

from dotenv import load_dotenv

load_dotenv()  # must run before local imports so env vars are set when modules load

from common.db import get_connection
from common.warehouse import get_warehouse_session


def _print_header(title: str) -> None:
    print(f"\n=== {title} ===")


def check_kafka() -> None:
    _print_header("Kafka — consumer group lag")
    try:
        result = subprocess.run(
            [
                "docker", "exec", "healthai-kafka",
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server", "localhost:9092",
                "--describe", "--group", "pipeline-ingestion",
            ],
            capture_output=True, text=True, timeout=15,
        )
        print(result.stdout.strip() or "(no output — consumer group may not exist yet)")
        if result.stderr.strip():
            print(result.stderr.strip())
    except Exception as e:
        print(f"Could not reach Kafka: {e}")


def check_documents() -> None:
    _print_header("Postgres — most recent documents")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT document_id, status, error_reason, retry_count, updated_at
                FROM documents
                ORDER BY updated_at DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
            if not rows:
                print("No documents found.")
                return
            for document_id, status, error_reason, retry_count, updated_at in rows:
                line = f"{updated_at}  {status:10s}  {document_id}"
                if error_reason:
                    line += f"  — {error_reason[:80]}"
                print(line)


def check_snowflake() -> None:
    _print_header("Snowflake — row counts")
    try:
        with get_warehouse_session() as session:
            for table in ["OCR_EXTRACTIONS", "OCR_RESULTS", "RAW_DATA"]:
                n = session.sql(f"SELECT COUNT(*) AS n FROM HEALTHCARE.RAW.{table}").collect()[0]["N"]
                print(f"{table}: {n} rows")
    except Exception as e:
        print(f"Could not reach Snowflake: {e}")


if __name__ == "__main__":
    check_kafka()
    check_documents()
    check_snowflake()
    print()
