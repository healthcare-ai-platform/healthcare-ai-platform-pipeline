"""
Kafka consumer for the pipeline ingestion layer.

Flow:
  Backend publishes event to "healthai.report.received"
      → consumer reads the event
      → saves document metadata into raw.document_events (warehouse)
      → triggers ingest of the S3 file into raw warehouse tables

Run from pipeline root:
    python -m kafka.consumer
"""

import json
import signal
import sys
from pathlib import Path

from kafka import KafkaConsumer  # kafka-python
from kafka.errors import KafkaError

from common.db import get_connection
from common.logger import get_logger
from common.snowpipe import notify as snowpipe_notify
from kafka.config import BOOTSTRAP_SERVERS, CONSUMER_GROUP, TOPIC_REPORT_RECEIVED
from scripts.ingest import ingest_file
from scripts.ocr import extract_pdf

log = get_logger(__name__)

_running = True


def _handle_shutdown(sig, frame):
    global _running
    log.info("Shutdown signal received — draining and stopping…")
    _running = False


def _parse_s3_key(s3_path: str) -> str:
    """Convert 's3://bucket-name/raw/tenant/...' → 'raw/tenant/...'"""
    # s3_path format: s3://<bucket>/<key>
    parts = s3_path.replace("s3://", "").split("/", 1)
    return parts[1] if len(parts) == 2 else s3_path


def _save_event(payload: dict):
    """Persist the raw Kafka event metadata into the warehouse."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.document_events (
                    document_id, tenant_id, user_id, source,
                    report_type, s3_path, uploaded_at, received_at
                ) VALUES (
                    %(document_id)s, %(tenant_id)s, %(user_id)s, %(source)s,
                    %(report_type)s, %(s3_path)s, %(uploaded_at)s, NOW()
                )
                ON CONFLICT (document_id) DO NOTHING
                """,
                payload,
            )


_STRUCTURED_EXTS = {".csv", ".json", ".jsonl", ".parquet"}


def _process(payload: dict):
    document_id = payload.get("document_id", "unknown")
    s3_path     = payload.get("s3_path", "")
    report_type = payload.get("report_type", "raw_data")
    tenant_id   = payload.get("tenant_id", "unknown")

    log.info("[%s] Processing event — report_type=%s s3_path=%s", document_id, report_type, s3_path)

    # 1. Save event metadata to the warehouse — always, regardless of file type
    _save_event(payload)
    log.info("[%s] Event metadata saved to raw.document_events", document_id)

    s3_key = _parse_s3_key(s3_path)
    ext    = Path(s3_key).suffix.lower()

    if ext in _STRUCTURED_EXTS:
        # 2a. Structured file — COPY directly from S3 bronze into the warehouse
        try:
            ingest_file(s3_key, table=report_type)
            log.info("[%s] Ingested s3://%s into raw.%s", document_id, s3_key, report_type)
        except Exception as e:
            log.error("[%s] Ingestion failed (event still saved): %s", document_id, e)

    elif ext == ".pdf":
        # 2b. PDF — LangChain agent extracts patient + report + test results,
        #     writes two silver Parquets to S3, then Snowpipe loads them async.
        try:
            summary_key, results_key = extract_pdf(s3_key, document_id, tenant_id, report_type)
            snowpipe_notify(summary_key, table="ocr_extractions")
            snowpipe_notify(results_key, table="ocr_results")
            log.info("[%s] OCR complete — Snowpipe notified for ocr_extractions + ocr_results", document_id)
        except Exception as e:
            log.error("[%s] OCR/Snowpipe failed (event still saved): %s", document_id, e)

    else:
        log.warning("[%s] Unknown file type '%s' — skipping ingestion", document_id, ext)


def run():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    log.info("Connecting to Kafka at %s…", BOOTSTRAP_SERVERS)

    consumer = KafkaConsumer(
        TOPIC_REPORT_RECEIVED,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",   # start from beginning if no committed offset
        enable_auto_commit=False,        # manual commit — only after successful processing
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )

    log.info("Listening on topic '%s' (group=%s)…", TOPIC_REPORT_RECEIVED, CONSUMER_GROUP)

    try:
        while _running:
            # poll() returns immediately if no messages; timeout keeps the loop responsive
            records = consumer.poll(timeout_ms=1_000)

            for partition, messages in records.items():
                for message in messages:
                    try:
                        _process(message.value)
                        # Commit only after the message is fully processed
                        consumer.commit()
                    except Exception as e:
                        log.error(
                            "Unhandled error on partition=%s offset=%s: %s",
                            message.partition, message.offset, e,
                        )
                        # Don't commit — message will be re-delivered on restart
    except KafkaError as e:
        log.error("Kafka connection error: %s", e)
        sys.exit(1)
    finally:
        consumer.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    run()
