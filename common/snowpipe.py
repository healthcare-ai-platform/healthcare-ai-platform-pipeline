"""
Snowpipe REST API client.

After a file lands in S3, call notify() to tell Snowpipe to load it immediately.
Snowpipe picks it up within seconds — no polling, no cron.

Setup required (one-time in Snowflake):
    ALTER USER <user> SET RSA_PUBLIC_KEY = '<your_public_key>';

Then set SNOWFLAKE_PRIVATE_KEY in your env to the PEM content of the private key.
"""

import os
from pathlib import Path

from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)
from snowflake.ingest import SimpleIngestManager, StagedFile

from common.logger import get_logger

log = get_logger(__name__)

_DB     = os.getenv("SNOWFLAKE_DATABASE", "HEALTHCARE")
_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA",   "RAW")

# One pipe per (table, file extension) — all pipes created by scripts/schema.py
_PIPE_MAP: dict[tuple[str, str], str] = {
    ("ocr_extractions", ".parquet"): f"{_DB}.{_SCHEMA}.OCR_EXTRACTIONS_PIPE",
    ("ocr_results",     ".parquet"): f"{_DB}.{_SCHEMA}.OCR_RESULTS_PIPE",
}


def _private_key_der() -> bytes:
    pem = os.environ["SNOWFLAKE_PRIVATE_KEY"].encode()
    key = load_pem_private_key(pem, password=None)
    return key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def _manager(pipe: str) -> SimpleIngestManager:
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    return SimpleIngestManager(
        account=account,
        host=f"{account}.snowflakecomputing.com",
        user=os.environ["SNOWFLAKE_USER"],
        pipe=pipe,
        private_key=_private_key_der(),
    )


def notify(s3_key: str, table: str) -> None:
    """
    Notify Snowpipe to load s3_key into table.
    s3_key is relative to the S3 stage root (same as the key in the bucket).
    Snowpipe accepts the file and loads it asynchronously within seconds.
    """
    ext  = Path(s3_key).suffix.lower()
    pipe = _PIPE_MAP.get((table, ext))
    if pipe is None:
        raise ValueError(f"No Snowpipe configured for table={table!r} ext={ext!r}")

    resp = _manager(pipe).ingest_files([StagedFile(s3_key)])
    log.info("Snowpipe notified — table=%s key=%s response=%s", table, s3_key, resp)
