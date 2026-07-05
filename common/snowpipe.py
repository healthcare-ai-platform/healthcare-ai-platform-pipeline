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

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from snowflake.ingest import SimpleIngestManager, StagedFile

from common.logger import get_logger
from common.s3 import get_object_size

log = get_logger(__name__)

_DB     = os.getenv("SNOWFLAKE_DATABASE", "HEALTHCARE")
_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA",   "RAW")

# Must match scripts/ocr.py's SILVER_PREFIX — both derive from the same env var.
_SILVER_PREFIX = os.getenv("SILVER_PREFIX", "processed")

# One pipe per (table, file extension) — all pipes created by scripts/schema.py
_PIPE_MAP: dict[tuple[str, str], str] = {
    ("ocr_extractions", ".parquet"): f"{_DB}.{_SCHEMA}.OCR_EXTRACTIONS_PIPE",
    ("ocr_results",     ".parquet"): f"{_DB}.{_SCHEMA}.OCR_RESULTS_PIPE",
}


def _private_key_pem() -> str:
    pem = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    # Fail fast with a clear error if the PEM is malformed, rather than letting
    # snowflake-ingest's SecurityManager surface a confusing internal error later.
    # snowflake-ingest's SecurityManager wants the PEM string itself — it calls
    # .encode() and load_pem_private_key() internally — so we hand back the
    # original string, not a pre-converted DER form.
    load_pem_private_key(pem.encode(), password=None)
    return pem


def _manager(pipe: str) -> SimpleIngestManager:
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    return SimpleIngestManager(
        account=account,
        host=f"{account}.snowflakecomputing.com",
        user=os.environ["SNOWFLAKE_USER"],
        pipe=pipe,
        private_key=_private_key_pem(),
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

    # The pipe's COPY INTO is scoped to @STAGE/{_SILVER_PREFIX}/{table}/ (see
    # scripts/schema.py:create_pipes()), so insertFiles wants the path *relative
    # to that*, not relative to the stage root — passing the full key makes
    # Snowflake double-prefix it and fail with "Remote file ... was not found".
    pipe_prefix = f"{_SILVER_PREFIX}/{table}/"
    if not s3_key.startswith(pipe_prefix):
        raise ValueError(f"s3_key {s3_key!r} does not start with expected pipe prefix {pipe_prefix!r}")
    relative_path = s3_key[len(pipe_prefix):]

    size = get_object_size(s3_key)
    resp = _manager(pipe).ingest_files([StagedFile(relative_path, size)])
    log.info(
        "Snowpipe notified — table=%s key=%s relative_path=%s size=%d response=%s",
        table, s3_key, relative_path, size, resp,
    )
