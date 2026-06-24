import argparse
import os
from pathlib import Path

from common.db import get_connection
from common.logger import get_logger
from common.s3 import S3_BUCKET, list_s3_files
from scripts.loaders import load_csv, load_json, load_parquet
from scripts.tracker import is_loaded, mark_failed, mark_loaded

log = get_logger(__name__)

LOADERS = {
    ".csv": load_csv,
    ".parquet": load_parquet,
    ".json": load_json,
    ".jsonl": load_json,
}

# Format clauses for Redshift COPY — keyed by file extension
_REDSHIFT_FORMAT = {
    ".csv": "FORMAT AS CSV IGNOREHEADER 1",
    ".parquet": "FORMAT AS PARQUET",
    ".json": "FORMAT AS JSON 'auto'",
    ".jsonl": "FORMAT AS JSON 'auto'",
}


def redshift_copy_from_s3(s3_key: str, table: str, schema: str = "raw") -> int:
    """Issue a Redshift COPY command that loads directly from S3."""
    ext = Path(s3_key).suffix.lower()
    fmt = _REDSHIFT_FORMAT.get(ext)
    if fmt is None:
        raise ValueError(f"Unsupported file type for Redshift COPY: {ext}")

    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
    iam_role = os.getenv("REDSHIFT_IAM_ROLE", "")

    if iam_role:
        auth = f"IAM_ROLE '{iam_role}'"
    else:
        key_id = os.environ["AWS_ACCESS_KEY_ID"]
        secret = os.environ["AWS_SECRET_ACCESS_KEY"]
        auth = f"ACCESS_KEY_ID '{key_id}' SECRET_ACCESS_KEY '{secret}'"

    sql = (
        f"COPY {schema}.{table} "
        f"FROM '{s3_uri}' "
        f"{auth} "
        f"{fmt} "
        f"COMPUPDATE OFF STATUPDATE OFF"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute("SELECT pg_last_copy_count()")
            row_count = cur.fetchone()[0]

    log.info("Redshift COPY loaded %d rows from %s into %s.%s", row_count, s3_key, schema, table)
    return row_count


def ingest_file(s3_key: str, table: str):
    if is_loaded(s3_key):
        log.info("Skipping already-loaded file: %s", s3_key)
        return

    ext = Path(s3_key).suffix.lower()

    # Use native Redshift COPY when a Redshift target is configured
    if os.getenv("USE_REDSHIFT_COPY", "").lower() in ("1", "true", "yes"):
        try:
            row_count = redshift_copy_from_s3(s3_key, table)
            mark_loaded(s3_key, row_count)
        except Exception as e:
            log.error("Redshift COPY failed for %s: %s", s3_key, e)
            mark_failed(s3_key, str(e))
            raise
        return

    loader = LOADERS.get(ext)
    if loader is None:
        log.warning("No loader for file type '%s' — skipping %s", ext, s3_key)
        return

    try:
        row_count = loader(s3_key, table)
        mark_loaded(s3_key, row_count)
    except Exception as e:
        log.error("Failed to load %s: %s", s3_key, e)
        mark_failed(s3_key, str(e))
        raise


def run(prefix: str = "", table: str = "raw_data"):
    keys = list_s3_files(prefix=prefix)
    log.info("Found %d files under prefix '%s'", len(keys), prefix)
    for key in keys:
        ingest_file(key, table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest files from S3 into raw warehouse")
    parser.add_argument("--prefix", default="", help="S3 key prefix to filter files")
    parser.add_argument("--table", default="raw_data", help="Target raw table name")
    args = parser.parse_args()
    run(prefix=args.prefix, table=args.table)
