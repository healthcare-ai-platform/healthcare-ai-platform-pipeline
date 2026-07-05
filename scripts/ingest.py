import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # must run before local imports so env vars are set when modules load

from common.logger import get_logger
from common.s3 import S3_BUCKET, list_s3_files
from common.warehouse import get_warehouse_session
from scripts.tracker import is_loaded, mark_failed, mark_loaded

log = get_logger(__name__)

# Snowpark DataFrameReader format options per extension (storage-integration path)
_READER_OPTIONS = {
    ".csv":    {"format": "csv",     "options": {"SKIP_HEADER": "1", "FIELD_OPTIONALLY_ENCLOSED_BY": '"'}},
    ".parquet":{"format": "parquet", "options": {}},
    ".json":   {"format": "json",    "options": {}},
    ".jsonl":  {"format": "json",    "options": {}},
}

# Fallback SQL FILE_FORMAT clause used when no storage integration is configured
_SQL_FORMAT = {
    ".csv":    "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
    ".parquet":"FILE_FORMAT = (TYPE = PARQUET)  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
    ".json":   "FILE_FORMAT = (TYPE = JSON)      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
    ".jsonl":  "FILE_FORMAT = (TYPE = JSON)      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
}


def _rows_loaded(copy_result) -> int:
    """Sum rows_loaded across all files in a COPY INTO result."""
    total = 0
    for row in copy_result:
        status = row["status"] if "status" in row.asDict() else row[1]
        loaded = row["rows_loaded"] if "rows_loaded" in row.asDict() else row[3]
        if status == "LOADED":
            total += loaded
    return total


def snowflake_copy_from_s3(s3_key: str, table: str, schema: str = "RAW") -> int:
    """
    Load an S3 file into a Snowflake table using Snowpark.

    When SNOWFLAKE_STORAGE_INTEGRATION is set: uses Snowpark's DataFrameReader
    (session.read.<format>) + copy_into_table() — fully Snowpark-native.

    Without it: falls back to session.sql(COPY INTO ...) with inline AWS credentials,
    still through the Snowpark session.
    """
    ext = Path(s3_key).suffix.lower()
    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

    if ext not in _READER_OPTIONS:
        raise ValueError(f"Unsupported file type for Snowpark COPY: {ext}")

    with get_warehouse_session() as session:
        storage_integration = os.getenv("SNOWFLAKE_STORAGE_INTEGRATION", "")

        if storage_integration:
            # Snowpark-native path — DataFrameReader → copy_into_table
            cfg   = _READER_OPTIONS[ext]
            reader = session.read
            for k, v in cfg["options"].items():
                reader = reader.option(k, v)

            df = getattr(reader, cfg["format"])(s3_uri)
            result = df.copy_into_table(
                f"{schema}.{table}",
                force=False,
                match_by_column_name="case_insensitive",
            )
        else:
            # SQL fallback — inline AWS credentials, still through Snowpark session
            key_id = os.environ["AWS_ACCESS_KEY_ID"]
            secret = os.environ["AWS_SECRET_ACCESS_KEY"]
            fmt    = _SQL_FORMAT[ext]
            sql = (
                f"COPY INTO {schema}.{table} "
                f"FROM '{s3_uri}' "
                f"CREDENTIALS = (AWS_KEY_ID='{key_id}' AWS_SECRET_KEY='{secret}') "
                f"{fmt} "
                f"PURGE = FALSE ON_ERROR = ABORT_STATEMENT"
            )
            result = session.sql(sql).collect()

        rows = _rows_loaded(result)

    log.info("Snowpark COPY loaded %d rows from %s into %s.%s", rows, s3_key, schema, table)
    return rows


def ingest_file(s3_key: str, table: str):
    if is_loaded(s3_key):
        log.info("Skipping already-loaded file: %s", s3_key)
        return

    try:
        row_count = snowflake_copy_from_s3(s3_key, table)
        mark_loaded(s3_key, row_count)
    except Exception as e:
        log.error("Snowpark COPY failed for %s: %s", s3_key, e)
        mark_failed(s3_key, str(e))
        raise


def run(prefix: str = "", table: str = "raw_data"):
    keys = list_s3_files(prefix=prefix)
    log.info("Found %d files under prefix '%s'", len(keys), prefix)
    for key in keys:
        ingest_file(key, table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest files from S3 into Snowflake via Snowpark")
    parser.add_argument("--prefix", default="", help="S3 key prefix to filter files")
    parser.add_argument("--table",  default="raw_data", help="Target Snowflake table name")
    args = parser.parse_args()
    run(prefix=args.prefix, table=args.table)
