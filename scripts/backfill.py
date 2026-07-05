import argparse

from dotenv import load_dotenv

load_dotenv()  # must run before local imports so env vars are set when modules load

from common.logger import get_logger
from common.s3 import list_s3_files
from scripts.tracker import clear
from scripts.ingest import ingest_file

log = get_logger(__name__)


def backfill(prefix: str, table: str):
    keys = list_s3_files(prefix=prefix)
    log.info("Backfilling %d files under prefix '%s'", len(keys), prefix)
    for key in keys:
        log.info("Clearing tracker for %s", key)
        clear(key)
        ingest_file(key, table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Re-load S3 files into raw warehouse")
    parser.add_argument("--prefix", required=True, help="S3 key prefix to backfill")
    parser.add_argument("--table", default="raw_data", help="Target raw table name")
    args = parser.parse_args()
    backfill(prefix=args.prefix, table=args.table)
