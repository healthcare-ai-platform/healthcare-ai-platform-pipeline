import io

import pandas as pd
from psycopg2.extras import execute_values

from common.db import get_connection
from common.logger import get_logger
from common.s3 import download_file

log = get_logger(__name__)


def load_parquet(s3_key: str, table: str, schema: str = "raw") -> int:
    raw = download_file(s3_key)
    df = pd.read_parquet(io.BytesIO(raw))
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    cols = list(df.columns)
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO {schema}.{table} ({','.join(cols)}) VALUES %s",
                rows,
            )

    log.info("Loaded %d rows from %s into %s.%s", len(df), s3_key, schema, table)
    return len(df)
