import io

import pandas as pd

from common.db import get_connection
from common.logger import get_logger
from common.s3 import download_file

log = get_logger(__name__)


def load_csv(s3_key: str, table: str, schema: str = "raw") -> int:
    raw = download_file(s3_key)
    df = pd.read_csv(io.BytesIO(raw))
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {schema}.{table} FROM STDIN WITH CSV", buf)

    log.info("Loaded %d rows from %s into %s.%s", len(df), s3_key, schema, table)
    return len(df)
