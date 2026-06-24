import pandas as pd

from common.logger import get_logger

log = get_logger(__name__)


def validate(df: pd.DataFrame, s3_key: str, required_columns: list[str] | None = None) -> bool:
    if df.empty:
        log.warning("%s — empty file, skipping", s3_key)
        return False

    if required_columns:
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            log.error("%s — missing required columns: %s", s3_key, missing)
            return False

    null_pct = df.isnull().mean()
    high_null = null_pct[null_pct > 0.9]
    if not high_null.empty:
        log.warning("%s — columns with >90%% nulls: %s", s3_key, high_null.index.tolist())

    log.info("%s — validated OK (%d rows, %d cols)", s3_key, len(df), len(df.columns))
    return True
