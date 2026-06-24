from common.db import get_connection


def is_loaded(s3_key: str) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM raw.ingestion_tracker WHERE s3_key = %s AND status = 'loaded'",
                (s3_key,),
            )
            return cur.fetchone() is not None


def mark_loaded(s3_key: str, row_count: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.ingestion_tracker (s3_key, status, row_count)
                VALUES (%s, 'loaded', %s)
                ON CONFLICT (s3_key) DO UPDATE
                    SET status = 'loaded',
                        row_count = EXCLUDED.row_count,
                        loaded_at = NOW(),
                        error_msg = NULL
                """,
                (s3_key, row_count),
            )


def mark_failed(s3_key: str, error: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.ingestion_tracker (s3_key, status, error_msg)
                VALUES (%s, 'failed', %s)
                ON CONFLICT (s3_key) DO UPDATE
                    SET status = 'failed',
                        error_msg = EXCLUDED.error_msg,
                        loaded_at = NOW()
                """,
                (s3_key, error),
            )


def clear(s3_key: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM raw.ingestion_tracker WHERE s3_key = %s",
                (s3_key,),
            )
