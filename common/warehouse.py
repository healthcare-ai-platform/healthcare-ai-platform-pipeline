import os
from contextlib import contextmanager

from snowflake.snowpark import Session


def _connection_params() -> dict:
    return {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "database":  os.getenv("SNOWFLAKE_DATABASE",  "HEALTHCARE"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA",    "RAW"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "role":      os.getenv("SNOWFLAKE_ROLE",      "SYSADMIN"),
    }


@contextmanager
def get_warehouse_session():
    session = Session.builder.configs(_connection_params()).create()
    try:
        yield session
    finally:
        session.close()
