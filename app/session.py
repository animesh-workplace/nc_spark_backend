import clickhouse_connect
from functools import lru_cache

# --- Configuration ---
# You can load these from .env or os.getenv() just like your example
CLICKHOUSE_HOST = "10.10.9.24"
CLICKHOUSE_PORT = 80
CLICKHOUSE_DB = "nc_spark"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""

# This is the equivalent of your 'engine = create_engine(...)'
# It's created ONCE when the app starts.
# It manages the connection pool.
try:
    _client = clickhouse_connect.get_client(
        interface="http",
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        database=CLICKHOUSE_DB,
        proxy_path="clickhouse/",
        password=CLICKHOUSE_PASSWORD,
    )
    _client.ping()
except Exception as e:
    print(f"FATAL: Could not connect to ClickHouse. {e}")
    _client = None


# This is the equivalent of your 'get_db()' dependency
# We use @lru_cache(maxsize=1) as a trick to ensure
# it just returns the same client object without re-evaluating.
@lru_cache(maxsize=1)
def get_clickhouse_client_cached():
    """
    Returns the globally shared ClickHouse client.
    Using @lru_cache ensures this function effectively
    returns the same _client instance every time.
    """
    if _client is None:
        raise Exception("Database client is not initialized.")
    return _client


def get_db():
    """
    FastAPI Dependency to get the ClickHouse client.

    This is different from SQLAlchemy:
    - We are NOT creating a new session.
    - We are NOT closing anything.

    The client object manages its own connection pool and is
    meant to be long-lived and shared.
    """
    client = get_clickhouse_client_cached()
    try:
        yield client
    finally:
        # We do NOT client.close() here.
        # The client lives for the life of the app.
        pass
