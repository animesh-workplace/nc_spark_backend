import urllib3
import clickhouse_connect
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.orm import registry, sessionmaker

# --- Configuration ---
CLICKHOUSE_PORT = 80
CLICKHOUSE_PASSWORD = ""
CLICKHOUSE_DB = "nc_spark"
CLICKHOUSE_USER = "default"
CLICKHOUSE_HOST = "10.10.9.24"
SQLALCHEMY_DATABASE_URL = "sqlite:///database/database.sqlite3"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
    if "sqlite" in SQLALCHEMY_DATABASE_URL
    else {},
)

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
    """
    client = get_clickhouse_client_cached()
    try:
        yield client
    finally:
        # We do NOT client.close() here.
        # The client lives for the life of the app.
        pass


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
mapper_registry = registry()
Base = mapper_registry.generate_base()


# ── Fresh client for background tasks ─────────
def get_background_client():
    """
    Always creates a new client instance.
    Background tasks must NEVER share a client with request-scoped queries.
    """
    background_pool = urllib3.PoolManager(
        num_pools=1,
        maxsize=2,
        headers={"Connection": "close"},  # disable keep-alive for background client
    )

    return clickhouse_connect.get_client(
        interface="http",
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        database=CLICKHOUSE_DB,
        proxy_path="clickhouse/",
        pool_mgr=background_pool,
        send_receive_timeout=1860,
        password=CLICKHOUSE_PASSWORD,
        autogenerate_session_id=False,
        settings={
            "priority": 10,
            "max_threads": 0,
            "max_insert_threads": 0,
            "join_algorithm": "hash",
            "max_block_size": 131072,
            "max_execution_time": 1800,
            "use_uncompressed_cache": 1,
            "optimize_read_in_order": 1,
            "optimize_move_to_prewhere": 1,
            "max_memory_usage": 2199023255552,
            "max_bytes_in_join": 214748364800,
            "optimize_aggregation_in_order": 1,
            "min_insert_block_size_rows": 1048576,
            "min_insert_block_size_bytes": 268435456,
            "max_bytes_before_external_sort": 214748364800,
        },
    )
