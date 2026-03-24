import queue
import urllib3
import threading
import clickhouse_connect
from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy.orm import registry, sessionmaker

# Configuration
REMOTE_CLICKHOUSE_PORT = 80
REMOTE_CLICKHOUSE_PASSWORD = ""
REMOTE_CLICKHOUSE_DB = "nc_spark"
REMOTE_CLICKHOUSE_USER = "default"
REMOTE_CLICKHOUSE_HOST = "10.10.9.24"
REMOTE_CLICKHOUSE_PROXY = "clickhouse/"

LOCAL_CLICKHOUSE_PROXY = ""
LOCAL_CLICKHOUSE_PORT = 8123
LOCAL_CLICKHOUSE_PASSWORD = ""
LOCAL_CLICKHOUSE_DB = "nc_spark"
LOCAL_CLICKHOUSE_USER = "default"
LOCAL_CLICKHOUSE_HOST = "0.0.0.0"

SQLALCHEMY_DATABASE_URL = "sqlite:///database/database.sqlite3"

# SQLAlchemy
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
    if "sqlite" in SQLALCHEMY_DATABASE_URL
    else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
mapper_registry = registry()
Base = mapper_registry.generate_base()


# Connection Pool
class ClickHousePool:
    """
    Thread-safe pool of clickhouse-connect clients.
    Each borrowed client has its own session_id — no concurrent session errors.
    """

    def __init__(self, host, port, user, password, db, proxy, pool_size=10):
        self._config = dict(
            host=host,
            port=port,
            user=user,
            database=db,
            proxy_path=proxy,
            password=password,
        )
        self._pool_size = pool_size
        self._pool = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()

        # Pre-fill the pool
        for _ in range(pool_size):
            self._pool.put(self._make_client())

        print(f"✓ ClickHouse pool ready: {host}:{port} ({pool_size} clients)")

    def _make_client(self):
        return clickhouse_connect.get_client(
            interface="http",
            autogenerate_session_id=True,  # each client gets its own unique session_id
            **self._config,
        )

    @contextmanager
    def borrow(self, timeout: float = 10.0):
        """
        Borrow a client from the pool.
        Returns it automatically when the with-block exits — even on exception.

        Usage:
            with pool.borrow() as client:
                client.query(...)
        """
        try:
            client = self._pool.get(timeout=timeout)
        except queue.Empty:
            raise Exception(
                f"ClickHouse pool exhausted after {timeout}s. "
                f"All {self._pool_size} clients are in use. Consider increasing pool_size."
            )
        try:
            yield client
        except Exception:
            # If the client errored, replace it with a fresh one instead of returning the broken one
            try:
                client.close()
            except Exception:
                pass
            client = self._make_client()
            raise
        finally:
            self._pool.put(client)


# Pool instances (created once at startup)
local_pool = ClickHousePool(
    pool_size=10,
    db=LOCAL_CLICKHOUSE_DB,
    host=LOCAL_CLICKHOUSE_HOST,
    port=LOCAL_CLICKHOUSE_PORT,
    user=LOCAL_CLICKHOUSE_USER,
    proxy=LOCAL_CLICKHOUSE_PROXY,
    password=LOCAL_CLICKHOUSE_PASSWORD,
)

remote_pool = ClickHousePool(
    pool_size=10,
    db=REMOTE_CLICKHOUSE_DB,
    host=REMOTE_CLICKHOUSE_HOST,
    port=REMOTE_CLICKHOUSE_PORT,
    user=REMOTE_CLICKHOUSE_USER,
    proxy=REMOTE_CLICKHOUSE_PROXY,
    password=REMOTE_CLICKHOUSE_PASSWORD,
)


# FastAPI Dependencies
def get_local_db():
    """
    FastAPI dependency — borrows a client from the local pool for the
    duration of the request, then returns it automatically.
    """
    with local_pool.borrow() as client:
        yield client


def get_remote_db():
    """
    FastAPI dependency — borrows a client from the remote pool for the
    duration of the request, then returns it automatically.
    """
    with remote_pool.borrow() as client:
        yield client


# Background task clients
def _make_background_client(host, port, user, password, db, proxy):
    pool = urllib3.PoolManager(
        num_pools=1,
        maxsize=2,
        headers={"Connection": "close"},
    )
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        user=user,
        database=db,
        pool_mgr=pool,
        interface="http",
        proxy_path=proxy,
        password=password,
        send_receive_timeout=1860,
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


def get_local_background_client():
    return _make_background_client(
        LOCAL_CLICKHOUSE_HOST,
        LOCAL_CLICKHOUSE_PORT,
        LOCAL_CLICKHOUSE_USER,
        LOCAL_CLICKHOUSE_PASSWORD,
        LOCAL_CLICKHOUSE_DB,
        LOCAL_CLICKHOUSE_PROXY,
    )


def get_remote_background_client():
    return _make_background_client(
        REMOTE_CLICKHOUSE_HOST,
        REMOTE_CLICKHOUSE_PORT,
        REMOTE_CLICKHOUSE_USER,
        REMOTE_CLICKHOUSE_PASSWORD,
        REMOTE_CLICKHOUSE_DB,
        REMOTE_CLICKHOUSE_PROXY,
    )
