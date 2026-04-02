import queue
import urllib3
import clickhouse_connect
from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy.orm import registry, sessionmaker


# ── Configuration ──────────────────────────────────────────────────────────────

REMOTE_CLICKHOUSE_PORT = 80
REMOTE_CLICKHOUSE_PASSWORD = ""
REMOTE_CLICKHOUSE_DB = "nc_spark"
REMOTE_CLICKHOUSE_USER = "default"
REMOTE_CLICKHOUSE_HOST = "172.15.1.61"
REMOTE_CLICKHOUSE_PROXY = "clickhouse/"

LOCAL_CLICKHOUSE_PROXY = ""
LOCAL_CLICKHOUSE_PORT = 8123
LOCAL_CLICKHOUSE_PASSWORD = ""
LOCAL_CLICKHOUSE_DB = "nc_spark"
LOCAL_CLICKHOUSE_USER = "default"
LOCAL_CLICKHOUSE_HOST = "0.0.0.0"


# ── SQLAlchemy (SQLite) ────────────────────────────────────────────────────────

SQLALCHEMY_DATABASE_URL = "sqlite:///database/database.sqlite3"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
    if "sqlite" in SQLALCHEMY_DATABASE_URL
    else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
mapper_registry = registry()
Base = mapper_registry.generate_base()


# ── Shared urllib3 Pool Managers ───────────────────────────────────────────────
# FastAPI — local ClickHouse (status checks, upload acks, etc.)
local_http_pool = urllib3.PoolManager(
    num_pools=1,
    maxsize=50,
    block=True,
)

# Celery background workers — local ClickHouse (annotation writes, job updates)
local_bg_http_pool = urllib3.PoolManager(
    num_pools=1,
    maxsize=50,
    block=True,
)

# FastAPI — remote ClickHouse
remote_http_pool = urllib3.PoolManager(
    num_pools=1,
    maxsize=50,
    block=True,
)

# Celery background workers — remote ClickHouse
remote_bg_http_pool = urllib3.PoolManager(
    num_pools=1,
    maxsize=50,
    block=True,
)


# ── Connection Pool ────────────────────────────────────────────────────────────


class ClickHousePool:
    """
    Thread-safe pool of clickhouse-connect clients backed by a shared
    urllib3.PoolManager.

    * Clients are pre-created at startup and reused across requests.
    * A broken client is replaced with a fresh one rather than being
      returned to the pool in an unknown state.
    * pool_size should satisfy:
          pool_size_per_process × num_uvicorn_workers  <=  CH max_connections
    """

    def __init__(
        self, host, port, user, password, db, proxy, pool_mgr, pool_size: int = 10
    ):
        self._config = dict(
            host=host,
            port=port,
            user=user,
            database=db,
            proxy_path=proxy,
            password=password,
        )
        self._pool_mgr = pool_mgr
        self._pool_size = pool_size
        self._pool = queue.Queue(maxsize=pool_size)

        for _ in range(pool_size):
            self._pool.put(self._make_client())

        print(f"✓ ClickHouse pool ready: {host}:{port} ({pool_size} clients)")

    def _make_client(self):
        return clickhouse_connect.get_client(
            interface="http",
            pool_mgr=self._pool_mgr,
            autogenerate_session_id=True,
            **self._config,
        )

    @contextmanager
    def borrow(self, timeout: float = 10.0):
        """
        Borrow a client for the duration of a with-block.
        Automatically returns (or replaces) it on exit — even on exception.

        Usage:
            with pool.borrow() as client:
                client.query(...)
        """
        try:
            client = self._pool.get(timeout=timeout)
        except queue.Empty:
            raise RuntimeError(
                f"ClickHouse pool exhausted after {timeout}s — "
                f"all {self._pool_size} clients are in use. "
                f"Consider increasing pool_size or reducing concurrency."
            )

        try:
            yield client
        except Exception:
            # Replace a potentially broken client with a fresh one
            try:
                client.close()
            except Exception:
                pass
            client = self._make_client()
            raise
        finally:
            self._pool.put(client)


# ── Pool Instances (created once at process startup) ──────────────────────────
#
# pool_size sizing guide (per Uvicorn worker process):
#   local_pool  : handles GET /status + POST /upload — short, fast requests
#   remote_pool : handles annotation reads from remote CH — medium duration
#
#   Total CH connections = pool_size × num_uvicorn_workers
#   Keep this well below ClickHouse's max_connections (default 1024).

local_pool = ClickHousePool(
    pool_size=50,
    db=LOCAL_CLICKHOUSE_DB,
    pool_mgr=local_http_pool,
    host=LOCAL_CLICKHOUSE_HOST,
    port=LOCAL_CLICKHOUSE_PORT,
    user=LOCAL_CLICKHOUSE_USER,
    proxy=LOCAL_CLICKHOUSE_PROXY,
    password=LOCAL_CLICKHOUSE_PASSWORD,
)

remote_pool = ClickHousePool(
    pool_size=30,
    db=REMOTE_CLICKHOUSE_DB,
    pool_mgr=remote_http_pool,
    host=REMOTE_CLICKHOUSE_HOST,
    port=REMOTE_CLICKHOUSE_PORT,
    user=REMOTE_CLICKHOUSE_USER,
    proxy=REMOTE_CLICKHOUSE_PROXY,
    password=REMOTE_CLICKHOUSE_PASSWORD,
)


# ── FastAPI Dependencies ───────────────────────────────────────────────────────


def get_local_db():
    """
    FastAPI Depends() generator.
    Borrows a pooled client for exactly one request lifetime,
    then returns it to the pool automatically.
    """
    with local_pool.borrow() as client:
        yield client


def get_remote_db():
    """
    FastAPI Depends() generator for the remote ClickHouse cluster.
    """
    with remote_pool.borrow() as client:
        yield client


# ── Background / Celery Client Factory ────────────────────────────────────────
#
# Background clients are NOT pooled — Celery workers are long-lived processes
# that each hold one client for the duration of a task.  They use a SEPARATE
# urllib3 pool from the FastAPI clients so heavy annotation tasks cannot
# starve lightweight API requests.
#
# Key settings restored / fixed vs the "new" version:
#   async_insert=1          : batch-buffer inserts server-side (throughput)
#   wait_for_async_insert=1 : WAIT for disk flush before returning —
#                             prevents silent data loss on server restart
#   max_memory_usage        : explicit 2 TB cap — never let a runaway query
#                             OOM the server (max_memory_usage=0 is unsafe)
#   autogenerate_session_id=False : single reused session per worker (correct
#                             for long-lived Celery tasks)


def _make_background_client(host, port, user, password, db, proxy, pool_mgr):
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        user=user,
        database=db,
        pool_mgr=pool_mgr,
        interface="http",
        proxy_path=proxy,
        password=password,
        send_receive_timeout=1860,
        autogenerate_session_id=False,
        settings={
            # ── Insert behaviour ──────────────────────────────────────────
            "async_insert": 1,  # server buffers batches → throughput
            "wait_for_async_insert": 1,  # MUST be 1 — waits for disk flush;
            # 0 risks silent data loss on restart
            # ── Query execution ───────────────────────────────────────────
            "max_threads": 4,
            "max_insert_threads": 0,
            "join_algorithm": "hash",
            "max_block_size": 131072,
            "max_execution_time": 1800,
            # ── Memory limits ─────────────────────────────────────────────
            "max_memory_usage": 2199023255552,  # 2 TB hard cap (not 0)
            "max_bytes_in_join": 214748364800,  # 200 GB
            "max_bytes_before_external_sort": 214748364800,
            # ── Caching & optimisations ───────────────────────────────────
            "use_uncompressed_cache": 1,
            "optimize_read_in_order": 1,
            "optimize_move_to_prewhere": 1,
            "optimize_aggregation_in_order": 1,
            "min_insert_block_size_rows": 1048576,
            "min_insert_block_size_bytes": 268435456,
        },
    )


def get_local_background_client():
    """
    Returns a new ClickHouse client configured for Celery/background tasks
    against the LOCAL cluster.  Uses a pool manager that is completely
    separate from the FastAPI request pool so heavy tasks cannot starve
    status-check endpoints.
    """
    return _make_background_client(
        db=LOCAL_CLICKHOUSE_DB,
        host=LOCAL_CLICKHOUSE_HOST,
        port=LOCAL_CLICKHOUSE_PORT,
        user=LOCAL_CLICKHOUSE_USER,
        pool_mgr=local_bg_http_pool,
        proxy=LOCAL_CLICKHOUSE_PROXY,
        password=LOCAL_CLICKHOUSE_PASSWORD,
    )


def get_remote_background_client():
    """
    Returns a new ClickHouse client configured for Celery/background tasks
    against the REMOTE cluster.
    """
    return _make_background_client(
        db=REMOTE_CLICKHOUSE_DB,
        host=REMOTE_CLICKHOUSE_HOST,
        port=REMOTE_CLICKHOUSE_PORT,
        user=REMOTE_CLICKHOUSE_USER,
        pool_mgr=remote_bg_http_pool,
        proxy=REMOTE_CLICKHOUSE_PROXY,
        password=REMOTE_CLICKHOUSE_PASSWORD,
    )
