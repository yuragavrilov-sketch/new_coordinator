"""PostgreSQL connection pool."""

import os
import re
import threading

import psycopg2
import psycopg2.pool

PG_DSN = os.environ.get(
    "STATE_DB_DSN",
    "postgresql://postgres:postgres@localhost:5432/migration_state",
)
_PG_POOL_MIN = int(os.environ.get("PG_POOL_MIN", "2"))
_PG_POOL_MAX = int(os.environ.get("PG_POOL_MAX", "10"))


def _masked_dsn(dsn: str) -> str:
    """Replace password in DSN URL with ***."""
    return re.sub(r"(://[^:]+:)[^@]+(@)", r"\1***\2", dsn)


print(f"[state_db] DSN = {_masked_dsn(PG_DSN)}")

_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is None:
            _pool = psycopg2.pool.ThreadedConnectionPool(
                _PG_POOL_MIN, _PG_POOL_MAX, PG_DSN
            )
            print(
                f"[state_db] connection pool ready "
                f"(min={_PG_POOL_MIN} max={_PG_POOL_MAX})"
            )
    return _pool


class _PooledConn:
    """Wraps a psycopg2 connection so that close() returns it to the pool."""
    __slots__ = ("_conn", "_pool")

    def __init__(self, conn, pool: psycopg2.pool.ThreadedConnectionPool):
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_pool", pool)

    def __getattr__(self, name: str):
        return getattr(object.__getattribute__(self, "_conn"), name)

    def __setattr__(self, name: str, value):
        setattr(object.__getattribute__(self, "_conn"), name, value)

    def close(self):
        pool = object.__getattribute__(self, "_pool")
        conn = object.__getattribute__(self, "_conn")
        try:
            if not conn.closed:
                conn.rollback()
        except Exception:
            pass
        pool.putconn(conn)


def get_conn() -> _PooledConn:
    try:
        pool = _get_pool()
        conn = pool.getconn()
        return _PooledConn(conn, pool)
    except psycopg2.pool.PoolError as exc:
        print(f"[state_db] pool exhausted ({_PG_POOL_MAX} max): {exc}")
        raise
    except Exception as exc:
        print(f"[state_db] connection FAILED ({_masked_dsn(PG_DSN)}): {exc}")
        raise
