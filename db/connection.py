"""Centralized PostgreSQL connection helpers.

Defaults use environment variables with sensible fallbacks so other scripts can
import and reuse a single connection pool.

Defaults used (can be overridden with env vars):
- host: localhost
- port: 5432
- user: postgres
- password: admin
- dbname: NSE

Usage:
    from db.connection import get_cursor
    with get_cursor(commit=True) as cur:
        cur.execute("SELECT 1")
        print(cur.fetchone())
"""
from __future__ import annotations

import contextlib
import logging
import os
from typing import Iterator, Optional

try:
    import psycopg2
    from psycopg2 import pool
    _HAS_PSYCOPG2 = True
except Exception:
    # Do not raise here — allow module import even when psycopg2 isn't installed.
    # The actual functions that need DB access will raise a clear ImportError
    # at runtime. This makes the package importable in test environments that
    # mock DB calls.
    psycopg2 = None
    pool = None
    _HAS_PSYCOPG2 = False

logger = logging.getLogger(__name__)

# Module-level pool instance
_POOL: Optional[pool.ThreadedConnectionPool] = None

# Default DB configuration (can be overridden by environment variables)
_DEFAULT_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin"),
    "dbname": os.getenv("DB_NAME", "NSE"),
}


def init_pool(minconn: int = 1, maxconn: int = 5, **overrides) -> pool.ThreadedConnectionPool:
    """Initialize and return a module-level threaded connection pool.

    Subsequent calls will return the already-initialized pool.
    Pass overrides to change host/user/password/dbname at runtime.
    """
    global _POOL

    if not _HAS_PSYCOPG2:
        raise ImportError("psycopg2 is required. Install with `pip install psycopg2-binary`")

    if _POOL is not None:
        return _POOL

    cfg = _DEFAULT_CONFIG.copy()
    cfg.update({k: v for k, v in overrides.items() if v is not None})

    logger.debug("Initializing PostgreSQL pool: host=%s port=%s db=%s user=%s",
                 cfg.get("host"), cfg.get("port"), cfg.get("dbname"), cfg.get("user"))

    _POOL = pool.ThreadedConnectionPool(minconn, maxconn, **cfg)
    return _POOL


@contextlib.contextmanager
def get_connection() -> Iterator[psycopg2.extensions.connection]:
    """Context manager yielding a DB connection from the pool.

    Automatically initializes the pool on first use with default settings.
    Use in a `with` block and the connection will be returned to the pool.
    """
    global _POOL
    if _POOL is None:
        init_pool()

    conn = _POOL.getconn()
    try:
        yield conn
    finally:
        _POOL.putconn(conn)


@contextlib.contextmanager
def get_cursor(commit: bool = False) -> Iterator[psycopg2.extensions.cursor]:
    """Context manager yielding a cursor from a pooled connection.

    If `commit=True` the connection will be committed on successful exit,
    and rolled back on exception.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            yield cur
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def close_pool() -> None:
    """Close all pooled connections and clear the pool."""
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None


__all__ = ["init_pool", "get_connection", "get_cursor", "close_pool"]
