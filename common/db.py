"""Shared Postgres connection helper. Not a locked contract file — plain
utility so feedback/ and monitoring/ don't duplicate connection boilerplate.
Uses the same psycopg2 style as training/train.py from Phase 3.
"""
from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://frauduser:fraudpass@localhost:5432/frauddb"
)


@contextmanager
def get_conn(retries: int = 5, backoff_seconds: float = 2.0) -> Iterator["psycopg2.extensions.connection"]:
    """Yields a psycopg2 connection, retrying on startup races (Postgres
    container not ready yet) — the same pattern training/train.py needs
    when run under `docker compose up` where startup order isn't guaranteed
    by `depends_on` alone."""
    last_exc: Exception | None = None
    conn = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            break
        except psycopg2.OperationalError as exc:
            last_exc = exc
            logger.warning(
                "Postgres connection attempt %d/%d failed: %s", attempt, retries, exc
            )
            time.sleep(backoff_seconds * attempt)
    if conn is None:
        raise ConnectionError(
            f"Could not connect to Postgres after {retries} attempts"
        ) from last_exc
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
