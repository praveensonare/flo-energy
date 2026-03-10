"""
PostgreSQL handler with connection pooling and thread-safe batch inserts.

Key design choices:
  - psycopg2.pool.ThreadedConnectionPool: safe for multi-threaded writes
  - Batched execute_values(): significantly faster than single-row INSERTs
  - ON CONFLICT DO NOTHING: idempotent; safe to re-run on the same file
  - ThreadPoolExecutor: parallel batch submissions for large datasets
  - Tenacity retry: transient connection failures are retried automatically
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras
import psycopg2.pool
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.models.meter_reading import MeterReading

logger = structlog.get_logger(__name__)

_DEFAULT_BATCH_SIZE = 1_000
_MAX_WORKERS = 4  # parallel batch writers

_INSERT_SQL = """
INSERT INTO meter_readings (id, nmi, "timestamp", consumption)
VALUES %s
ON CONFLICT ON CONSTRAINT meter_readings_unique_consumption DO NOTHING;
"""


class PostgresHandler:
    """
    Thread-safe PostgreSQL client for meter_readings table.

    Lifecycle::

        handler = PostgresHandler.from_env()
        handler.ensure_schema()          # optional: create table if absent
        stats = handler.bulk_insert(readings, batch_size=1000)
        handler.close()
    """

    def __init__(
        self,
        dsn: str,
        min_connections: int = 2,
        max_connections: int = 10,
    ) -> None:
        self._dsn = dsn
        self._pool: psycopg2.pool.ThreadedConnectionPool | None = None
        self._min_conn = min_connections
        self._max_conn = max_connections

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "PostgresHandler":
        """
        Construct from DATABASE_URL or individual PG* env vars.

        Priority: DATABASE_URL > individual vars > defaults.
        """
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            host = os.getenv("PGHOST", "localhost")
            port = os.getenv("PGPORT", "5432")
            dbname = os.getenv("PGDATABASE", "meter_db")
            user = os.getenv("PGUSER", "postgres")
            password = os.getenv("PGPASSWORD", "postgres")
            dsn = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        return cls(dsn=dsn)

    # ------------------------------------------------------------------
    # Connection pool
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Initialise the connection pool. Idempotent."""
        if self._pool is not None:
            return
        logger.info("postgres.pool_init", dsn=self._redact_dsn())
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=self._min_conn,
            maxconn=self._max_conn,
            dsn=self._dsn,
            connect_timeout=10,
        )

    def close(self) -> None:
        """Close all pool connections."""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("postgres.pool_closed")

    @contextmanager
    def _get_conn(self) -> Generator[psycopg2.extensions.connection, None, None]:
        if self._pool is None:
            self.connect()
        conn = self._pool.getconn()  # type: ignore[union-attr]
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def ensure_schema(self) -> None:
        """Create the meter_readings table if it does not already exist."""
        ddl = """
        CREATE TABLE IF NOT EXISTS meter_readings (
            id          UUID DEFAULT gen_random_uuid() NOT NULL,
            nmi         VARCHAR(10) NOT NULL,
            "timestamp" TIMESTAMP  NOT NULL,
            consumption NUMERIC    NOT NULL,
            CONSTRAINT meter_readings_pk PRIMARY KEY (id),
            CONSTRAINT meter_readings_unique_consumption
                UNIQUE (nmi, "timestamp")
        );
        """
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
        logger.info("postgres.schema_ensured")

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type(psycopg2.OperationalError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    def _insert_batch(self, batch: list[MeterReading]) -> tuple[int, int]:
        """
        Insert one batch and return (attempted, skipped_due_to_conflict).

        Retried up to 3× on transient OperationalError.
        """
        tuples = [
            (r.id, r.nmi, r.timestamp, r.consumption)
            for r in batch
        ]
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                before = cur.rowcount if cur.rowcount >= 0 else 0
                psycopg2.extras.execute_values(
                    cur,
                    _INSERT_SQL,
                    tuples,
                    template=None,
                    page_size=len(tuples),
                )
                inserted = cur.rowcount if cur.rowcount >= 0 else len(tuples)

        skipped = len(tuples) - inserted
        return inserted, skipped

    def bulk_insert(
        self,
        readings: list[MeterReading],
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> dict[str, int]:
        """
        Insert all readings in parallel batches.

        Returns::

            {"inserted": int, "skipped": int, "failed": int, "total": int}
        """
        if not readings:
            return {"inserted": 0, "skipped": 0, "failed": 0, "total": 0}

        self.connect()

        batches = [
            readings[i : i + batch_size]
            for i in range(0, len(readings), batch_size)
        ]

        total_inserted = 0
        total_skipped = 0
        total_failed = 0

        logger.info(
            "postgres.bulk_insert_start",
            total=len(readings),
            batches=len(batches),
            batch_size=batch_size,
        )

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._insert_batch, batch): idx
                for idx, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                batch_idx = futures[future]
                try:
                    inserted, skipped = future.result()
                    total_inserted += inserted
                    total_skipped += skipped
                    logger.debug(
                        "postgres.batch_done",
                        batch=batch_idx,
                        inserted=inserted,
                        skipped=skipped,
                    )
                except Exception as exc:
                    batch = batches[batch_idx]
                    total_failed += len(batch)
                    logger.error(
                        "postgres.batch_failed",
                        batch=batch_idx,
                        size=len(batch),
                        error=str(exc),
                    )

        result = {
            "inserted": total_inserted,
            "skipped": total_skipped,
            "failed": total_failed,
            "total": len(readings),
        }
        logger.info("postgres.bulk_insert_complete", **result)
        return result

    def generate_insert_sql(self, readings: list[MeterReading]) -> str:
        """
        Return a string of SQL INSERT statements (no DB connection needed).

        Useful for dry-run / audit output.
        """
        lines: list[str] = []
        for r in readings:
            ts = r.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            lines.append(
                f"INSERT INTO meter_readings (id, nmi, \"timestamp\", consumption) "
                f"VALUES ('{r.id}', '{r.nmi}', '{ts}', {r.consumption}) "
                f"ON CONFLICT ON CONSTRAINT meter_readings_unique_consumption "
                f"DO NOTHING;"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Healthcheck
    # ------------------------------------------------------------------

    def ping(self) -> bool:
        """Return True if the database is reachable."""
        try:
            self.connect()
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception as exc:
            logger.warning("postgres.ping_failed", error=str(exc))
            return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _redact_dsn(self) -> str:
        """Return a DSN with the password replaced by ***."""
        import re
        return re.sub(r":[^@/]+@", ":***@", self._dsn)
