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
from datetime import datetime
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

from src.config import settings
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

        # Connect to localhost:5432/meter_db (development default)
        handler = PostgresHandler.from_local()
        handler.ensure_schema()   # CREATE TABLE IF NOT EXISTS — safe to call on existing DB
        stats = handler.bulk_insert(readings, batch_size=1000)
        handler.close()

        # Or from environment variables / DATABASE_URL:
        handler = PostgresHandler.from_env()
    """

    # Default connection parameters come from settings (loaded from .env).
    # These class-level constants are kept for explicit override at call-sites.
    DEFAULT_HOST = settings.pg_host
    DEFAULT_PORT = str(settings.pg_port)
    DEFAULT_DBNAME = settings.pg_database
    DEFAULT_USER = settings.pg_user
    DEFAULT_PASSWORD = settings.pg_password

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
    def from_local(
        cls,
        *,
        host: str = DEFAULT_HOST,
        port: str | int = DEFAULT_PORT,
        dbname: str = DEFAULT_DBNAME,
        user: str = DEFAULT_USER,
        password: str = DEFAULT_PASSWORD,
    ) -> "PostgresHandler":
        """
        Construct a handler targeting **localhost:5432** with sensible defaults.

        All parameters can be overridden individually without needing to
        compose a full DSN string.  Suitable for local development and the
        tech-assessment runner.

        Example::

            handler = PostgresHandler.from_local()
            # → postgresql://postgres:postgres@localhost:5432/meter_db

            handler = PostgresHandler.from_local(dbname="flo_energy", password="secret")
        """
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        logger.debug("postgres.from_local", host=host, port=port, dbname=dbname)
        return cls(dsn=dsn)

    @classmethod
    def from_env(cls) -> "PostgresHandler":
        """
        Construct using the application :mod:`src.config` settings.

        Resolution order (handled by pydantic-settings in ``settings``):
          1. Actual environment variables
          2. ``.env`` file in the project root
          3. Built-in defaults  (localhost:5432/meter_db)

        The ``effective_database_url`` property on ``settings`` assembles the
        final DSN from either ``DATABASE_URL`` or individual ``PG*`` vars.
        """
        dsn = settings.effective_database_url
        return cls(
            dsn=dsn,
            min_connections=settings.pg_min_connections,
            max_connections=settings.pg_max_connections,
        )

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

    def ensure_schema(self) -> bool:
        """
        Create the ``meter_readings`` table **only if it does not already exist**.

        This method is **non-destructive**: it will never drop, truncate, or
        alter an existing table.  Safe to call on every application start.

        Returns:
            ``True``  – table was created in this call.
            ``False`` – table already existed; no DDL was executed.

        Schema (matches the tech-assessment specification)::

            CREATE TABLE meter_readings (
                id          UUID DEFAULT gen_random_uuid() NOT NULL,
                nmi         VARCHAR(10) NOT NULL,
                timestamp   TIMESTAMP  NOT NULL,
                consumption NUMERIC    NOT NULL,
                CONSTRAINT meter_readings_pk
                    PRIMARY KEY (id),
                CONSTRAINT meter_readings_unique_consumption
                    UNIQUE (nmi, timestamp)
            );
        """
        check_sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public'
            AND   table_name   = 'meter_readings'
        );
        """
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
                cur.execute(check_sql)
                already_exists: bool = cur.fetchone()[0]  # type: ignore[index]
                if not already_exists:
                    cur.execute(ddl)
                    logger.info("postgres.schema_created")
                else:
                    logger.info("postgres.schema_exists_skipped")

        return not already_exists

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

    def atomic_bulk_insert(
        self,
        readings: list[MeterReading],
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> dict[str, int]:
        """
        Insert all readings in a **single PostgreSQL transaction** (ACID-safe).

        Unlike ``bulk_insert`` (parallel batches, separate commits), this method
        wraps every batch in one connection+transaction:
          - **Atomicity**   – all batches commit together or all roll back.
          - **Consistency** – DB constraints enforce NMI/timestamp uniqueness.
          - **Isolation**   – other sessions see either all or none of the rows.
          - **Durability**  – PostgreSQL WAL guarantees persistence after commit.

        Use this when processing a single file end-to-end to ensure no partial
        writes survive a failure mid-file.
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

        logger.info(
            "postgres.atomic_insert_start",
            total=len(readings),
            batches=len(batches),
        )

        # Single connection = single transaction; commit once at the end.
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                for batch in batches:
                    tuples = [
                        (r.id, r.nmi, r.timestamp, r.consumption) for r in batch
                    ]
                    psycopg2.extras.execute_values(
                        cur,
                        _INSERT_SQL,
                        tuples,
                        template=None,
                        page_size=len(tuples),
                    )
                    inserted = cur.rowcount if cur.rowcount >= 0 else len(tuples)
                    total_inserted += inserted
                    total_skipped += len(tuples) - inserted

        result = {
            "inserted": total_inserted,
            "skipped": total_skipped,
            "failed": 0,
            "total": len(readings),
        }
        logger.info("postgres.atomic_insert_complete", **result)
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
    # Reads
    # ------------------------------------------------------------------

    def fetch_by_nmi(
        self,
        nmi: str,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        limit: int = 10_000,
    ) -> list[MeterReading]:
        """
        Fetch readings for *nmi* optionally filtered by a time range.

        Args:
            nmi:     National Metering Identifier (exact match, max 10 chars).
            from_dt: Inclusive lower bound on the interval timestamp.
            to_dt:   Inclusive upper bound on the interval timestamp.
            limit:   Maximum number of rows returned (default 10 000).

        Returns:
            List of :class:`MeterReading` ordered by timestamp ascending.
        """
        self.connect()

        conditions: list[str] = ["nmi = %s"]
        params: list = [nmi]

        if from_dt is not None:
            conditions.append('"timestamp" >= %s')
            params.append(from_dt)
        if to_dt is not None:
            conditions.append('"timestamp" <= %s')
            params.append(to_dt)

        sql = (
            f'SELECT id, nmi, "timestamp", consumption '
            f"FROM meter_readings "
            f"WHERE {' AND '.join(conditions)} "
            f'ORDER BY "timestamp" ASC '
            f"LIMIT %s;"
        )
        params.append(limit)

        readings: list[MeterReading] = []
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row_id, row_nmi, row_ts, row_con in cur.fetchall():
                    readings.append(
                        MeterReading(
                            id=str(row_id),
                            nmi=row_nmi,
                            timestamp=row_ts,
                            consumption=row_con,
                        )
                    )

        logger.debug(
            "postgres.fetch_by_nmi",
            nmi=nmi,
            from_dt=from_dt,
            to_dt=to_dt,
            rows=len(readings),
        )
        return readings

    def fetch_nmis(self) -> list[str]:
        """Return a sorted list of all distinct NMIs in the table."""
        self.connect()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT nmi FROM meter_readings ORDER BY nmi;")
                return [row[0] for row in cur.fetchall()]

    def fetch_date_range(self, nmi: str) -> tuple[datetime | None, datetime | None]:
        """
        Return (earliest_timestamp, latest_timestamp) for *nmi*.
        Returns (None, None) if the NMI has no data.
        """
        self.connect()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT MIN("timestamp"), MAX("timestamp") '
                    "FROM meter_readings WHERE nmi = %s;",
                    (nmi,),
                )
                row = cur.fetchone()
                if row:
                    return row[0], row[1]
                return None, None

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
