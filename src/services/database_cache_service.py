"""
Database Cache Service — write-through in-memory cache over PostgreSQL.

Architecture
------------
This service sits between the application layer and PostgreSQL and implements
the **write-through caching** pattern:

* Every write updates **both** the in-memory cache and the database in the
  same call.  If the DB write fails the cache is not updated, ensuring the
  two stores remain consistent.
* Every read checks the cache first.  On a cache miss the service queries
  PostgreSQL, populates the cache, and returns the result.

Internal data structures
------------------------
``_cache``
    ``dict[(nmi, timestamp_iso), MeterReading]``
    Primary lookup table – O(1) point reads.

``_nmi_index``
    ``dict[nmi, SortedTimestampList]``
    Secondary index mapping each NMI to its ordered list of cached timestamps.
    Used for O(k) range queries without a full table scan of ``_cache``.

Thread safety
-------------
A single ``threading.RLock`` guards **all** mutations to both ``_cache`` and
``_nmi_index``.  RLock (re-entrant) is used so that public methods that call
other public methods do not deadlock.

``bulk_write`` releases the lock between the DB write and the cache update
to avoid holding the lock during a network call.

Eviction policy
---------------
No automatic eviction (cache grows with data).  For large deployments add an
LRU layer on top of this class.  The ``clear_cache()`` method provides a
manual escape hatch.
"""
from __future__ import annotations

import threading
from bisect import bisect_left, bisect_right, insort
from datetime import datetime
from decimal import Decimal
from typing import Optional

import structlog

from src.database.postgres_handler import PostgresHandler
from src.models.meter_reading import MeterReading

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

_CacheKey = tuple[str, str]  # (nmi, timestamp.isoformat())


# ---------------------------------------------------------------------------
# Query result
# ---------------------------------------------------------------------------

class QueryResult:
    """Holds the result of a user query and its provenance."""

    __slots__ = ("readings", "from_cache", "from_db", "total")

    def __init__(
        self,
        readings: list[MeterReading],
        *,
        from_cache: int = 0,
        from_db: int = 0,
    ) -> None:
        self.readings = readings
        self.from_cache = from_cache
        self.from_db = from_db
        self.total = len(readings)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "from_cache": self.from_cache,
            "from_db": self.from_db,
            "readings": [r.to_dict() for r in self.readings],
        }


# ---------------------------------------------------------------------------
# Cache write/query result
# ---------------------------------------------------------------------------

class WriteResult:
    """Outcome of a write operation."""

    __slots__ = ("inserted", "skipped", "failed", "total", "cache_updated")

    def __init__(
        self,
        *,
        inserted: int = 0,
        skipped: int = 0,
        failed: int = 0,
        total: int = 0,
        cache_updated: int = 0,
    ) -> None:
        self.inserted = inserted
        self.skipped = skipped
        self.failed = failed
        self.total = total
        self.cache_updated = cache_updated

    def to_dict(self) -> dict:
        return {
            "inserted": self.inserted,
            "skipped": self.skipped,
            "failed": self.failed,
            "total": self.total,
            "cache_updated": self.cache_updated,
        }


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class DatabaseCacheService:
    """
    Write-through cache layer over :class:`PostgresHandler`.

    Write-through semantics
    -----------------------
    ``write(readings)`` → persists to DB → updates cache.
    If DB write raises an exception the cache is **not** updated and the
    exception propagates to the caller.

    Read semantics
    --------------
    ``query_by_nmi(nmi, …)`` checks the cache index first.  If the NMI is
    fully loaded (``_loaded_nmis`` set) the query is served entirely from
    cache.  Otherwise the DB is queried, results are merged into the cache,
    and the response is built from the unified view.

    Usage::

        handler = PostgresHandler.from_env()
        svc = DatabaseCacheService(handler)
        svc.initialize()        # ensure schema + warm cache (optional)

        result = svc.write(readings)
        result = svc.query_by_nmi("NEM1201009")
        result = svc.query_by_nmi("NEM1201009",
                                   from_dt=datetime(2026, 3, 1),
                                   to_dt=datetime(2026, 3, 31))
        summary = svc.cache_stats()
        svc.close()
    """

    def __init__(self, db: PostgresHandler) -> None:
        self._db = db

        # Primary cache: (nmi, ts_iso) → MeterReading
        self._cache: dict[_CacheKey, MeterReading] = {}

        # NMI secondary index: nmi → sorted list of timestamp ISO strings
        # Sorted list enables efficient range queries via bisect.
        self._nmi_index: dict[str, list[str]] = {}

        # Track which NMIs have been fully loaded from the DB.
        # If nmi ∈ _loaded_nmis, cache is authoritative for that NMI.
        self._loaded_nmis: set[str] = set()

        # Single re-entrant lock protects _cache, _nmi_index, _loaded_nmis
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def initialize(self, *, warm_nmis: list[str] | None = None) -> None:
        """
        Ensure the DB schema exists and optionally warm the cache.

        Args:
            warm_nmis: If provided, pre-load these NMIs into the cache.
                       Pass an empty list or omit to skip warming.
        """
        self._db.ensure_schema()
        logger.info("db_cache.initialized")

        if warm_nmis:
            for nmi in warm_nmis:
                self._load_nmi_from_db(nmi)

    def close(self) -> None:
        """Release DB connections."""
        self._db.close()
        logger.info("db_cache.closed")

    # ------------------------------------------------------------------
    # Write-through write
    # ------------------------------------------------------------------

    def write(self, readings: list[MeterReading]) -> WriteResult:
        """
        Persist *readings* to PostgreSQL then update the in-memory cache.

        The DB write happens **outside** the lock to avoid blocking readers
        during network I/O.  The cache is updated inside the lock only if
        the DB write succeeds.

        Args:
            readings: Readings to persist.  Duplicates (same NMI+timestamp)
                      are handled by DB's ON CONFLICT DO NOTHING.

        Returns:
            :class:`WriteResult` with insert/skip/fail counts.
        """
        if not readings:
            return WriteResult()

        # 1. Write to DB (no lock held – network I/O)
        try:
            db_stats = self._db.bulk_insert(readings)
        except Exception as exc:
            logger.error("db_cache.write_failed", error=str(exc), count=len(readings))
            raise

        # 2. Update cache (lock held – in-memory only)
        cache_updated = 0
        written_nmis: set[str] = set()
        with self._lock:
            for r in readings:
                key: _CacheKey = (r.nmi, r.timestamp.isoformat())
                if key not in self._cache:
                    self._cache[key] = r
                    self._index_reading(r)
                    cache_updated += 1
                written_nmis.add(r.nmi)

            # Mark each written NMI as fully loaded in cache.
            # Contract: this service is the authoritative writer; all writes
            # flow through write(), so after the first write for a given NMI
            # the cache is the source of truth for that NMI.
            for nmi in written_nmis:
                self._loaded_nmis.add(nmi)

        logger.info(
            "db_cache.write_through_complete",
            total=db_stats["total"],
            inserted=db_stats["inserted"],
            skipped=db_stats["skipped"],
            failed=db_stats["failed"],
            cache_updated=cache_updated,
        )

        return WriteResult(
            inserted=db_stats["inserted"],
            skipped=db_stats["skipped"],
            failed=db_stats["failed"],
            total=db_stats["total"],
            cache_updated=cache_updated,
        )

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------

    def query_by_nmi(
        self,
        nmi: str,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        limit: int = 10_000,
    ) -> QueryResult:
        """
        Retrieve interval readings for *nmi*, optionally within a time range.

        Cache-first strategy:
          - If *nmi* is in ``_loaded_nmis`` the query is served from cache.
          - Otherwise the DB is queried, results are merged into cache, and
            the NMI is added to ``_loaded_nmis``.

        Args:
            nmi:     National Metering Identifier.
            from_dt: Inclusive start of the time window (None = no lower bound).
            to_dt:   Inclusive end of the time window (None = no upper bound).
            limit:   Maximum rows to return.

        Returns:
            :class:`QueryResult` containing readings and provenance metadata.
        """
        from_cache = 0
        from_db = 0

        with self._lock:
            is_loaded = nmi in self._loaded_nmis

        if is_loaded:
            readings = self._query_cache(nmi, from_dt, to_dt, limit)
            from_cache = len(readings)
            logger.debug(
                "db_cache.cache_hit",
                nmi=nmi,
                rows=len(readings),
            )
        else:
            # DB query (no lock during I/O)
            db_readings = self._db.fetch_by_nmi(nmi, from_dt, to_dt, limit)
            from_db = len(db_readings)

            # Merge into cache
            with self._lock:
                for r in db_readings:
                    key: _CacheKey = (r.nmi, r.timestamp.isoformat())
                    if key not in self._cache:
                        self._cache[key] = r
                        self._index_reading(r)
                # Mark NMI as fully loaded only for unbounded queries
                if from_dt is None and to_dt is None:
                    self._loaded_nmis.add(nmi)

            # Build result from cache to guarantee consistent ordering
            readings = self._query_cache(nmi, from_dt, to_dt, limit)
            from_cache = len(readings) - from_db  # items already in cache

            logger.debug(
                "db_cache.db_fallback",
                nmi=nmi,
                from_db=from_db,
                total=len(readings),
            )

        return QueryResult(
            readings=readings,
            from_cache=from_cache,
            from_db=from_db,
        )

    def query_nmis(self) -> list[str]:
        """
        Return a sorted list of all known NMIs.

        If at least one NMI has been loaded into cache, returns the cached
        set.  Otherwise falls back to a DB query.
        """
        with self._lock:
            if self._nmi_index:
                return sorted(self._nmi_index.keys())

        return self._db.fetch_nmis()

    def query_date_range(self, nmi: str) -> tuple[datetime | None, datetime | None]:
        """
        Return (earliest, latest) timestamps for *nmi*.

        Uses the cache index when the NMI is loaded; otherwise queries DB.
        """
        with self._lock:
            if nmi in self._loaded_nmis and nmi in self._nmi_index:
                ts_list = self._nmi_index[nmi]
                if not ts_list:
                    return None, None
                return (
                    datetime.fromisoformat(ts_list[0]),
                    datetime.fromisoformat(ts_list[-1]),
                )

        return self._db.fetch_date_range(nmi)

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Remove all in-memory data (DB is not affected)."""
        with self._lock:
            self._cache.clear()
            self._nmi_index.clear()
            self._loaded_nmis.clear()
        logger.info("db_cache.cache_cleared")

    def evict_nmi(self, nmi: str) -> int:
        """
        Remove all cached readings for *nmi*.

        Returns the number of entries evicted.
        """
        with self._lock:
            keys_to_remove = [k for k in self._cache if k[0] == nmi]
            for k in keys_to_remove:
                del self._cache[k]
            self._nmi_index.pop(nmi, None)
            self._loaded_nmis.discard(nmi)

        logger.info("db_cache.nmi_evicted", nmi=nmi, count=len(keys_to_remove))
        return len(keys_to_remove)

    def cache_stats(self) -> dict:
        """Return a snapshot of cache metrics (thread-safe)."""
        with self._lock:
            return {
                "total_readings": len(self._cache),
                "total_nmis": len(self._nmi_index),
                "fully_loaded_nmis": sorted(self._loaded_nmis),
                "nmi_reading_counts": {
                    nmi: len(ts_list)
                    for nmi, ts_list in self._nmi_index.items()
                },
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _index_reading(self, reading: MeterReading) -> None:
        """Insert a reading's timestamp into the NMI sorted index. NOT thread-safe; caller must hold _lock."""
        ts_iso = reading.timestamp.isoformat()
        nmi = reading.nmi
        if nmi not in self._nmi_index:
            self._nmi_index[nmi] = []
        ts_list = self._nmi_index[nmi]
        # insort keeps the list sorted; O(log n) search + O(n) insert
        # Acceptable for cache-sized data; replace with SortedList for huge caches.
        if ts_iso not in ts_list:
            insort(ts_list, ts_iso)

    def _query_cache(
        self,
        nmi: str,
        from_dt: datetime | None,
        to_dt: datetime | None,
        limit: int,
    ) -> list[MeterReading]:
        """
        Return readings from the in-memory cache for *nmi* within [from_dt, to_dt].
        Result is ordered by timestamp ascending. NOT thread-safe; caller must hold _lock.
        """
        with self._lock:
            ts_list = self._nmi_index.get(nmi, [])
            if not ts_list:
                return []

            # Binary search to find the slice of timestamps in range
            lo = bisect_left(ts_list, from_dt.isoformat()) if from_dt else 0
            hi = (
                bisect_right(ts_list, to_dt.isoformat())
                if to_dt
                else len(ts_list)
            )

            relevant_timestamps = ts_list[lo:hi][:limit]
            readings = [
                self._cache[(nmi, ts)]
                for ts in relevant_timestamps
                if (nmi, ts) in self._cache
            ]

        return readings

    def _load_nmi_from_db(self, nmi: str) -> None:
        """Fetch all readings for *nmi* from DB and store in cache."""
        logger.info("db_cache.warming", nmi=nmi)
        db_readings = self._db.fetch_by_nmi(nmi)
        with self._lock:
            for r in db_readings:
                key: _CacheKey = (r.nmi, r.timestamp.isoformat())
                if key not in self._cache:
                    self._cache[key] = r
                    self._index_reading(r)
            self._loaded_nmis.add(nmi)
        logger.info("db_cache.warmed", nmi=nmi, count=len(db_readings))
