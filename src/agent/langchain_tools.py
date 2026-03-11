"""
LangChain async tool definitions for the NEM12/NEM13 ETL agent.

Design
------
* Every tool is an ``async def`` decorated with ``@tool`` so LangGraph can
  execute them concurrently when the model emits multiple tool calls in one
  turn (e.g. validate_data + generate_sql in parallel after parse_file).

* Session state is stored in a module-level dict guarded by ``asyncio.Lock``
  to prevent race conditions when multiple files are processed concurrently.

* All blocking I/O (file parsing, DB writes) is offloaded to the default
  ``ThreadPoolExecutor`` via ``asyncio.get_running_loop().run_in_executor()``
  so the event loop is never blocked.

* ``write_to_database`` calls ``atomic_bulk_insert`` which wraps every batch
  in a single PostgreSQL transaction — ensuring ACID atomicity per file.
"""
from __future__ import annotations

import asyncio
import functools
import json
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import structlog
from langchain_core.tools import tool

from src.database.postgres_handler import PostgresHandler
from src.models.meter_reading import ParseSession, ValidationResult
from src.notifications.notification_service import NotificationService
from src.observability.metrics import get_metrics
from src.parsers.parser_factory import ParserFactory

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Shared state — asyncio.Lock guards concurrent tool calls for the same session
# ---------------------------------------------------------------------------

_session_lock: asyncio.Lock = asyncio.Lock()
_sessions: dict[str, ParseSession] = {}

# Lazily initialised singletons (one per process)
_db: PostgresHandler | None = None
_notifier: NotificationService | None = None


def _get_db() -> PostgresHandler:
    global _db
    if _db is None:
        _db = PostgresHandler.from_env()
    return _db


def _get_notifier() -> NotificationService:
    global _notifier
    if _notifier is None:
        _notifier = NotificationService.default()
    return _notifier


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serialisable: {type(obj)}")


def _ok(data: dict[str, Any]) -> str:
    return json.dumps({"status": "ok", **data}, default=_json_default)


def _error(message: str) -> str:
    return json.dumps({"status": "error", "error": message})


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool
async def read_file(file_path: str) -> str:
    """
    Validate a meter data file and detect its format (NEM12 or NEM13).
    Returns session_id, format, and file size. Call this first.
    """
    if not os.path.exists(file_path):
        return _error(f"File not found: '{file_path}'")
    if not os.path.isfile(file_path):
        return _error(f"Path is not a file: '{file_path}'")

    file_size = os.path.getsize(file_path)
    loop = asyncio.get_running_loop()
    detected_format = await loop.run_in_executor(
        None, ParserFactory.get_format, file_path
    )

    session = ParseSession()
    session.file_path = file_path
    session.file_format = detected_format
    session.file_size_bytes = file_size

    async with _session_lock:
        _sessions[session.session_id] = session

    logger.info(
        "tool.read_file",
        session_id=session.session_id,
        format=detected_format,
        size_bytes=file_size,
    )
    return _ok(
        {
            "session_id": session.session_id,
            "file_path": file_path,
            "format": detected_format,
            "file_size_bytes": file_size,
        }
    )


@tool
async def parse_file(file_path: str, format: str) -> str:
    """
    Parse a meter data file and extract all MeterReading records.
    Returns session_id, total readings, NMIs found, and any parse errors.
    """
    loop = asyncio.get_running_loop()

    # Find or create session; lock for consistent read-then-write
    async with _session_lock:
        session = next(
            (s for s in _sessions.values() if s.file_path == file_path), None
        )
        if session is None:
            session = ParseSession()
            session.file_path = file_path
            session.file_format = format
            _sessions[session.session_id] = session

    session_id = session.session_id
    get_metrics().active_sessions.inc()

    try:
        parser = ParserFactory.get_parser(file_path)
        result = await loop.run_in_executor(None, parser.parse, file_path)

        async with _session_lock:
            session.readings = result.readings
            session.total_readings = len(result.readings)
            session.nmis = result.nmis
            session.total_nmis = len(result.nmis)
            session.errors.extend(result.errors)
            session.warnings.extend(result.warnings)
            session.parsed_at = datetime.utcnow()

        logger.info(
            "tool.parse_file",
            session_id=session_id,
            total_readings=len(result.readings),
            nmis=result.nmis,
            errors=len(result.errors),
        )
        return _ok(session.summary())

    except NotImplementedError as exc:
        return _error(str(exc))
    except Exception as exc:
        logger.exception("tool.parse_file.error", file=file_path, error=str(exc))
        return _error(f"Parse failed: {exc}")
    finally:
        get_metrics().active_sessions.dec()


@tool
async def validate_data(session_id: str) -> str:
    """
    Validate parsed meter readings: duplicates, negative consumption, future timestamps.
    Returns a validation report with counts. Can run in parallel with generate_sql.
    """
    async with _session_lock:
        session = _sessions.get(session_id)
    if session is None:
        return _error(f"Session not found: '{session_id}'")

    validation = ValidationResult(is_valid=True)
    seen: set[tuple[str, str]] = set()

    for reading in session.readings:
        key = (reading.nmi, reading.timestamp.isoformat())
        if key in seen:
            validation.duplicate_count += 1
            validation.warnings.append(
                f"Duplicate: NMI={reading.nmi} ts={reading.timestamp}"
            )
        else:
            seen.add(key)

        if reading.consumption < 0:
            validation.invalid_count += 1
            validation.errors.append(
                f"Negative consumption: NMI={reading.nmi} ts={reading.timestamp}"
            )

        if reading.timestamp > datetime.utcnow():
            validation.warnings.append(
                f"Future timestamp: NMI={reading.nmi} ts={reading.timestamp}"
            )

    validation.valid_count = (
        len(session.readings)
        - validation.invalid_count
        - validation.duplicate_count
    )
    validation.is_valid = validation.invalid_count == 0

    async with _session_lock:
        session.validated = True

    get_metrics().validation_errors.inc(validation.invalid_count)
    logger.info(
        "tool.validate_data",
        session_id=session_id,
        is_valid=validation.is_valid,
        valid=validation.valid_count,
        invalid=validation.invalid_count,
    )
    return _ok(validation.to_dict())


@tool
async def generate_sql(session_id: str, output_path: str = "") -> str:
    """
    Generate SQL INSERT statements for parsed readings.
    Optionally write to a file. Can run in parallel with validate_data.
    """
    async with _session_lock:
        session = _sessions.get(session_id)
    if session is None:
        return _error(f"Session not found: '{session_id}'")

    if not session.readings:
        return _ok({"statement_count": 0, "sql_preview": "", "output_path": None})

    loop = asyncio.get_running_loop()
    sql = await loop.run_in_executor(
        None, _get_db().generate_insert_sql, session.readings
    )

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w") as fh:
            fh.write(sql)
        logger.info(
            "tool.generate_sql",
            session_id=session_id,
            output=output_path,
            statements=len(session.readings),
        )

    preview = "\n".join(sql.split("\n")[:5])
    if len(session.readings) > 5:
        preview += f"\n... ({len(session.readings) - 5} more)"

    return _ok(
        {
            "statement_count": len(session.readings),
            "sql_preview": preview,
            "output_path": output_path or None,
        }
    )


@tool
async def write_to_database(session_id: str, batch_size: int = 1000) -> str:
    """
    Write parsed meter readings to PostgreSQL with ACID guarantees.
    Uses a single transaction per file (atomic_bulk_insert) so either all
    readings are committed or none are. Returns insert/skip/fail counts.
    """
    async with _session_lock:
        session = _sessions.get(session_id)
    if session is None:
        return _error(f"Session not found: '{session_id}'")

    if not session.readings:
        return _ok({"inserted": 0, "skipped": 0, "failed": 0, "total": 0})

    # Filter invalid readings before the atomic transaction
    valid_readings = [r for r in session.readings if r.consumption >= 0]
    skipped_invalid = len(session.readings) - len(valid_readings)

    loop = asyncio.get_running_loop()
    try:
        # Ensure the table exists before writing; CREATE TABLE IF NOT EXISTS
        # is a no-op when the table already exists, so this is always safe.
        await loop.run_in_executor(None, _get_db().ensure_schema)

        # atomic_bulk_insert wraps all batches in ONE PostgreSQL transaction:
        #   Atomicity  – all commit together or all roll back
        #   Consistency – UNIQUE(nmi, timestamp) constraint enforced
        #   Isolation  – other sessions see all-or-nothing
        #   Durability – PostgreSQL WAL guarantees persistence
        fn = functools.partial(
            _get_db().atomic_bulk_insert, valid_readings, batch_size
        )
        result = await loop.run_in_executor(None, fn)
    except Exception as exc:
        logger.error("tool.write_to_database.failed", session_id=session_id, error=str(exc))
        return _error(f"DB write failed (transaction rolled back): {exc}")

    async with _session_lock:
        session.inserted = result["inserted"]
        session.skipped = result["skipped"] + skipped_invalid
        session.failed = result["failed"]

    get_metrics().readings_inserted.inc(result["inserted"])
    get_metrics().readings_skipped.inc(result["skipped"] + skipped_invalid)
    get_metrics().readings_failed.inc(result["failed"])

    logger.info(
        "tool.write_to_database",
        session_id=session_id,
        skipped_invalid=skipped_invalid,
        **result,
    )
    return _ok({**result, "skipped_invalid": skipped_invalid})


@tool
async def send_notification(
    level: str, message: str, context: Optional[dict] = None
) -> str:
    """
    Send a notification about processing status.
    level: 'info' | 'warning' | 'error' | 'success'
    """
    _get_notifier().notify(level, message, context)
    return _ok({"dispatched": True, "level": level, "message": message})


@tool
async def get_processing_stats(session_id: str) -> str:
    """Return real-time processing statistics for the current session."""
    async with _session_lock:
        session = _sessions.get(session_id)
    if session is None:
        return _error(f"Session not found: '{session_id}'")
    return _ok(session.summary())


# ---------------------------------------------------------------------------
# Tool registry exported to the agent
# ---------------------------------------------------------------------------

ALL_TOOLS = [
    read_file,
    parse_file,
    validate_data,
    generate_sql,
    write_to_database,
    send_notification,
    get_processing_stats,
]
