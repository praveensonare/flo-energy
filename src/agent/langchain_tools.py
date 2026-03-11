"""
LangChain async tool definitions for the NEM12/NEM13 ETL agent.
All tools are async; blocking I/O runs in a ThreadPoolExecutor via run_in_executor.

Design
------
* Every tool is ``async def`` decorated with ``@tool``.  LangGraph can call
  multiple tools concurrently in a single turn (e.g. validate_data +
  generate_sql after parse_file).
* Both CSV and ZIP input files are supported: ``read_file`` and ``parse_file``
  transparently extract ZIP archives before passing the CSV to the parser.
* Session state is stored in a module-level dict guarded by ``asyncio.Lock``
  to prevent race conditions when multiple files are processed concurrently.
* ``write_to_database`` calls ``ensure_schema()`` before every write, so the
  ``meter_readings`` table is created automatically on first use without
  requiring any manual setup.
* All DB writes use ``atomic_bulk_insert``: every batch shares one PostgreSQL
  connection/transaction, giving full ACID guarantees per file.
"""
from __future__ import annotations

import asyncio
import functools
import json
import os
import re
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
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
# NEM12 filename validation — spec section 3.2.2
# ---------------------------------------------------------------------------

# NEM12 Alternative Delivery naming convention:
#   NEM12#<UniqueID>#<From>#<To>.csv|zip  (case-insensitive)
#   UniqueID: 1-36 alphanumeric chars; From/To: 1-10 alphanumeric chars
_FILENAME_RE = re.compile(
    r"^nem12"
    r"#([A-Za-z0-9]{1,36})"   # #UniqueID
    r"#([A-Za-z0-9]{1,10})"   # #From participant
    r"#([A-Za-z0-9]{1,10})"   # #To participant
    r"\.(csv|zip)$",
    re.IGNORECASE,
)


@dataclass
class FilenameInfo:
    # Parsed components of a validated NEM12 filename.
    # Returned by validate_filename and used to log audit-trail metadata.
    unique_id: str
    from_participant: str
    to_participant: str
    extension: str  # "csv" or "zip"


# ---------------------------------------------------------------------------
# Shared state — asyncio.Lock prevents concurrent mutation of the same session
# ---------------------------------------------------------------------------

_session_lock: asyncio.Lock = asyncio.Lock()
# Maps session_id → ParseSession; each file run gets its own entry.
_sessions: dict[str, ParseSession] = {}

# Lazily initialised process-wide singletons.
_db: PostgresHandler | None = None
_notifier: NotificationService | None = None


def _get_db() -> PostgresHandler:
    # Return the shared PostgresHandler, creating it on first call.
    # Uses from_env() so all connection config is driven by .env / env vars.
    global _db
    if _db is None:
        _db = PostgresHandler.from_env()
    return _db


def _get_notifier() -> NotificationService:
    # Return the shared NotificationService, creating it on first call.
    # default() registers Console + Log handlers out of the box.
    global _notifier
    if _notifier is None:
        _notifier = NotificationService.default()
    return _notifier


# ---------------------------------------------------------------------------
# ZIP / CSV resolution helper
# ---------------------------------------------------------------------------

def _resolve_csv_path(file_path: str) -> tuple[str, str | None]:
    """
    Return (csv_path, temp_dir) for the given input file.
    For CSV files temp_dir is None; for ZIP files a temp directory is created
    and the caller is responsible for calling shutil.rmtree(temp_dir).
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return file_path, None

    if ext == ".zip":
        if not zipfile.is_zipfile(file_path):
            raise ValueError(f"File has .zip extension but is not a valid ZIP archive: {file_path}")

        temp_dir = tempfile.mkdtemp(prefix="nem_extract_")
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                # Reject password-protected archives — NEM12 spec section 3.2.2
                for info in zf.infolist():
                    if info.flag_bits & 0x1:
                        raise ValueError(
                            f"Password-protected ZIP is not supported: {file_path}"
                        )

                csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_members:
                    raise ValueError(f"No .csv file found inside ZIP archive: {file_path}")

                # NEM12 spec implies one CSV file per archive; take the first one.
                zf.extract(csv_members[0], temp_dir)
                csv_path = os.path.join(temp_dir, csv_members[0])

            logger.info("tool.zip_extracted", zip=file_path, csv=csv_path)
            return csv_path, temp_dir

        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    raise ValueError(
        f"Unsupported file extension '{ext}' for '{file_path}'. Expected .csv or .zip."
    )


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def _json_default(obj: Any) -> Any:
    # Handle Decimal and datetime objects during JSON serialisation.
    # Raises TypeError for anything else so silent data loss is avoided.
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serialisable: {type(obj)}")


def _ok(data: dict[str, Any]) -> str:
    # Wrap a success payload as a JSON string with status="ok".
    # All tools return strings so Claude receives structured JSON it can reason about.
    return json.dumps({"status": "ok", **data}, default=_json_default)


def _error(message: str) -> str:
    # Wrap an error message as a JSON string with status="error".
    # Returning an error (not raising) lets the agent decide how to recover.
    return json.dumps({"status": "error", "error": message})


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool
async def validate_filename(file_path: str) -> str:
    """
    Validate that the filename follows the NEM12 Alternative Delivery naming
    convention: NEM12#<UniqueID>#<From>#<To>.csv|zip (case-insensitive).
    Returns parsed filename components. Always call this before read_file.
    """
    filename = Path(file_path).name
    match = _FILENAME_RE.match(filename)
    if not match:
        return _error(
            f"Filename '{filename}' does not conform to the NEM12 naming convention "
            "NEM12#<UniqueID>#<From>#<To>.<csv|zip> (case-insensitive). "
            "Example: nem12#ABC123#TESTMDP1#TESTRETAIL.csv"
        )
    unique_id, from_p, to_p, ext = match.groups()
    logger.info(
        "tool.validate_filename",
        filename=filename,
        unique_id=unique_id,
        from_participant=from_p,
        to_participant=to_p,
    )
    return _ok(
        {
            "filename": filename,
            "unique_id": unique_id,
            "from_participant": from_p,
            "to_participant": to_p,
            "extension": ext.lower(),
        }
    )


@tool
async def read_file(file_path: str) -> str:
    """
    Validate a meter data file (CSV or ZIP) and detect its format (NEM12/NEM13).
    Returns session_id, detected format, and file size in bytes. Call this first.
    """
    if not os.path.exists(file_path):
        return _error(f"File not found: '{file_path}'")
    if not os.path.isfile(file_path):
        return _error(f"Path is not a file: '{file_path}'")

    file_size = os.path.getsize(file_path)
    loop = asyncio.get_running_loop()

    # Resolve CSV path (extracts ZIP if needed) then detect format.
    # For ZIP files the temp dir is cleaned up immediately after detection.
    temp_dir: str | None = None
    try:
        csv_path, temp_dir = await loop.run_in_executor(None, _resolve_csv_path, file_path)
        detected_format = await loop.run_in_executor(None, ParserFactory.get_format, csv_path)
    except ValueError as exc:
        return _error(str(exc))
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)

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
    Parse a meter data file (CSV or ZIP) and extract all MeterReading records.
    Returns session_id, total readings, NMIs found, and any parse errors.
    """
    loop = asyncio.get_running_loop()

    # Find an existing session for this file or create a new one.
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

    temp_dir: str | None = None
    try:
        # Extract ZIP to a temp directory if needed; CSV files pass through unchanged.
        csv_path, temp_dir = await loop.run_in_executor(None, _resolve_csv_path, file_path)
        parser = ParserFactory.get_parser(csv_path)
        result = await loop.run_in_executor(None, parser.parse, csv_path)

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
        # NEM13 parser is a stub; report clearly rather than crashing.
        return _error(str(exc))
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        logger.exception("tool.parse_file.error", file=file_path, error=str(exc))
        return _error(f"Parse failed: {exc}")
    finally:
        get_metrics().active_sessions.dec()
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


@tool
async def validate_data(session_id: str) -> str:
    """
    Validate parsed meter readings for duplicates, negative consumption, and future timestamps.
    Returns a validation report with counts. Safe to run in parallel with generate_sql.
    """
    async with _session_lock:
        session = _sessions.get(session_id)
    if session is None:
        return _error(f"Session not found: '{session_id}'")

    validation = ValidationResult(is_valid=True)
    now = datetime.utcnow()

    # -- Duplicate (NMI, timestamp) detection --
    seen: set[tuple[str, str]] = set()
    for reading in session.readings:
        key = (reading.nmi, reading.timestamp.isoformat())
        if key in seen:
            validation.duplicate_count += 1
        else:
            seen.add(key)
    if validation.duplicate_count:
        validation.warnings.append(
            f"{validation.duplicate_count} duplicate (NMI, timestamp) pair(s). "
            "ON CONFLICT DO NOTHING will deduplicate on insert."
        )

    # -- Negative consumption (NEM12 spec section 4.4) --
    negative = [r for r in session.readings if r.consumption < Decimal("0")]
    if negative:
        validation.invalid_count += len(negative)
        validation.errors.append(
            f"{len(negative)} reading(s) have negative consumption, "
            "which is not allowed by NEM12 spec section 4.4."
        )

    # -- NMI length (spec: max 10 alphanumeric chars) --
    invalid_nmi = [r for r in session.readings if not r.nmi or len(r.nmi) > 10]
    if invalid_nmi:
        validation.invalid_count += len(invalid_nmi)
        validation.errors.append(
            f"{len(invalid_nmi)} reading(s) have an invalid NMI "
            "(empty or longer than 10 characters)."
        )

    # -- Future timestamps --
    future = [r for r in session.readings if r.timestamp > now]
    if future:
        validation.warnings.append(
            f"{len(future)} reading(s) have timestamps in the future "
            f"(latest: {max(r.timestamp for r in future).isoformat()})."
        )

    # -- Zero consumption (informational warning) --
    zero_count = sum(1 for r in session.readings if r.consumption == Decimal("0"))
    if zero_count:
        validation.warnings.append(
            f"{zero_count} interval(s) have zero consumption "
            "(may indicate meter outage or off-peak period)."
        )

    # -- Sequential date ordering per NMI (spec section 4.4) --
    nmi_dates: dict[str, list[datetime]] = {}
    for r in session.readings:
        nmi_dates.setdefault(r.nmi, []).append(r.timestamp)
    for nmi, timestamps in nmi_dates.items():
        if timestamps != sorted(timestamps):
            validation.warnings.append(
                f"NMI {nmi}: interval timestamps are not in ascending order "
                "(NEM12 spec section 4.4 requires sequential date order)."
            )

    validation.valid_count = (
        len(session.readings) - validation.invalid_count - validation.duplicate_count
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
        duplicates=validation.duplicate_count,
    )
    return _ok(validation.to_dict())


@tool
async def generate_sql(session_id: str, output_path: str = "") -> str:
    """
    Generate SQL INSERT statements for the session's parsed readings.
    Optionally writes them to a file. Safe to run in parallel with validate_data.
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
        # Create parent directories if needed then write the SQL file.
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w") as fh:
            fh.write(sql)
        logger.info(
            "tool.generate_sql",
            session_id=session_id,
            output=output_path,
            statements=len(session.readings),
        )

    # Return only the first 5 statements to keep the LLM context window manageable.
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
    Write parsed readings to PostgreSQL using a single ACID transaction per file.
    Auto-creates the meter_readings table if it does not yet exist.
    """
    async with _session_lock:
        session = _sessions.get(session_id)
    if session is None:
        return _error(f"Session not found: '{session_id}'")

    if not session.readings:
        return _ok({"inserted": 0, "skipped": 0, "failed": 0, "total": 0})

    # Reject readings with negative consumption before touching the database.
    valid_readings = [r for r in session.readings if r.consumption >= 0]
    skipped_invalid = len(session.readings) - len(valid_readings)

    loop = asyncio.get_running_loop()
    try:
        # ensure_schema() runs CREATE TABLE IF NOT EXISTS — safe on an existing DB.
        await loop.run_in_executor(None, _get_db().ensure_schema)

        # atomic_bulk_insert wraps all batches in ONE psycopg2 transaction:
        #   Atomicity  – all batches commit together or all roll back.
        #   Consistency – UNIQUE(nmi, timestamp) constraint enforced by DB.
        #   Isolation  – other sessions see all-or-nothing for this file.
        #   Durability – PostgreSQL WAL guarantees persistence after commit.
        fn = functools.partial(
            _get_db().atomic_bulk_insert, valid_readings, batch_size
        )
        result = await loop.run_in_executor(None, fn)

    except Exception as exc:
        logger.error(
            "tool.write_to_database.failed",
            session_id=session_id,
            error=str(exc),
        )
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
    Dispatch a notification about processing status to all registered handlers.
    level must be one of: 'info', 'warning', 'error', 'success'.
    """
    _get_notifier().notify(level, message, context)
    return _ok({"dispatched": True, "level": level, "message": message})


@tool
async def get_processing_stats(session_id: str) -> str:
    """
    Return a summary dict for the given session: readings, NMIs, insert counts.
    Useful for the agent to diagnose partial failures or report final status.
    """
    async with _session_lock:
        session = _sessions.get(session_id)
    if session is None:
        return _error(f"Session not found: '{session_id}'")
    return _ok(session.summary())


# ---------------------------------------------------------------------------
# Exported tool list — passed to create_react_agent in langchain_agent.py
# ---------------------------------------------------------------------------

ALL_TOOLS = [
    validate_filename,
    read_file,
    parse_file,
    validate_data,
    generate_sql,
    write_to_database,
    send_notification,
    get_processing_stats,
]
