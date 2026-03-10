"""
Agent tool definitions and implementations.

Each tool is:
  1. Declared as a JSON schema (TOOL_DEFINITIONS) – sent to the Claude API
  2. Implemented as a callable (ToolRegistry.execute) – called when Claude
     requests it

Session state is kept in an in-memory dict keyed by session_id.  For
production deployments handling many concurrent files this would be backed
by Redis or a database, but an in-memory dict is sufficient for a
containerised single-process service.

Tool interface contract:
  Input:  dict (validated against the JSON schema Claude sends)
  Output: str  (returned to Claude as tool_result content)

All tools catch exceptions internally and return structured error JSON
so the agent can reason about failures and decide on a recovery strategy.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

import structlog

from src.database.postgres_handler import PostgresHandler
from src.models.meter_reading import ParseSession, ValidationResult
from src.notifications.notification_service import (
    NotificationLevel,
    NotificationService,
)
from src.observability.metrics import get_metrics
from src.parsers.parser_factory import ParserFactory

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Session store
# ---------------------------------------------------------------------------

_sessions: dict[str, ParseSession] = {}


def _get_session(session_id: str) -> ParseSession | None:
    return _sessions.get(session_id)


def _new_session() -> ParseSession:
    session = ParseSession()
    _sessions[session.session_id] = session
    return session


# ---------------------------------------------------------------------------
# Tool schemas (sent to the Claude API)
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "read_file",
        "description": (
            "Validate a meter data file and detect its format (NEM12 / NEM13). "
            "Returns metadata: session_id, detected format, file size, and whether "
            "the file is accessible.  Call this first before any other tool."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Absolute or relative path to the NEM12/NEM13 CSV file.",
                }
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "parse_file",
        "description": (
            "Parse a meter data file and extract all MeterReading records. "
            "Handles arbitrarily large files by streaming line-by-line. "
            "Returns session_id, total readings parsed, NMIs found, and any errors."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the meter data file.",
                },
                "format": {
                    "type": "string",
                    "enum": ["NEM12", "NEM13"],
                    "description": "File format.  Use the value returned by read_file.",
                },
            },
            "required": ["file_path", "format"],
        },
    },
    {
        "name": "validate_data",
        "description": (
            "Validate the parsed meter readings for data integrity: "
            "checks for duplicates, out-of-range consumption, future timestamps, "
            "and malformed NMIs.  Returns a validation report."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "Session ID returned by parse_file.",
                }
            },
            "required": ["session_id"],
        },
    },
    {
        "name": "generate_sql",
        "description": (
            "Generate SQL INSERT statements for the parsed meter readings. "
            "Optionally writes them to an output file. "
            "Returns the SQL as a string (truncated for large datasets) and "
            "the path to the output file if requested."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "Session ID from parse_file / validate_data.",
                },
                "output_path": {
                    "type": "string",
                    "description": (
                        "Optional path to write the SQL file "
                        "(e.g. /output/inserts.sql).  "
                        "If omitted, SQL is returned inline."
                    ),
                },
            },
            "required": ["session_id"],
        },
    },
    {
        "name": "write_to_database",
        "description": (
            "Write the parsed meter readings to the PostgreSQL database. "
            "Uses multi-threaded batch inserts with ON CONFLICT DO NOTHING "
            "for idempotent operation.  Returns counts of inserted/skipped/failed records."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "Session ID from parse_file / validate_data.",
                },
                "batch_size": {
                    "type": "integer",
                    "description": "Records per batch (default 1000).",
                    "default": 1000,
                },
            },
            "required": ["session_id"],
        },
    },
    {
        "name": "send_notification",
        "description": (
            "Send a notification about processing status. "
            "Use for errors, warnings, and completion events."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "level": {
                    "type": "string",
                    "enum": ["info", "warning", "error", "success"],
                    "description": "Severity level.",
                },
                "message": {
                    "type": "string",
                    "description": "Human-readable notification message.",
                },
                "context": {
                    "type": "object",
                    "description": "Optional key-value pairs for additional context.",
                },
            },
            "required": ["level", "message"],
        },
    },
    {
        "name": "get_processing_stats",
        "description": (
            "Return real-time processing statistics for the current session: "
            "readings parsed, validation errors, DB insert counts."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "Session ID to retrieve stats for.",
                }
            },
            "required": ["session_id"],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

class ToolRegistry:
    """
    Executes tool calls requested by the Claude agent.

    Each execute_<name> method corresponds to one entry in TOOL_DEFINITIONS.
    Returns a JSON string that becomes the tool_result content sent back
    to the model.
    """

    def __init__(
        self,
        db_handler: PostgresHandler | None = None,
        notification_service: NotificationService | None = None,
    ) -> None:
        self._db = db_handler or PostgresHandler.from_env()
        self._notifier = notification_service or NotificationService.default()

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def execute(self, tool_name: str, tool_input: dict[str, Any]) -> str:
        """Route to the appropriate execute_* method."""
        handler = getattr(self, f"_tool_{tool_name}", None)
        if handler is None:
            return _error(f"Unknown tool: '{tool_name}'")
        try:
            return handler(tool_input)
        except Exception as exc:
            logger.exception("tool.unhandled_error", tool=tool_name, error=str(exc))
            return _error(f"Tool '{tool_name}' raised an unexpected error: {exc}")

    # ------------------------------------------------------------------
    # Individual tool implementations
    # ------------------------------------------------------------------

    def _tool_read_file(self, inp: dict[str, Any]) -> str:
        file_path = inp["file_path"]

        if not os.path.exists(file_path):
            return _error(f"File not found: '{file_path}'")

        if not os.path.isfile(file_path):
            return _error(f"Path is not a file: '{file_path}'")

        file_size = os.path.getsize(file_path)
        detected_format = ParserFactory.get_format(file_path)

        session = _new_session()
        session.file_path = file_path
        session.file_format = detected_format
        session.file_size_bytes = file_size

        logger.info(
            "tool.read_file",
            session_id=session.session_id,
            file=file_path,
            format=detected_format,
            size_bytes=file_size,
        )
        return _ok(
            {
                "session_id": session.session_id,
                "file_path": file_path,
                "format": detected_format,
                "file_size_bytes": file_size,
                "file_size_human": _human_size(file_size),
            }
        )

    def _tool_parse_file(self, inp: dict[str, Any]) -> str:
        file_path = inp["file_path"]
        fmt = inp.get("format", "")

        # Find or create session
        session = self._find_session_by_path(file_path)
        if session is None:
            session = _new_session()
            session.file_path = file_path
            session.file_format = fmt

        get_metrics().active_sessions.inc()
        start = time.monotonic()

        try:
            parser = ParserFactory.get_parser(file_path)
            result = parser.parse(file_path)

            session.readings = result.readings
            session.total_readings = len(result.readings)
            session.nmis = result.nmis
            session.total_nmis = len(result.nmis)
            session.errors.extend(result.errors)
            session.warnings.extend(result.warnings)
            session.parsed_at = datetime.utcnow()

            elapsed = time.monotonic() - start
            get_metrics().parse_duration.labels(format=parser.format_name).observe(elapsed)
            for nmi in result.nmis:
                get_metrics().readings_parsed.labels(nmi=nmi).inc(
                    sum(1 for r in result.readings if r.nmi == nmi)
                )

            logger.info(
                "tool.parse_file",
                session_id=session.session_id,
                total_readings=session.total_readings,
                nmis=session.nmis,
                errors=len(result.errors),
                warnings=len(result.warnings),
                elapsed_s=round(elapsed, 3),
            )
            return _ok(session.summary())

        except NotImplementedError as exc:
            return _error(str(exc))
        except Exception as exc:
            logger.exception("tool.parse_file.error", file=file_path, error=str(exc))
            return _error(f"Parse failed: {exc}")
        finally:
            get_metrics().active_sessions.dec()

    def _tool_validate_data(self, inp: dict[str, Any]) -> str:
        session_id = inp["session_id"]
        session = _get_session(session_id)
        if session is None:
            return _error(f"Session not found: '{session_id}'")

        validation = ValidationResult(is_valid=True)
        seen: set[tuple[str, str]] = set()

        for reading in session.readings:
            key = (reading.nmi, reading.timestamp.isoformat())
            if key in seen:
                validation.duplicate_count += 1
                validation.warnings.append(
                    f"Duplicate reading: NMI={reading.nmi} "
                    f"timestamp={reading.timestamp}"
                )
            else:
                seen.add(key)

            if reading.consumption < 0:
                validation.invalid_count += 1
                validation.errors.append(
                    f"Negative consumption: NMI={reading.nmi} "
                    f"timestamp={reading.timestamp} "
                    f"value={reading.consumption}"
                )

            if reading.timestamp > datetime.utcnow():
                validation.warnings.append(
                    f"Future timestamp: NMI={reading.nmi} "
                    f"ts={reading.timestamp}"
                )

        validation.valid_count = (
            len(session.readings)
            - validation.invalid_count
            - validation.duplicate_count
        )
        validation.is_valid = validation.invalid_count == 0
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

    def _tool_generate_sql(self, inp: dict[str, Any]) -> str:
        session_id = inp["session_id"]
        output_path = inp.get("output_path")
        session = _get_session(session_id)
        if session is None:
            return _error(f"Session not found: '{session_id}'")

        if not session.readings:
            return _ok(
                {
                    "statement_count": 0,
                    "sql_preview": "",
                    "output_path": None,
                    "message": "No readings to generate SQL for.",
                }
            )

        sql = self._db.generate_insert_sql(session.readings)
        statement_count = len(session.readings)

        if output_path:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w") as fh:
                fh.write(sql)
            logger.info(
                "tool.generate_sql",
                session_id=session_id,
                output=output_path,
                statements=statement_count,
            )
        else:
            output_path = None

        # Return a preview (first 5 statements) to keep the context window sane
        preview_lines = sql.split("\n")[:5]
        preview = "\n".join(preview_lines)
        if statement_count > 5:
            preview += f"\n... ({statement_count - 5} more statements)"

        return _ok(
            {
                "statement_count": statement_count,
                "sql_preview": preview,
                "output_path": output_path,
            }
        )

    def _tool_write_to_database(self, inp: dict[str, Any]) -> str:
        session_id = inp["session_id"]
        batch_size = int(inp.get("batch_size", 1000))
        session = _get_session(session_id)
        if session is None:
            return _error(f"Session not found: '{session_id}'")

        if not session.readings:
            return _ok(
                {
                    "inserted": 0,
                    "skipped": 0,
                    "failed": 0,
                    "total": 0,
                    "message": "No readings to insert.",
                }
            )

        # Only insert valid readings (skip readings with negative consumption)
        valid_readings = [r for r in session.readings if r.consumption >= 0]
        skipped_invalid = len(session.readings) - len(valid_readings)

        start = time.monotonic()
        result = self._db.bulk_insert(valid_readings, batch_size=batch_size)
        elapsed = time.monotonic() - start

        session.inserted = result["inserted"]
        session.skipped = result["skipped"] + skipped_invalid
        session.failed = result["failed"]

        get_metrics().readings_inserted.inc(result["inserted"])
        get_metrics().readings_skipped.inc(result["skipped"] + skipped_invalid)
        get_metrics().readings_failed.inc(result["failed"])
        get_metrics().db_write_duration.observe(elapsed)

        logger.info(
            "tool.write_to_database",
            session_id=session_id,
            elapsed_s=round(elapsed, 3),
            **result,
        )
        return _ok({**result, "skipped_invalid": skipped_invalid, "elapsed_s": round(elapsed, 3)})

    def _tool_send_notification(self, inp: dict[str, Any]) -> str:
        level = inp["level"]
        message = inp["message"]
        context = inp.get("context")
        self._notifier.notify(level, message, context)
        return _ok({"dispatched": True, "level": level, "message": message})

    def _tool_get_processing_stats(self, inp: dict[str, Any]) -> str:
        session_id = inp["session_id"]
        session = _get_session(session_id)
        if session is None:
            return _error(f"Session not found: '{session_id}'")
        return _ok(session.summary())

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_session_by_path(file_path: str) -> ParseSession | None:
        for s in _sessions.values():
            if s.file_path == file_path:
                return s
        return None


# ---------------------------------------------------------------------------
# JSON response helpers
# ---------------------------------------------------------------------------

def _ok(data: dict[str, Any]) -> str:
    return json.dumps({"status": "ok", **data}, default=_json_default)


def _error(message: str) -> str:
    return json.dumps({"status": "error", "error": message})


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serialisable: {type(obj)}")


def _human_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes //= 1024
    return f"{size_bytes:.1f} PB"
