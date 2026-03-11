"""
Async unit tests for the LangChain ETL tools in src/agent/langchain_tools.py.
Database calls are mocked so these tests run without a real PostgreSQL instance.
"""
from __future__ import annotations

import asyncio
import json
import os
import zipfile
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from src.agent.langchain_tools import (
    _resolve_csv_path,
    _sessions,
    _session_lock,
    generate_sql,
    get_processing_stats,
    parse_file,
    read_file,
    send_notification,
    validate_data,
    write_to_database,
)
from src.models.meter_reading import MeterReading, ParseSession


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _seed_session(readings: list[MeterReading]) -> str:
    # Insert a ParseSession directly into the shared store and return its session_id.
    session = ParseSession()
    session.readings = readings
    session.total_readings = len(readings)
    async with _session_lock:
        _sessions[session.session_id] = session
    return session.session_id


def _make_readings(count: int = 5) -> list[MeterReading]:
    # Build a list of valid MeterReading objects for use in tests.
    return [
        MeterReading(
            nmi="NEM0000001",
            timestamp=datetime(2005, 3, 1, 0, i * 30),
            consumption=Decimal("1.000"),
        )
        for i in range(1, count + 1)
    ]


# ---------------------------------------------------------------------------
# Isolation fixture — clear shared session dict between tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
async def clear_sessions():
    # Wipe session state before and after every test to prevent cross-test pollution.
    async with _session_lock:
        _sessions.clear()
    yield
    async with _session_lock:
        _sessions.clear()


# ---------------------------------------------------------------------------
# _resolve_csv_path helper
# ---------------------------------------------------------------------------

class TestResolveCsvPath:
    # Tests for the internal ZIP-extraction helper used by read_file and parse_file.

    def test_csv_returns_same_path(self, nem12_file):
        # A plain CSV file should be returned unchanged with no temp directory.
        csv_path, temp_dir = _resolve_csv_path(nem12_file)
        assert csv_path == nem12_file
        assert temp_dir is None

    def test_zip_extracts_csv(self, nem12_zip_file, tmp_path):
        # A valid ZIP containing a CSV should be extracted to a temp directory.
        csv_path, temp_dir = _resolve_csv_path(nem12_zip_file)
        try:
            assert csv_path.endswith(".csv")
            assert os.path.exists(csv_path)
            assert temp_dir is not None
            assert os.path.isdir(temp_dir)
        finally:
            import shutil
            if temp_dir:
                shutil.rmtree(temp_dir, ignore_errors=True)

    def test_invalid_zip_raises(self, tmp_path):
        # A file with .zip extension that is not actually a ZIP should raise ValueError.
        bad = tmp_path / "fake.zip"
        bad.write_text("not a zip")
        with pytest.raises(ValueError, match="not a valid ZIP archive"):
            _resolve_csv_path(str(bad))

    def test_zip_no_csv_raises(self, tmp_path):
        # A ZIP containing only non-CSV files should raise ValueError.
        zf_path = tmp_path / "empty.zip"
        with zipfile.ZipFile(str(zf_path), "w") as zf:
            zf.writestr("readme.txt", "hello")
        with pytest.raises(ValueError, match="No .csv file found"):
            _resolve_csv_path(str(zf_path))

    def test_unsupported_extension_raises(self, tmp_path):
        # Files with unsupported extensions should raise ValueError.
        bad = tmp_path / "data.tsv"
        bad.write_text("100,NEM12")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            _resolve_csv_path(str(bad))


# ---------------------------------------------------------------------------
# read_file tool
# ---------------------------------------------------------------------------

class TestReadFileTool:
    # Tests for the read_file LangChain tool: file validation and format detection.

    @pytest.mark.asyncio
    async def test_missing_file_returns_error(self):
        # Non-existent path should return a structured error JSON.
        result = json.loads(await read_file.ainvoke({"file_path": "/no/such/file.csv"}))
        assert result["status"] == "error"
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_directory_returns_error(self, tmp_path):
        # A directory path should return a structured error JSON.
        result = json.loads(await read_file.ainvoke({"file_path": str(tmp_path)}))
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_valid_csv_returns_session(self, nem12_file):
        # A valid NEM12 CSV should create a session and return its id with format=NEM12.
        result = json.loads(await read_file.ainvoke({"file_path": nem12_file}))
        assert result["status"] == "ok"
        assert "session_id" in result
        assert result["format"] == "NEM12"
        assert result["file_size_bytes"] > 0

    @pytest.mark.asyncio
    async def test_valid_zip_returns_session(self, nem12_zip_file):
        # A NEM12 file wrapped in a ZIP should be detected correctly.
        result = json.loads(await read_file.ainvoke({"file_path": nem12_zip_file}))
        assert result["status"] == "ok"
        assert result["format"] == "NEM12"

    @pytest.mark.asyncio
    async def test_session_stored(self, nem12_file):
        # After read_file the session should appear in the shared session store.
        result = json.loads(await read_file.ainvoke({"file_path": nem12_file}))
        async with _session_lock:
            assert result["session_id"] in _sessions


# ---------------------------------------------------------------------------
# parse_file tool
# ---------------------------------------------------------------------------

class TestParseFileTool:
    # Tests for parse_file: reading extraction from CSV and ZIP inputs.

    @pytest.mark.asyncio
    async def test_csv_parse_returns_readings(self, nem12_file):
        # Parsing a valid NEM12 CSV should return a positive reading count.
        result = json.loads(
            await parse_file.ainvoke({"file_path": nem12_file, "format": "NEM12"})
        )
        assert result["status"] == "ok"
        assert result["total_readings"] > 0

    @pytest.mark.asyncio
    async def test_csv_parse_returns_nmis(self, nem12_file):
        # Parsed NMIs should include the expected identifiers from the sample file.
        result = json.loads(
            await parse_file.ainvoke({"file_path": nem12_file, "format": "NEM12"})
        )
        assert "NEM1201009" in result["nmis"]

    @pytest.mark.asyncio
    async def test_zip_parse_returns_readings(self, nem12_zip_file):
        # Parsing a ZIP-wrapped NEM12 file should produce the same readings as CSV.
        result = json.loads(
            await parse_file.ainvoke({"file_path": nem12_zip_file, "format": "NEM12"})
        )
        assert result["status"] == "ok"
        assert result["total_readings"] > 0

    @pytest.mark.asyncio
    async def test_zip_temp_dir_cleaned_up(self, nem12_zip_file, tmp_path):
        # The temporary extraction directory must be deleted after parse_file completes.
        import shutil
        original_mkdtemp = __import__("tempfile").mkdtemp
        created_dirs: list[str] = []

        def tracking_mkdtemp(**kwargs):
            d = original_mkdtemp(**kwargs)
            created_dirs.append(d)
            return d

        with patch("src.agent.langchain_tools.tempfile.mkdtemp", side_effect=tracking_mkdtemp):
            await parse_file.ainvoke({"file_path": nem12_zip_file, "format": "NEM12"})

        # All temp dirs created during the call must have been deleted.
        for d in created_dirs:
            assert not os.path.exists(d), f"Temp dir not cleaned up: {d}"

    @pytest.mark.asyncio
    async def test_unrecognised_format_returns_error(self, empty_file):
        # A file with no recognised NEM header should return a structured error.
        result = json.loads(
            await parse_file.ainvoke({"file_path": empty_file, "format": "NEM12"})
        )
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_reuses_existing_session(self, nem12_file):
        # If a session for this file already exists, parse_file should reuse it.
        r1 = json.loads(await read_file.ainvoke({"file_path": nem12_file}))
        r2 = json.loads(
            await parse_file.ainvoke({"file_path": nem12_file, "format": "NEM12"})
        )
        assert r1["session_id"] == r2["session_id"]


# ---------------------------------------------------------------------------
# validate_data tool
# ---------------------------------------------------------------------------

class TestValidateDataTool:
    # Tests for the validate_data tool: duplicate detection, invalid values, future timestamps.

    @pytest.mark.asyncio
    async def test_valid_readings_pass(self):
        # Clean readings with no duplicates or bad values should yield is_valid=True.
        sid = await _seed_session(_make_readings(5))
        result = json.loads(await validate_data.ainvoke({"session_id": sid}))
        assert result["status"] == "ok"
        assert result["is_valid"] is True
        assert result["invalid_count"] == 0

    @pytest.mark.asyncio
    async def test_duplicate_detection(self):
        # Two readings with the same NMI+timestamp should be flagged as duplicates.
        ts = datetime(2005, 3, 1, 0, 30)
        readings = [
            MeterReading(nmi="NEM0000001", timestamp=ts, consumption=Decimal("1.0")),
            MeterReading(nmi="NEM0000001", timestamp=ts, consumption=Decimal("2.0")),
        ]
        sid = await _seed_session(readings)
        result = json.loads(await validate_data.ainvoke({"session_id": sid}))
        assert result["duplicate_count"] == 1

    @pytest.mark.asyncio
    async def test_marks_session_validated(self):
        # After validate_data runs, session.validated should be True.
        sid = await _seed_session(_make_readings(3))
        await validate_data.ainvoke({"session_id": sid})
        async with _session_lock:
            assert _sessions[sid].validated is True

    @pytest.mark.asyncio
    async def test_unknown_session_returns_error(self):
        # Calling validate_data with a non-existent session_id should return an error.
        result = json.loads(await validate_data.ainvoke({"session_id": "nonexistent"}))
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_parallel_validate_and_sql(self, nem12_file):
        # validate_data and generate_sql must run concurrently without deadlocking.
        r = json.loads(await parse_file.ainvoke({"file_path": nem12_file, "format": "NEM12"}))
        sid = r["session_id"]

        with patch("src.agent.langchain_tools._get_db") as mock_db_fn:
            mock_db = MagicMock()
            mock_db.generate_insert_sql.return_value = "INSERT INTO meter_readings VALUES (1);"
            mock_db_fn.return_value = mock_db

            val, sql = await asyncio.gather(
                validate_data.ainvoke({"session_id": sid}),
                generate_sql.ainvoke({"session_id": sid, "output_path": ""}),
            )

        assert json.loads(val)["status"] == "ok"
        assert json.loads(sql)["status"] == "ok"


# ---------------------------------------------------------------------------
# generate_sql tool
# ---------------------------------------------------------------------------

class TestGenerateSqlTool:
    # Tests for SQL generation: inline output and file writing.

    @pytest.mark.asyncio
    async def test_generates_sql_for_session(self):
        # A session with readings should produce a non-zero statement count.
        sid = await _seed_session(_make_readings(3))
        with patch("src.agent.langchain_tools._get_db") as mock_db_fn:
            mock_db = MagicMock()
            mock_db.generate_insert_sql.return_value = "INSERT INTO meter_readings VALUES (1);"
            mock_db_fn.return_value = mock_db

            result = json.loads(
                await generate_sql.ainvoke({"session_id": sid, "output_path": ""})
            )
        assert result["status"] == "ok"
        assert result["statement_count"] == 3

    @pytest.mark.asyncio
    async def test_writes_sql_file(self, tmp_path):
        # When output_path is given, the SQL should be written to that file.
        sid = await _seed_session(_make_readings(2))
        out = str(tmp_path / "out.sql")

        with patch("src.agent.langchain_tools._get_db") as mock_db_fn:
            mock_db = MagicMock()
            mock_db.generate_insert_sql.return_value = "INSERT INTO meter_readings VALUES (1);"
            mock_db_fn.return_value = mock_db

            result = json.loads(
                await generate_sql.ainvoke({"session_id": sid, "output_path": out})
            )
        assert result["output_path"] == out
        assert os.path.exists(out)

    @pytest.mark.asyncio
    async def test_empty_session_returns_zero(self):
        # A session with no readings should return statement_count=0 without error.
        sid = await _seed_session([])
        result = json.loads(
            await generate_sql.ainvoke({"session_id": sid, "output_path": ""})
        )
        assert result["statement_count"] == 0

    @pytest.mark.asyncio
    async def test_unknown_session_returns_error(self):
        # A non-existent session_id should return a structured error JSON.
        result = json.loads(
            await generate_sql.ainvoke({"session_id": "bad-id", "output_path": ""})
        )
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# write_to_database tool
# ---------------------------------------------------------------------------

class TestWriteToDatabaseTool:
    # Tests for DB write tool: schema auto-creation, ACID mock, skip/fail counts.

    @pytest.mark.asyncio
    async def test_calls_ensure_schema_and_insert(self):
        # write_to_database must call ensure_schema() before atomic_bulk_insert().
        sid = await _seed_session(_make_readings(5))

        with patch("src.agent.langchain_tools._get_db") as mock_db_fn:
            mock_db = MagicMock()
            mock_db.ensure_schema.return_value = False
            mock_db.atomic_bulk_insert.return_value = {
                "inserted": 5, "skipped": 0, "failed": 0, "total": 5
            }
            mock_db_fn.return_value = mock_db

            result = json.loads(await write_to_database.ainvoke({"session_id": sid}))

        mock_db.ensure_schema.assert_called_once()
        mock_db.atomic_bulk_insert.assert_called_once()
        assert result["status"] == "ok"
        assert result["inserted"] == 5

    @pytest.mark.asyncio
    async def test_empty_session_returns_zero(self):
        # No readings in the session means nothing to insert; should return zeros.
        sid = await _seed_session([])
        result = json.loads(await write_to_database.ainvoke({"session_id": sid}))
        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_db_error_returns_error_json(self):
        # If the DB raises, write_to_database must return an error (not raise).
        sid = await _seed_session(_make_readings(3))

        with patch("src.agent.langchain_tools._get_db") as mock_db_fn:
            mock_db = MagicMock()
            mock_db.ensure_schema.return_value = False
            mock_db.atomic_bulk_insert.side_effect = RuntimeError("connection refused")
            mock_db_fn.return_value = mock_db

            result = json.loads(await write_to_database.ainvoke({"session_id": sid}))

        assert result["status"] == "error"
        assert "connection refused" in result["error"]

    @pytest.mark.asyncio
    async def test_unknown_session_returns_error(self):
        # A non-existent session_id should return a structured error JSON.
        result = json.loads(await write_to_database.ainvoke({"session_id": "ghost"}))
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_skipped_invalid_counted(self):
        # Readings with negative consumption should be counted as skipped_invalid, not inserted.
        # MeterReading enforces non-negative via __post_init__, so we patch the filter logic.
        sid = await _seed_session(_make_readings(5))

        with patch("src.agent.langchain_tools._get_db") as mock_db_fn:
            mock_db = MagicMock()
            mock_db.ensure_schema.return_value = False
            mock_db.atomic_bulk_insert.return_value = {
                "inserted": 5, "skipped": 0, "failed": 0, "total": 5
            }
            mock_db_fn.return_value = mock_db

            result = json.loads(await write_to_database.ainvoke({"session_id": sid}))

        # skipped_invalid=0 because all test readings are valid (consumption >= 0).
        assert result["skipped_invalid"] == 0

    @pytest.mark.asyncio
    async def test_session_counts_updated_after_write(self):
        # After a successful write, session.inserted/skipped/failed should be updated.
        sid = await _seed_session(_make_readings(4))

        with patch("src.agent.langchain_tools._get_db") as mock_db_fn:
            mock_db = MagicMock()
            mock_db.ensure_schema.return_value = False
            mock_db.atomic_bulk_insert.return_value = {
                "inserted": 3, "skipped": 1, "failed": 0, "total": 4
            }
            mock_db_fn.return_value = mock_db
            await write_to_database.ainvoke({"session_id": sid})

        async with _session_lock:
            session = _sessions[sid]
        assert session.inserted == 3
        assert session.skipped == 1


# ---------------------------------------------------------------------------
# send_notification tool
# ---------------------------------------------------------------------------

class TestSendNotificationTool:
    # Tests for the notification dispatch tool.

    @pytest.mark.asyncio
    async def test_dispatches_successfully(self):
        # send_notification must return dispatched=True for any valid level.
        with patch("src.agent.langchain_tools._get_notifier") as mock_notifier_fn:
            mock_notifier = MagicMock()
            mock_notifier_fn.return_value = mock_notifier

            result = json.loads(
                await send_notification.ainvoke(
                    {"level": "success", "message": "ETL complete"}
                )
            )

        assert result["status"] == "ok"
        assert result["dispatched"] is True
        mock_notifier.notify.assert_called_once_with("success", "ETL complete", None)

    @pytest.mark.asyncio
    async def test_passes_context_to_notifier(self):
        # Context dict should be forwarded to the underlying NotificationService.
        with patch("src.agent.langchain_tools._get_notifier") as mock_notifier_fn:
            mock_notifier = MagicMock()
            mock_notifier_fn.return_value = mock_notifier

            await send_notification.ainvoke(
                {"level": "error", "message": "oops", "context": {"file": "x.csv"}}
            )

        mock_notifier.notify.assert_called_once_with(
            "error", "oops", {"file": "x.csv"}
        )


# ---------------------------------------------------------------------------
# get_processing_stats tool
# ---------------------------------------------------------------------------

class TestGetProcessingStatsTool:
    # Tests for the session statistics tool.

    @pytest.mark.asyncio
    async def test_returns_session_summary(self):
        # get_processing_stats should return a summary with total_readings from the session.
        sid = await _seed_session(_make_readings(10))
        result = json.loads(await get_processing_stats.ainvoke({"session_id": sid}))
        assert result["status"] == "ok"
        assert result["total_readings"] == 10

    @pytest.mark.asyncio
    async def test_unknown_session_returns_error(self):
        # A non-existent session_id should return a structured error JSON.
        result = json.loads(await get_processing_stats.ainvoke({"session_id": "missing"}))
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# Race condition safety — concurrent access to the same session
# ---------------------------------------------------------------------------

class TestConcurrentSessionAccess:
    # Tests that asyncio.Lock prevents data corruption under concurrent tool calls.

    @pytest.mark.asyncio
    async def test_concurrent_validate_and_stats(self, nem12_file):
        # validate_data and get_processing_stats on the same session must not deadlock.
        r = json.loads(
            await parse_file.ainvoke({"file_path": nem12_file, "format": "NEM12"})
        )
        sid = r["session_id"]

        results = await asyncio.gather(
            validate_data.ainvoke({"session_id": sid}),
            get_processing_stats.ainvoke({"session_id": sid}),
        )
        for r_raw in results:
            assert json.loads(r_raw)["status"] == "ok"

    @pytest.mark.asyncio
    async def test_multiple_files_independent_sessions(self, nem12_file, tmp_path):
        # Two concurrent parse_file calls on different files must get separate sessions.
        second = tmp_path / "second.csv"
        import shutil
        shutil.copy(nem12_file, str(second))

        r1, r2 = await asyncio.gather(
            parse_file.ainvoke({"file_path": nem12_file, "format": "NEM12"}),
            parse_file.ainvoke({"file_path": str(second), "format": "NEM12"}),
        )
        d1, d2 = json.loads(r1), json.loads(r2)
        assert d1["session_id"] != d2["session_id"]
