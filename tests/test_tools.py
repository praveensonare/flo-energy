"""
Unit tests for agent tools (ToolRegistry).

Database calls are mocked so these tests run without a real PostgreSQL instance.
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.agent.tools import ToolRegistry, _sessions
from src.models.meter_reading import MeterReading, ParseSession


@pytest.fixture(autouse=True)
def clear_sessions():
    """Isolate session state between tests."""
    _sessions.clear()
    yield
    _sessions.clear()


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.bulk_insert.return_value = {"inserted": 10, "skipped": 2, "failed": 0, "total": 12}
    db.generate_insert_sql.return_value = "INSERT INTO meter_readings VALUES ('x');"
    return db


@pytest.fixture
def registry(mock_db):
    return ToolRegistry(db_handler=mock_db)


# ---------------------------------------------------------------------------
# read_file tool
# ---------------------------------------------------------------------------

class TestReadFileTool:
    def test_returns_error_for_missing_file(self, registry):
        result = json.loads(registry.execute("read_file", {"file_path": "/nonexistent/file.csv"}))
        assert result["status"] == "error"
        assert "not found" in result["error"]

    def test_returns_session_id_for_valid_file(self, nem12_file, registry):
        result = json.loads(registry.execute("read_file", {"file_path": nem12_file}))
        assert result["status"] == "ok"
        assert "session_id" in result
        assert result["format"] == "NEM12"

    def test_file_size_populated(self, nem12_file, registry):
        result = json.loads(registry.execute("read_file", {"file_path": nem12_file}))
        assert result["file_size_bytes"] > 0

    def test_returns_error_for_directory(self, tmp_path, registry):
        result = json.loads(registry.execute("read_file", {"file_path": str(tmp_path)}))
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# parse_file tool
# ---------------------------------------------------------------------------

class TestParseFileTool:
    def test_parse_returns_total_readings(self, nem12_file, registry):
        result = json.loads(registry.execute("parse_file", {"file_path": nem12_file, "format": "NEM12"}))
        assert result["status"] == "ok"
        assert result["total_readings"] > 0

    def test_parse_returns_nmis(self, nem12_file, registry):
        result = json.loads(registry.execute("parse_file", {"file_path": nem12_file, "format": "NEM12"}))
        assert "NEM1201009" in result["nmis"]

    def test_parse_unknown_format_raises(self, empty_file, registry):
        result = json.loads(registry.execute("parse_file", {"file_path": empty_file, "format": "NEM12"}))
        # empty file has no NEM12 header → parser factory will raise
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# validate_data tool
# ---------------------------------------------------------------------------

class TestValidateDataTool:
    def _seed_session_with_readings(self, readings):
        session = ParseSession()
        session.readings = readings
        _sessions[session.session_id] = session
        return session.session_id

    def test_valid_data_passes(self, registry):
        readings = [
            MeterReading(nmi="NEM0000001", timestamp=datetime(2005, 3, 1, i, 0), consumption=Decimal("1.0"))
            for i in range(1, 5)
        ]
        sid = self._seed_session_with_readings(readings)
        result = json.loads(registry.execute("validate_data", {"session_id": sid}))
        assert result["status"] == "ok"
        assert result["is_valid"] is True

    def test_duplicate_detection(self, registry):
        ts = datetime(2005, 3, 1, 0, 30)
        readings = [
            MeterReading(nmi="NEM0000001", timestamp=ts, consumption=Decimal("1.0")),
            MeterReading(nmi="NEM0000001", timestamp=ts, consumption=Decimal("2.0")),
        ]
        sid = self._seed_session_with_readings(readings)
        result = json.loads(registry.execute("validate_data", {"session_id": sid}))
        assert result["duplicate_count"] == 1

    def test_session_not_found_returns_error(self, registry):
        result = json.loads(registry.execute("validate_data", {"session_id": "nonexistent"}))
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# generate_sql tool
# ---------------------------------------------------------------------------

class TestGenerateSqlTool:
    def test_generates_sql_for_valid_session(self, nem12_file, registry, mock_db):
        # Parse first to populate session
        parse_result = json.loads(registry.execute("parse_file", {"file_path": nem12_file, "format": "NEM12"}))
        sid = parse_result["session_id"]

        result = json.loads(registry.execute("generate_sql", {"session_id": sid}))
        assert result["status"] == "ok"
        assert result["statement_count"] > 0

    def test_generates_sql_to_file(self, nem12_file, registry, tmp_path, mock_db):
        parse_result = json.loads(registry.execute("parse_file", {"file_path": nem12_file, "format": "NEM12"}))
        sid = parse_result["session_id"]
        out = str(tmp_path / "out.sql")

        result = json.loads(registry.execute("generate_sql", {"session_id": sid, "output_path": out}))
        assert result["output_path"] == out
        import os
        assert os.path.exists(out)

    def test_empty_session_returns_zero(self, registry):
        session = ParseSession()
        session.readings = []
        _sessions[session.session_id] = session
        result = json.loads(registry.execute("generate_sql", {"session_id": session.session_id}))
        assert result["statement_count"] == 0


# ---------------------------------------------------------------------------
# write_to_database tool
# ---------------------------------------------------------------------------

class TestWriteToDatabaseTool:
    def test_calls_bulk_insert(self, nem12_file, registry, mock_db):
        parse_result = json.loads(registry.execute("parse_file", {"file_path": nem12_file, "format": "NEM12"}))
        sid = parse_result["session_id"]

        result = json.loads(registry.execute("write_to_database", {"session_id": sid}))
        assert result["status"] == "ok"
        mock_db.bulk_insert.assert_called_once()

    def test_respects_batch_size(self, nem12_file, registry, mock_db):
        parse_result = json.loads(registry.execute("parse_file", {"file_path": nem12_file, "format": "NEM12"}))
        sid = parse_result["session_id"]

        registry.execute("write_to_database", {"session_id": sid, "batch_size": 50})
        _, kwargs = mock_db.bulk_insert.call_args
        assert kwargs.get("batch_size") == 50 or mock_db.bulk_insert.call_args[0][1] == 50

    def test_empty_session_returns_zero(self, registry):
        session = ParseSession()
        session.readings = []
        _sessions[session.session_id] = session
        result = json.loads(registry.execute("write_to_database", {"session_id": session.session_id}))
        assert result["inserted"] == 0


# ---------------------------------------------------------------------------
# send_notification tool
# ---------------------------------------------------------------------------

class TestSendNotificationTool:
    def test_notification_dispatched(self, registry):
        result = json.loads(
            registry.execute(
                "send_notification",
                {"level": "info", "message": "Test notification"},
            )
        )
        assert result["status"] == "ok"
        assert result["dispatched"] is True


# ---------------------------------------------------------------------------
# get_processing_stats tool
# ---------------------------------------------------------------------------

class TestGetProcessingStatsTool:
    def test_returns_session_summary(self, registry):
        session = ParseSession()
        session.total_readings = 42
        _sessions[session.session_id] = session
        result = json.loads(
            registry.execute("get_processing_stats", {"session_id": session.session_id})
        )
        assert result["total_readings"] == 42

    def test_unknown_session_returns_error(self, registry):
        result = json.loads(
            registry.execute("get_processing_stats", {"session_id": "fake-id"})
        )
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# Unknown tool
# ---------------------------------------------------------------------------

def test_unknown_tool_returns_error(registry):
    result = json.loads(registry.execute("nonexistent_tool", {}))
    assert result["status"] == "error"
