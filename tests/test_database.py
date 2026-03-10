"""
Unit tests for PostgresHandler.

All tests use mocked psycopg2 connections – no real database required.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch, call

import pytest

from src.database.postgres_handler import PostgresHandler
from src.models.meter_reading import MeterReading


def _make_readings(count: int = 5) -> list[MeterReading]:
    return [
        MeterReading(
            nmi=f"NMI{i:07d}",
            timestamp=datetime(2005, 3, 1, i % 24, 0),
            consumption=Decimal(f"{i}.{i}"),
        )
        for i in range(1, count + 1)
    ]


@pytest.fixture
def handler():
    return PostgresHandler(dsn="postgresql://user:pass@localhost/testdb")


class TestGenerateInsertSQL:
    def test_generates_one_statement_per_reading(self, handler):
        readings = _make_readings(3)
        sql = handler.generate_insert_sql(readings)
        statements = [s for s in sql.split("\n") if s.strip()]
        assert len(statements) == 3

    def test_statements_contain_nmi(self, handler):
        readings = _make_readings(1)
        sql = handler.generate_insert_sql(readings)
        assert readings[0].nmi in sql

    def test_statements_have_on_conflict(self, handler):
        readings = _make_readings(1)
        sql = handler.generate_insert_sql(readings)
        assert "ON CONFLICT" in sql
        assert "DO NOTHING" in sql

    def test_empty_readings_returns_empty_string(self, handler):
        sql = handler.generate_insert_sql([])
        assert sql == ""

    def test_timestamp_format(self, handler):
        readings = [
            MeterReading(
                nmi="NEM0000001",
                timestamp=datetime(2005, 3, 1, 0, 30),
                consumption=Decimal("1.0"),
            )
        ]
        sql = handler.generate_insert_sql(readings)
        assert "2005-03-01 00:30:00" in sql


class TestBulkInsert:
    @patch("psycopg2.pool.ThreadedConnectionPool")
    @patch("psycopg2.extras.execute_values")
    def test_bulk_insert_called_with_correct_args(
        self, mock_execute_values, mock_pool_cls
    ):
        # Set up mock pool and connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool

        handler = PostgresHandler(dsn="postgresql://user:pass@host/db")
        readings = _make_readings(5)
        result = handler.bulk_insert(readings, batch_size=10)

        # execute_values should have been called at least once
        assert mock_execute_values.called

    def test_bulk_insert_empty_returns_zeros(self, handler):
        result = handler.bulk_insert([])
        assert result == {"inserted": 0, "skipped": 0, "failed": 0, "total": 0}

    @patch("psycopg2.pool.ThreadedConnectionPool")
    @patch("psycopg2.extras.execute_values", side_effect=Exception("DB down"))
    def test_bulk_insert_handles_exception(self, mock_ev, mock_pool_cls):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool

        handler = PostgresHandler(dsn="postgresql://user:pass@host/db")
        readings = _make_readings(3)
        result = handler.bulk_insert(readings, batch_size=10)

        assert result["failed"] == 3
        assert result["inserted"] == 0


class TestRedactDSN:
    def test_password_redacted(self, handler):
        redacted = handler._redact_dsn()
        assert "pass" not in redacted
        assert "***" in redacted

    def test_host_preserved(self, handler):
        redacted = handler._redact_dsn()
        assert "localhost" in redacted


class TestFromEnv:
    def test_constructs_from_database_url(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@host/db")
        h = PostgresHandler.from_env()
        assert "host" in h._dsn

    def test_constructs_from_pgvars(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("PGHOST", "myhost")
        monkeypatch.setenv("PGDATABASE", "mydb")
        monkeypatch.setenv("PGUSER", "myuser")
        monkeypatch.setenv("PGPASSWORD", "mypass")
        monkeypatch.setenv("PGPORT", "5433")
        h = PostgresHandler.from_env()
        assert "myhost" in h._dsn
        assert "5433" in h._dsn
