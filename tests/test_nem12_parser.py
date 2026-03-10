"""
Unit tests for the NEM12 parser.

Covers:
  - Format detection
  - Full parse result correctness
  - Timestamp calculation (interval-ending convention)
  - Streaming generator
  - Edge cases: malformed lines, short 200 records, bad consumption values
  - Large-file simulation via in-memory content
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime
from decimal import Decimal

import pytest

from src.parsers.nem12_parser import NEM12Parser
from src.parsers.parser_factory import ParserFactory


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

class TestNEM12Detection:
    def test_detects_valid_nem12(self, nem12_file):
        assert NEM12Parser.detect(nem12_file) is True

    def test_rejects_nem13(self, nem13_file):
        assert NEM12Parser.detect(nem13_file) is False

    def test_rejects_empty_file(self, empty_file):
        assert NEM12Parser.detect(empty_file) is False

    def test_rejects_nonexistent_file(self, tmp_path):
        assert NEM12Parser.detect(str(tmp_path / "ghost.csv")) is False

    def test_factory_returns_nem12_parser(self, nem12_file):
        parser = ParserFactory.get_parser(nem12_file)
        assert parser.format_name == "NEM12"

    def test_factory_raises_for_unknown(self, empty_file):
        with pytest.raises(ValueError, match="Unrecognised"):
            ParserFactory.get_parser(empty_file)


# ---------------------------------------------------------------------------
# Parsing correctness
# ---------------------------------------------------------------------------

class TestNEM12Parse:
    def test_parse_returns_readings(self, nem12_file):
        parser = NEM12Parser()
        result = parser.parse(nem12_file)
        assert len(result.readings) > 0

    def test_parse_correct_nmi_count(self, nem12_file):
        parser = NEM12Parser()
        result = parser.parse(nem12_file)
        assert set(result.nmis) == {"NEM1201009", "NEM1201010"}

    def test_parse_30min_intervals_per_day(self, nem12_file):
        """30-minute intervals = 48 readings per NMI per day."""
        parser = NEM12Parser()
        result = parser.parse(nem12_file)
        nmi_9_readings = [r for r in result.readings if r.nmi == "NEM1201009"]
        # 2 days × 48 intervals = 96
        assert len(nmi_9_readings) == 96

    def test_parse_minimal_file_all_ones(self, nem12_minimal_file):
        """All consumption values should be 1.000."""
        parser = NEM12Parser()
        result = parser.parse(nem12_minimal_file)
        assert len(result.readings) == 48  # 48 intervals for one day
        for r in result.readings:
            assert r.consumption == Decimal("1.000")

    def test_no_errors_on_valid_file(self, nem12_file):
        parser = NEM12Parser()
        result = parser.parse(nem12_file)
        assert result.errors == []

    def test_nmi_max_length(self, nem12_file):
        parser = NEM12Parser()
        result = parser.parse(nem12_file)
        for r in result.readings:
            assert len(r.nmi) <= 10


# ---------------------------------------------------------------------------
# Timestamp calculation
# ---------------------------------------------------------------------------

class TestNEM12Timestamps:
    def test_first_interval_is_half_past_midnight(self, nem12_minimal_file):
        """Interval 1 of 2005-03-01 with 30-min intervals = 2005-03-01 00:30."""
        parser = NEM12Parser()
        result = parser.parse(nem12_minimal_file)

        # Readings are not guaranteed to be sorted, find earliest
        ts_list = sorted(r.timestamp for r in result.readings)
        assert ts_list[0] == datetime(2005, 3, 1, 0, 30)

    def test_last_interval_is_midnight_next_day(self, nem12_minimal_file):
        """Interval 48 of 2005-03-01 with 30-min intervals = 2005-03-02 00:00."""
        parser = NEM12Parser()
        result = parser.parse(nem12_minimal_file)
        ts_list = sorted(r.timestamp for r in result.readings)
        assert ts_list[-1] == datetime(2005, 3, 2, 0, 0)

    def test_timestamps_are_monotonically_increasing(self, nem12_minimal_file):
        parser = NEM12Parser()
        result = parser.parse(nem12_minimal_file)
        ts_list = sorted(r.timestamp for r in result.readings)
        for a, b in zip(ts_list, ts_list[1:]):
            assert b > a

    def test_interval_spacing_is_30_minutes(self, nem12_minimal_file):
        from datetime import timedelta
        parser = NEM12Parser()
        result = parser.parse(nem12_minimal_file)
        ts_list = sorted(r.timestamp for r in result.readings)
        expected_delta = timedelta(minutes=30)
        for a, b in zip(ts_list, ts_list[1:]):
            assert b - a == expected_delta


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

class TestNEM12Streaming:
    def test_stream_yields_same_count_as_parse(self, nem12_file):
        parser = NEM12Parser()
        streamed = list(parser.stream_readings(nem12_file))
        parsed = parser.parse(nem12_file).readings
        assert len(streamed) == len(parsed)

    def test_stream_readings_are_meter_readings(self, nem12_file):
        from src.models.meter_reading import MeterReading
        parser = NEM12Parser()
        for reading in parser.stream_readings(nem12_file):
            assert isinstance(reading, MeterReading)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestNEM12EdgeCases:
    def test_malformed_nmi_skipped(self, tmp_path):
        """NMI longer than 10 characters should be skipped (not crash)."""
        content = (
            "100,NEM12,200506081149,X,Y\n"
            "200,TOOLONGFORNMI,E1,1,E1,N1,001,kWh,30,20050610\n"
            "300,20050301"
            + ",1.0" * 48
            + ",A,,,20050310121004,\n900\n"
        )
        p = tmp_path / "bad_nmi.csv"
        p.write_text(content)
        parser = NEM12Parser()
        result = parser.parse(str(p))
        # No readings because the NMI block was rejected
        assert len(result.readings) == 0

    def test_300_without_200_produces_warning(self, tmp_path):
        content = (
            "100,NEM12,200506081149,X,Y\n"
            "300,20050301" + ",1.0" * 48 + ",A,,,\n"
            "900\n"
        )
        p = tmp_path / "orphan_300.csv"
        p.write_text(content)
        parser = NEM12Parser()
        result = parser.parse(str(p))
        assert any("300" in w for w in result.warnings)

    def test_invalid_consumption_skipped(self, tmp_path):
        """Non-numeric consumption value should be skipped gracefully."""
        vals = ["abc"] + ["1.0"] * 47  # first value is bad
        row = ",".join(vals)
        content = (
            "100,NEM12,200506081149,X,Y\n"
            f"200,NEM1200001,E1,1,E1,N1,001,kWh,30,20050610\n"
            f"300,20050301,{row},A,,,\n"
            "900\n"
        )
        p = tmp_path / "bad_val.csv"
        p.write_text(content)
        parser = NEM12Parser()
        result = parser.parse(str(p))
        # 47 good values; 1 skipped
        assert len(result.readings) == 47

    def test_large_file_streaming_memory(self, tmp_path):
        """
        Simulate a 'large' file with 1000 days of data for one NMI.
        Using streaming ensures constant memory – verify count is correct.
        """
        lines = ["100,NEM12,200506081149,X,Y"]
        lines.append("200,LRGNMI0001,E1,1,E1,N1,001,kWh,30,20050610")
        for day in range(1000):
            from datetime import date, timedelta
            dt = date(2005, 1, 1) + timedelta(days=day)
            date_str = dt.strftime("%Y%m%d")
            vals = ",".join(["1.0"] * 48)
            lines.append(f"300,{date_str},{vals},A,,,")
        lines.append("900")

        p = tmp_path / "large_nem12.csv"
        p.write_text("\n".join(lines))

        parser = NEM12Parser()
        count = sum(1 for _ in parser.stream_readings(str(p)))
        assert count == 1000 * 48
