"""
Unit tests for data models.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from src.models.meter_reading import MeterReading, NEM12Block, ParseSession, QualityMethod


class TestMeterReading:
    def test_valid_reading(self):
        r = MeterReading(
            nmi="NEM1201009",
            timestamp=datetime(2005, 3, 1, 0, 30),
            consumption=Decimal("1.234"),
        )
        assert r.nmi == "NEM1201009"
        assert r.consumption == Decimal("1.234")
        assert len(r.id) == 36  # UUID

    def test_nmi_too_long_raises(self):
        with pytest.raises(ValueError, match="NMI exceeds"):
            MeterReading(
                nmi="X" * 11,
                timestamp=datetime(2005, 3, 1),
                consumption=Decimal("1.0"),
            )

    def test_negative_consumption_raises(self):
        with pytest.raises(ValueError, match="negative"):
            MeterReading(
                nmi="NEM0000001",
                timestamp=datetime(2005, 3, 1),
                consumption=Decimal("-0.001"),
            )

    def test_zero_consumption_is_valid(self):
        r = MeterReading(
            nmi="NEM0000001",
            timestamp=datetime(2005, 3, 1),
            consumption=Decimal("0"),
        )
        assert r.consumption == Decimal("0")

    def test_to_dict(self):
        r = MeterReading(
            nmi="NEM1201009",
            timestamp=datetime(2005, 3, 1, 0, 30),
            consumption=Decimal("1.234"),
            quality_method=QualityMethod.ACTUAL,
        )
        d = r.to_dict()
        assert d["nmi"] == "NEM1201009"
        assert d["quality_method"] == "A"

    def test_to_sql_tuple(self):
        r = MeterReading(
            nmi="NEM1201009",
            timestamp=datetime(2005, 3, 1, 0, 30),
            consumption=Decimal("1.234"),
        )
        tpl = r.to_sql_tuple()
        assert tpl[1] == "NEM1201009"
        assert tpl[2] == datetime(2005, 3, 1, 0, 30)

    def test_unique_ids_per_instance(self):
        r1 = MeterReading(
            nmi="NEM0000001",
            timestamp=datetime(2005, 3, 1),
            consumption=Decimal("1"),
        )
        r2 = MeterReading(
            nmi="NEM0000001",
            timestamp=datetime(2005, 3, 1),
            consumption=Decimal("1"),
        )
        assert r1.id != r2.id


class TestNEM12Block:
    def test_intervals_per_day_30min(self):
        block = NEM12Block(nmi="NEM0000001", nmi_suffix="E1", interval_length=30)
        assert block.intervals_per_day == 48

    def test_intervals_per_day_15min(self):
        block = NEM12Block(nmi="NEM0000001", nmi_suffix="E1", interval_length=15)
        assert block.intervals_per_day == 96

    def test_intervals_per_day_60min(self):
        block = NEM12Block(nmi="NEM0000001", nmi_suffix="E1", interval_length=60)
        assert block.intervals_per_day == 24


class TestQualityMethod:
    def test_known_values(self):
        assert QualityMethod("A") == QualityMethod.ACTUAL
        assert QualityMethod("S") == QualityMethod.SUBSTITUTED

    def test_from_str_known(self):
        assert QualityMethod.from_str("A") == QualityMethod.ACTUAL
        assert QualityMethod.from_str("F") == QualityMethod.FINAL_SUBSTITUTED

    def test_from_str_unknown_returns_none(self):
        assert QualityMethod.from_str("Z") is None
        assert QualityMethod.from_str("") is None


class TestParseSession:
    def test_summary_includes_session_id(self):
        s = ParseSession()
        summary = s.summary()
        assert "session_id" in summary
        assert summary["error_count"] == 0

    def test_errors_capped_at_10_in_summary(self):
        s = ParseSession()
        s.errors = [f"error {i}" for i in range(20)]
        summary = s.summary()
        assert len(summary["errors"]) == 10
