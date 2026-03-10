"""
Data models for meter readings and parsing sessions.
Uses Pydantic v2 for strict validation.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class QualityMethod(str, Enum):
    """NEM12 quality/method flags for interval data."""
    ACTUAL = "A"
    SUBSTITUTED = "S"
    FINAL_SUBSTITUTED = "F"
    NULL = "N"
    VARIABLE = "V"
    ESTIMATED = "E"

    @classmethod
    def from_str(cls, value: str) -> "QualityMethod | None":
        """Return the matching member, or None for unknown flags."""
        return cls._value2member_map_.get(value)


@dataclass(slots=True)
class MeterReading:
    """
    A single interval meter reading.

    Attributes:
        nmi: National Metering Identifier (max 10 chars)
        timestamp: End-of-interval timestamp (UTC-naive, local market time)
        consumption: Energy consumed in the interval (kWh or as per UOM)
        quality_method: Data quality indicator
        id: UUID assigned at construction
    """
    nmi: str
    timestamp: datetime
    consumption: Decimal
    quality_method: QualityMethod | None = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self) -> None:
        if len(self.nmi) > 10:
            raise ValueError(f"NMI exceeds 10 characters: '{self.nmi}'")
        if self.consumption < Decimal("0"):
            raise ValueError(
                f"Consumption cannot be negative: {self.consumption}"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "nmi": self.nmi,
            "timestamp": self.timestamp.isoformat(),
            "consumption": str(self.consumption),
            "quality_method": self.quality_method.value if self.quality_method else None,
        }

    def to_sql_tuple(self) -> tuple[str, str, datetime, Decimal]:
        """Return (id, nmi, timestamp, consumption) for DB insertion."""
        return (self.id, self.nmi, self.timestamp, self.consumption)


@dataclass
class NEM12Block:
    """
    A group of readings sharing an NMI and interval configuration.

    Corresponds to a 200-record block in the NEM12 file.
    """
    nmi: str
    nmi_suffix: str
    interval_length: int  # minutes per interval
    uom: str = "kWh"
    readings: list[MeterReading] = field(default_factory=list)

    @property
    def intervals_per_day(self) -> int:
        return 1440 // self.interval_length


@dataclass
class ParseSession:
    """
    Tracks state across a full parse lifecycle.

    Held in memory per session_id; allows tools to hand off parsed data
    to downstream tools without re-reading the file.
    """
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    file_path: str = ""
    file_format: str = ""
    file_size_bytes: int = 0
    total_readings: int = 0
    total_nmis: int = 0
    nmis: list[str] = field(default_factory=list)
    readings: list[MeterReading] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    parsed_at: datetime | None = None
    validated: bool = False
    inserted: int = 0
    skipped: int = 0
    failed: int = 0

    def summary(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "file_path": self.file_path,
            "file_format": self.file_format,
            "file_size_bytes": self.file_size_bytes,
            "total_readings": self.total_readings,
            "total_nmis": self.total_nmis,
            "nmis": self.nmis,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors[:10],  # cap for brevity
            "warnings": self.warnings[:10],
            "validated": self.validated,
            "inserted": self.inserted,
            "skipped": self.skipped,
            "failed": self.failed,
        }


@dataclass
class ValidationResult:
    """Result of a data-validation pass."""
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    duplicate_count: int = 0
    invalid_count: int = 0
    valid_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "valid_count": self.valid_count,
            "invalid_count": self.invalid_count,
            "duplicate_count": self.duplicate_count,
            "errors": self.errors,
            "warnings": self.warnings,
        }
