"""
NEM13 file parser – stub implementation.

NEM13 is the Australian standard for accumulation (non-interval) meter data.
Key differences from NEM12:

  - Only a single cumulative reading per register per read date (no intervals)
  - Records use Previous/Current read values with a Meter Read Type qualifier
  - No concept of IntervalLength; consumption = current_read - previous_read
    adjusted for multiplier and check digit rules

Record structure:
  100  File header    – version marker 'NEM13'
  250  NMI data block – NMI, register, UOM, meter serial
  350  Register read  – previous/current readings and dates
  550  B2B details
  900  End of file

This stub provides format detection and the interface contract.
Full implementation is tracked as future work (see README § Future Work).
"""
from __future__ import annotations

from typing import Generator

import structlog

from src.models.meter_reading import MeterReading
from src.parsers.base_parser import BaseParser, ParserResult

logger = structlog.get_logger(__name__)

_NEM13_VERSION = "NEM13"


# Stub parser for NEM13 accumulation meter files that implements format detection only.
# parse() and stream_readings() raise NotImplementedError pending a future implementation.
class NEM13Parser(BaseParser):
    """
    Stub parser for NEM13 accumulation meter files.

    Raises NotImplementedError on parse() and stream_readings() – full
    implementation is deferred to a future sprint (see README § Future Work).
    """

    @property
    def format_name(self) -> str:
        return "NEM13"

    # Checks whether the file's first non-blank line starts with '100,NEM13'.
    # Only reads a single line so detection cost is independent of file size.
    @classmethod
    def detect(cls, file_path: str) -> bool:
        """
        Returns True when the first non-blank line starts with '100,NEM13'.
        """
        try:
            with open(file_path, "r", encoding="utf-8-sig") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        parts = line.split(",")
                        return (
                            len(parts) >= 2
                            and parts[0].strip() == "100"
                            and parts[1].strip() == _NEM13_VERSION
                        )
            return False
        except OSError:
            return False

    # Placeholder for the future NEM13 streaming implementation.
    # Always raises NotImplementedError until the full implementation is provided.
    def stream_readings(self, file_path: str) -> Generator[MeterReading, None, None]:
        raise NotImplementedError(
            "NEM13 parsing is not yet implemented. "
            "See README § Future Work for the roadmap."
        )

    # Placeholder for the future NEM13 bulk-parse implementation.
    # Always raises NotImplementedError until the full implementation is provided.
    def parse(self, file_path: str) -> ParserResult:
        raise NotImplementedError(
            "NEM13 parsing is not yet implemented. "
            "See README § Future Work for the roadmap."
        )
