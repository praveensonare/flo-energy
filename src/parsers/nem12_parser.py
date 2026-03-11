"""
NEM12 file parser.

NEM12 (National Electricity Market) is the Australian standard for
interval meter data exchange.  Key record types:

  100  File header   – version, date/time, from/to participant
  200  NMI header    – NMI, suffix, register, interval length (minutes)
  300  Interval data – date + N interval values + quality flag
  400  Event data    – quality override for sub-ranges of a 300 record
  500  B2B details   – retailer/LNSP pair information
  900  End of file   – must be the last record

Parser contract:
  - Reads the file line-by-line; memory usage is O(batch_size), not O(file).
  - Timestamps follow the NEM12 "interval ending" convention:
      interval i (1-indexed) on date D = D + i * interval_length_minutes
  - Malformed lines are recorded as warnings/errors and skipped so the rest
    of the file is still processed.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Generator

import structlog

from src.models.meter_reading import MeterReading, NEM12Block, QualityMethod
from src.parsers.base_parser import BaseParser, ParserResult

logger = structlog.get_logger(__name__)

_RECORD_100 = "100"
_RECORD_200 = "200"
_RECORD_300 = "300"
_RECORD_400 = "400"
_RECORD_500 = "500"
_RECORD_900 = "900"

# Expected position of the NEM12 version string in the 100 record
_NEM12_VERSION = "NEM12"

# Maximum number of intervals per day for any valid interval length
# interval_length must divide 1440 (minutes per day) evenly
_VALID_INTERVAL_LENGTHS = {1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60}


# Converts a raw YYYYMMDD string into a Python datetime object.
# Returns None instead of raising if the string cannot be parsed.
def _parse_interval_date(raw: str) -> datetime | None:
    """Parse YYYYMMDD string into a datetime; return None on failure."""
    try:
        return datetime.strptime(raw.strip(), "%Y%m%d")
    except ValueError:
        return None


# Opens a CSV file and yields each non-blank row as a list of string fields.
# Uses csv.reader so quoted fields containing commas are handled correctly.
def _iter_csv_lines(file_path: str) -> Generator[list[str], None, None]:
    """
    Yield tokenised CSV rows, one per logical line.

    Uses csv.reader for correct handling of quoted fields (some participants
    include commas inside description fields).
    """
    with open(file_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if row:  # skip blank lines
                yield row


# Parser for NEM12 interval meter data files, processing records 100–900.
# Thread-safe: no mutable instance state is shared between parse() calls.
class NEM12Parser(BaseParser):
    """
    Parser for NEM12 interval meter data files.

    Thread-safe: maintains no instance-level mutable state between calls.
    Each parse() / stream_readings() call is independent.
    """

    @property
    def format_name(self) -> str:
        return "NEM12"

    # Checks whether the file's first non-blank line starts with '100,NEM12'.
    # Reads only that line so detection is O(1) in file size.
    @classmethod
    def detect(cls, file_path: str) -> bool:
        """
        Returns True when the first non-blank line starts with '100,NEM12'.
        Only reads the first line – O(1) in file size.
        """
        try:
            with open(file_path, "r", encoding="utf-8-sig") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        parts = line.split(",")
                        return (
                            len(parts) >= 2
                            and parts[0].strip() == _RECORD_100
                            and parts[1].strip() == _NEM12_VERSION
                        )
            return False
        except OSError:
            return False

    # ------------------------------------------------------------------
    # Core parsing
    # ------------------------------------------------------------------

    # Drives a state machine over the NEM12 file and yields one MeterReading per interval.
    # Memory usage stays constant regardless of file size since readings are not accumulated.
    def stream_readings(self, file_path: str) -> Generator[MeterReading, None, None]:
        """
        Yield MeterReading objects one at a time.

        State machine:
          idle → header_seen → nmi_block (repeats) → eof
        """
        current_block: NEM12Block | None = None
        seen_100 = False
        line_num = 0

        for row in _iter_csv_lines(file_path):
            line_num += 1
            record_type = row[0].strip()

            if record_type == _RECORD_100:
                if len(row) < 2 or row[1].strip() != _NEM12_VERSION:
                    logger.warning(
                        "nem12.invalid_header",
                        line=line_num,
                        row=row,
                    )
                    continue
                seen_100 = True

            elif record_type == _RECORD_200:
                if not seen_100:
                    logger.warning("nem12.200_before_100", line=line_num)
                current_block = self._parse_200(row, line_num)

            elif record_type == _RECORD_300:
                if current_block is None:
                    logger.warning("nem12.300_before_200", line=line_num)
                    continue
                yield from self._parse_300(row, current_block, line_num)

            elif record_type == _RECORD_400:
                # Quality overrides – complex to apply; log and continue
                logger.debug("nem12.400_skipped", line=line_num)

            elif record_type == _RECORD_500:
                logger.debug("nem12.500_skipped", line=line_num)

            elif record_type == _RECORD_900:
                logger.debug("nem12.eof", line=line_num)
                return

            else:
                logger.warning(
                    "nem12.unknown_record_type",
                    record_type=record_type,
                    line=line_num,
                )

    # Reads the entire NEM12 file and returns a ParserResult containing all readings and diagnostics.
    # Malformed lines are recorded as errors or warnings rather than aborting the parse.
    def parse(self, file_path: str) -> ParserResult:
        """
        Parse the complete file and return a ParserResult.

        For memory-sensitive callers, use stream_readings() to process
        readings in a pipeline without buffering all of them.
        """
        result = ParserResult()
        nmi_set: set[str] = set()
        current_block: NEM12Block | None = None
        seen_100 = False
        line_num = 0

        for row in _iter_csv_lines(file_path):
            line_num += 1
            result.lines_processed += 1
            record_type = row[0].strip()

            try:
                if record_type == _RECORD_100:
                    if len(row) < 2 or row[1].strip() != _NEM12_VERSION:
                        result.errors.append(
                            f"Line {line_num}: Invalid 100 record header"
                        )
                        continue
                    seen_100 = True

                elif record_type == _RECORD_200:
                    if not seen_100:
                        result.warnings.append(
                            f"Line {line_num}: 200 record before 100 record"
                        )
                    current_block = self._parse_200(row, line_num)
                    if current_block:
                        nmi_set.add(current_block.nmi)

                elif record_type == _RECORD_300:
                    if current_block is None:
                        result.warnings.append(
                            f"Line {line_num}: 300 record without preceding 200"
                        )
                        continue
                    for reading in self._parse_300(row, current_block, line_num):
                        result.readings.append(reading)

                elif record_type == _RECORD_900:
                    break

                # 400, 500 are intentionally not processed in this version

            except Exception as exc:
                result.errors.append(f"Line {line_num}: Unexpected error – {exc}")
                logger.exception(
                    "nem12.parse_error", line=line_num, error=str(exc)
                )

        result.nmis = sorted(nmi_set)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    # Parses a NEM12 200-record row into an NEM12Block holding NMI and interval metadata.
    # Returns None and logs a warning if the row is malformed or contains an invalid NMI.
    def _parse_200(self, row: list[str], line_num: int) -> NEM12Block | None:
        """
        Parse a 200 record and return an NEM12Block.

        200,<NMI>,<NMISuffix>,<RegisterID>,<NMISuffix>,<MDMDataStreamIdentifier>,
            <MeterSerialNumber>,<UOM>,<IntervalLength>,<NextScheduledReadDate>
        Index:  0     1          2            3           4                        5
                      6               7          8           9
        """
        try:
            if len(row) < 9:
                logger.warning(
                    "nem12.200_short", line=line_num, fields=len(row)
                )
                return None

            nmi = row[1].strip()
            if not nmi or len(nmi) > 10:
                logger.warning(
                    "nem12.200_invalid_nmi", line=line_num, nmi=nmi
                )
                return None

            interval_length_raw = row[8].strip()
            try:
                interval_length = int(interval_length_raw)
            except ValueError:
                logger.warning(
                    "nem12.200_invalid_interval_length",
                    line=line_num,
                    raw=interval_length_raw,
                )
                return None

            if interval_length not in _VALID_INTERVAL_LENGTHS:
                logger.warning(
                    "nem12.200_unsupported_interval_length",
                    line=line_num,
                    interval_length=interval_length,
                )
                # Still try to proceed; unusual but not fatal

            uom = row[7].strip() if len(row) > 7 else "kWh"
            nmi_suffix = row[2].strip() if len(row) > 2 else ""

            return NEM12Block(
                nmi=nmi,
                nmi_suffix=nmi_suffix,
                interval_length=interval_length,
                uom=uom,
            )
        except Exception as exc:
            logger.exception(
                "nem12.200_parse_failed", line=line_num, error=str(exc)
            )
            return None

    # Parses a NEM12 300-record row and yields one MeterReading for each interval value.
    # Invalid consumption values are skipped with a warning; valid ones use interval-ending timestamps.
    def _parse_300(
        self,
        row: list[str],
        block: NEM12Block,
        line_num: int,
    ) -> Generator[MeterReading, None, None]:
        """
        Parse a 300 record and yield one MeterReading per interval.

        300,<IntervalDate>,<Value1>,<Value2>,...,<ValueN>,
            <QualityMethod>,<ReasonCode>,<ReasonDescription>,
            <UpdateDatetime>,<MSATSLoadDatetime>

        Index 0: '300'
        Index 1: IntervalDate (YYYYMMDD)
        Index 2..2+N-1: N interval consumption values
        Index 2+N: QualityMethod
        """
        if len(row) < 3:
            logger.warning("nem12.300_short", line=line_num, fields=len(row))
            return

        interval_date = _parse_interval_date(row[1])
        if interval_date is None:
            logger.warning(
                "nem12.300_invalid_date", line=line_num, raw=row[1]
            )
            return

        intervals_per_day = 1440 // block.interval_length

        # Values occupy indices 2 .. 2+N-1 (N = intervals_per_day)
        value_end_idx = 2 + intervals_per_day
        raw_values = row[2:value_end_idx]

        # Quality method is the field immediately after the last value
        quality_raw = row[value_end_idx].strip() if len(row) > value_end_idx else ""
        quality = QualityMethod.from_str(quality_raw) if quality_raw else None

        for i, raw_val in enumerate(raw_values):
            raw_val = raw_val.strip()
            if not raw_val:
                continue  # sparse records may omit trailing zeros

            try:
                consumption = Decimal(raw_val)
            except InvalidOperation:
                logger.warning(
                    "nem12.300_invalid_consumption",
                    line=line_num,
                    interval=i + 1,
                    raw=raw_val,
                )
                continue

            # NEM12 "interval ending" convention:
            #   interval 1 ends at 00:30, interval 48 ends at 24:00 (= next day 00:00)
            interval_offset = timedelta(minutes=(i + 1) * block.interval_length)
            timestamp = interval_date + interval_offset

            try:
                yield MeterReading(
                    nmi=block.nmi,
                    timestamp=timestamp,
                    consumption=consumption,
                    quality_method=quality,
                )
            except ValueError as exc:
                logger.warning(
                    "nem12.300_reading_invalid",
                    line=line_num,
                    interval=i + 1,
                    error=str(exc),
                )
