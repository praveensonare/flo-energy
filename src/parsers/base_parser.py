"""
Abstract base parser defining the interface all format parsers must implement.

Design rationale:
- Polymorphism: new formats (NEM13, MSATS, etc.) only need to implement this
  interface; the agent tools and database handler remain unchanged.
- Generator-based: parse() is a generator so callers control memory budget.
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Generator

from src.models.meter_reading import MeterReading


# Holds the aggregated output of a single parse() call, including all readings,
# errors, warnings, NMIs encountered, and the number of lines processed.
@dataclass
class ParserResult:
    """Outcome of a parser.parse() call."""
    readings: list[MeterReading] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    nmis: list[str] = field(default_factory=list)
    lines_processed: int = 0


# Abstract base class that every file-format parser must subclass.
# Defines the required interface (detect, stream_readings, parse) and shared helpers.
class BaseParser(ABC):
    """
    Abstract base for all meter data file parsers.

    Subclasses must implement:
        - format_name: str property identifying the format
        - detect(file_path): classmethod returning True when the file
          matches this format
        - stream_readings(file_path): generator yielding MeterReading objects
        - parse(file_path): returns a ParserResult
    """

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Human-readable format identifier, e.g. 'NEM12'."""

    # Inspects the file header to decide whether this parser can handle the file.
    # Reads only the first line so detection cost is O(1) with respect to file size.
    @classmethod
    @abstractmethod
    def detect(cls, file_path: str) -> bool:
        """
        Inspect the file header and return True if this parser handles it.

        Implementations should read only the first line so detection is O(1)
        in file size.
        """

    # Yields MeterReading objects one at a time from the file without loading it all into memory.
    # Callers use this when processing files too large to buffer entirely in RAM.
    @abstractmethod
    def stream_readings(self, file_path: str) -> Generator[MeterReading, None, None]:
        """
        Yield individual MeterReading objects from the file.

        This generator approach keeps memory usage constant regardless of
        file size – critical for multi-GB NEM12 files.
        """

    # Parses the entire file and returns a single ParserResult with all readings collected.
    # Prefer stream_readings() for very large files to avoid buffering all data in RAM.
    @abstractmethod
    def parse(self, file_path: str) -> ParserResult:
        """
        Parse the entire file and return a ParserResult.

        Implementations should call stream_readings() internally and collect
        results.  For very large files callers may prefer stream_readings()
        directly to avoid buffering all readings in RAM.
        """

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    # Returns the size of the given file in bytes using os.stat.
    # Returns 0 rather than raising if the file cannot be accessed.
    @staticmethod
    def _file_size(file_path: str) -> int:
        """Return file size in bytes, or 0 if the file cannot be stat'd."""
        try:
            return os.path.getsize(file_path)
        except OSError:
            return 0

    # Opens a text file using UTF-8 encoding with BOM stripping for safe line iteration.
    # Used by subclasses that need raw line access rather than CSV parsing.
    @staticmethod
    def _open_file(file_path: str):
        """
        Open a text file with UTF-8 encoding and BOM handling.

        Returns a file object suitable for line-by-line iteration.
        """
        return open(file_path, "r", encoding="utf-8-sig", newline="")
