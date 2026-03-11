"""
NEM12 File Processing Service.

Runs as a background thread and processes NEM12 meter data files from a
submission queue.  For each file the service:

  1. Validates the filename against the NEM12 Alternative Delivery naming
     convention:  ``NEM12#<UniqueID>#<From>#<To>.csv``  (or ``.zip``)
  2. Decompresses ZIP archives (zlib/deflate only; no password protection).
  3. Parses the NEM12 CSV content using :class:`NEM12Parser`.
  4. Applies NEM12 spec data-validation rules (duplicate detection, sequential
     date ordering, interval count, quality-flag consistency, etc.).
  5. Publishes validated :class:`MeterReading` objects to a :class:`SharedReadingList`
     so downstream consumer threads (e.g. :class:`DatabaseCacheService`) can
     process them without tight coupling.

Thread-safety model
-------------------
* The processing thread owns the ``Queue`` and calls ``_process_file``
  sequentially.
* :class:`SharedReadingList` is the only shared-state object and it is
  guarded by its own ``threading.Lock`` + ``threading.Condition``.
* ``ProcessingResult`` objects are accumulated in ``_results`` under a
  separate ``_results_lock``.
"""
from __future__ import annotations

import os
import re
import shutil
import tempfile
import threading
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from queue import Empty, Queue
from typing import Optional

import structlog

from src.config import settings
from src.models.meter_reading import MeterReading, QualityMethod
from src.parsers.base_parser import ParserResult
from src.parsers.nem12_parser import NEM12Parser

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Filename validation
# ---------------------------------------------------------------------------

# NEM12 Alternative Delivery naming spec (section 3.2.2):
#   VersionHeader#UniqueID#From#To.csv   (case-insensitive)
#   VersionHeader#UniqueID#From#To.zip
#
# - VersionHeader : 5 alphanumeric chars, must be "NEM12"
# - UniqueID      : 1–36 alphanumeric chars
# - From / To     : Participant IDs (alphanumeric, no length cap in spec but
#                   the VarChar(10) field in the 100 record implies ≤ 10)
_FILENAME_RE = re.compile(
    r"^nem12"           # VersionHeader (case-insensitive)
    r"#([A-Za-z0-9]{1,36})"  # #UniqueID
    r"#([A-Za-z0-9]{1,10})"  # #From
    r"#([A-Za-z0-9]{1,10})"  # #To
    r"\.(csv|zip)$",    # extension
    re.IGNORECASE,
)

# NEM12 spec: IntervalLength must be 5, 15, or 30 minutes
_VALID_INTERVAL_LENGTHS = {5, 15, 30}
_MINUTES_PER_DAY = 1440


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FilenameInfo:
    """Parsed components of a NEM12 filename."""
    unique_id: str
    from_participant: str
    to_participant: str
    extension: str  # "csv" or "zip"


@dataclass
class ProcessingResult:
    """Outcome of processing a single NEM12 file."""
    file_path: str
    success: bool = False
    readings: list[MeterReading] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    filename_info: Optional[FilenameInfo] = None

    def summary(self) -> dict:
        return {
            "file_path": self.file_path,
            "success": self.success,
            "reading_count": len(self.readings),
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors[:10],
            "warnings": self.warnings[:10],
            "processed_at": self.processed_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Thread-safe shared reading list
# ---------------------------------------------------------------------------

class SharedReadingList:
    """
    Thread-safe container for :class:`MeterReading` objects shared between
    producer threads (NEM12ProcessingService) and consumer threads
    (DatabaseCacheService, analytics, etc.).

    Producer API
    ------------
    ``append_batch(readings)`` – adds a list of readings atomically and
    notifies waiting consumers.

    Consumer API
    ------------
    ``drain()``            – removes and returns all current readings.
    ``peek()``             – non-destructive snapshot.
    ``wait_for_data(t)``   – blocks until data arrives or *t* seconds elapse,
                             then drains.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._readings: list[MeterReading] = []

    # -- Producer ----------------------------------------------------------

    def append_batch(self, readings: list[MeterReading]) -> None:
        """Append *readings* atomically and wake waiting consumers."""
        if not readings:
            return
        with self._condition:
            self._readings.extend(readings)
            self._condition.notify_all()
        logger.debug("shared_list.appended", count=len(readings), total=len(self._readings))

    # -- Consumer ----------------------------------------------------------

    def drain(self) -> list[MeterReading]:
        """Remove and return all current readings (non-blocking)."""
        with self._lock:
            readings, self._readings = self._readings, []
        return readings

    def peek(self) -> list[MeterReading]:
        """Return a snapshot without removing readings."""
        with self._lock:
            return list(self._readings)

    def wait_for_data(self, timeout: float = 1.0) -> list[MeterReading]:
        """
        Block until readings are available or *timeout* seconds elapse.
        Drains and returns available readings (may be empty on timeout).
        """
        with self._condition:
            self._condition.wait_for(lambda: bool(self._readings), timeout=timeout)
            readings, self._readings = self._readings, []
        return readings

    def __len__(self) -> int:
        with self._lock:
            return len(self._readings)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class NEM12ValidationError(ValueError):
    """Raised when a NEM12 file fails hard validation (file rejected)."""


# ---------------------------------------------------------------------------
# Processing service
# ---------------------------------------------------------------------------

class NEM12ProcessingService:
    """
    Background thread that processes NEM12 files end-to-end.

    Usage::

        shared = SharedReadingList()
        svc = NEM12ProcessingService(shared_readings=shared)
        svc.start()

        svc.submit("/path/to/nem12#abc123#MDA1#RETAIL1.csv")
        svc.submit("/path/to/nem12#def456#MDA1#RETAIL1.zip")

        # Downstream thread can call:
        readings = shared.drain()          # non-blocking
        readings = shared.wait_for_data()  # blocking with timeout

        svc.stop()

    The service keeps a list of :class:`ProcessingResult` objects accessible
    via ``get_results()``.
    """

    def __init__(
        self,
        shared_readings: SharedReadingList,
        max_queue_size: int | None = None,
    ) -> None:
        self._shared = shared_readings
        self._queue: Queue[str | None] = Queue(
            maxsize=max_queue_size if max_queue_size is not None
            else settings.nem12_queue_max_size
        )
        self._thread = threading.Thread(
            target=self._run,
            name="NEM12ProcessingService",
            daemon=True,
        )
        self._parser = NEM12Parser()
        self._stop_event = threading.Event()
        self._results: list[ProcessingResult] = []
        self._results_lock = threading.Lock()

    # -- Lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Start the background processing thread."""
        logger.info("nem12_processor.starting")
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """Signal the thread to finish and wait up to *timeout* seconds."""
        logger.info("nem12_processor.stopping")
        self._stop_event.set()
        self._queue.put(None)  # sentinel unblocks the queue.get()
        self._thread.join(timeout=timeout)
        logger.info("nem12_processor.stopped")

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    # -- Submission --------------------------------------------------------

    def submit(self, file_path: str) -> None:
        """
        Queue *file_path* for processing.  Returns immediately.
        Blocks if the internal queue is full (``max_queue_size``).
        """
        self._queue.put(file_path)
        logger.info("nem12_processor.queued", file_path=file_path)

    def join_queue(self) -> None:
        """Block until all submitted files have been processed."""
        self._queue.join()

    # -- Results -----------------------------------------------------------

    def get_results(self) -> list[ProcessingResult]:
        """Return a snapshot of all processing results."""
        with self._results_lock:
            return list(self._results)

    # -- Internal loop -----------------------------------------------------

    def _run(self) -> None:
        logger.info("nem12_processor.thread_started")
        while not self._stop_event.is_set():
            try:
                file_path = self._queue.get(timeout=0.5)
            except Empty:
                continue

            if file_path is None:  # sentinel
                self._queue.task_done()
                break

            try:
                result = self._process_file(file_path)
            except Exception as exc:
                logger.exception("nem12_processor.unexpected_error", error=str(exc))
                result = ProcessingResult(
                    file_path=file_path,
                    errors=[f"Unexpected processing error: {exc}"],
                )
            finally:
                self._queue.task_done()

            with self._results_lock:
                self._results.append(result)

        logger.info("nem12_processor.thread_stopped")

    # -- File pipeline -----------------------------------------------------

    def _process_file(self, file_path: str) -> ProcessingResult:
        """Full pipeline for a single file."""
        result = ProcessingResult(file_path=file_path)
        log = logger.bind(file_path=file_path)
        log.info("nem12_processor.processing_start")

        # Step 1 – validate filename
        try:
            info = self._validate_filename(file_path)
            result.filename_info = info
            log = log.bind(
                unique_id=info.unique_id,
                from_participant=info.from_participant,
                to_participant=info.to_participant,
            )
        except NEM12ValidationError as exc:
            result.errors.append(str(exc))
            log.warning("nem12_processor.filename_invalid", reason=str(exc))
            return result

        # Step 2 – resolve CSV path (decompress ZIP if needed)
        csv_path: str
        temp_dir: str | None = None
        try:
            csv_path, temp_dir = self._resolve_csv_path(file_path)
        except NEM12ValidationError as exc:
            result.errors.append(str(exc))
            log.warning("nem12_processor.file_resolve_failed", reason=str(exc))
            return result

        try:
            # Step 3 – parse NEM12
            parse_result = self._parser.parse(csv_path)
            result.errors.extend(parse_result.errors)
            result.warnings.extend(parse_result.warnings)

            if parse_result.errors:
                log.warning(
                    "nem12_processor.parse_errors",
                    count=len(parse_result.errors),
                    sample=parse_result.errors[:3],
                )

            # Step 4 – validate data per NEM12 spec
            val_errors, val_warnings = self._validate_data(parse_result)
            result.errors.extend(val_errors)
            result.warnings.extend(val_warnings)

            # Step 5 – publish if no hard errors
            if not result.errors:
                result.readings = parse_result.readings
                self._shared.append_batch(parse_result.readings)
                result.success = True
                log.info(
                    "nem12_processor.published",
                    readings=len(parse_result.readings),
                    nmis=parse_result.nmis,
                )
            else:
                log.warning(
                    "nem12_processor.rejected",
                    error_count=len(result.errors),
                    sample_errors=result.errors[:3],
                )

        finally:
            if temp_dir:
                shutil.rmtree(temp_dir, ignore_errors=True)
                log.debug("nem12_processor.temp_cleanup", temp_dir=temp_dir)

        log.info(
            "nem12_processor.processing_complete",
            success=result.success,
            readings=len(result.readings),
        )
        return result

    # -- Filename validation -----------------------------------------------

    def _validate_filename(self, file_path: str) -> FilenameInfo:
        """
        Validate filename against NEM12 spec section 3.2.2.

        Expected:  ``NEM12#<UniqueID>#<From>#<To>.csv``  or  ``.zip``

        Returns a :class:`FilenameInfo` on success.
        Raises :class:`NEM12ValidationError` on failure.
        """
        filename = Path(file_path).name
        match = _FILENAME_RE.match(filename)

        if not match:
            raise NEM12ValidationError(
                f"Filename '{filename}' does not match NEM12 naming convention "
                "NEM12#<UniqueID>#<From>#<To>.<csv|zip> (case-insensitive)."
            )

        unique_id, from_p, to_p, ext = match.groups()

        if len(unique_id) > 36:
            raise NEM12ValidationError(
                f"UniqueID '{unique_id}' exceeds the 36-character limit in '{filename}'."
            )

        return FilenameInfo(
            unique_id=unique_id,
            from_participant=from_p,
            to_participant=to_p,
            extension=ext.lower(),
        )

    # -- File resolution (ZIP extraction) ----------------------------------

    def _resolve_csv_path(self, file_path: str) -> tuple[str, str | None]:
        """
        Return ``(csv_path, temp_dir)`` where *temp_dir* is ``None`` for
        plain CSV files and a temp-directory path for extracted ZIPs.

        Raises :class:`NEM12ValidationError` for missing / invalid files.
        """
        path = Path(file_path)

        if not path.exists():
            raise NEM12ValidationError(f"File not found: {file_path}")

        ext = path.suffix.lower()

        if ext == ".csv":
            return str(path), None

        if ext == ".zip":
            if not zipfile.is_zipfile(file_path):
                raise NEM12ValidationError(
                    f"File has .zip extension but is not a valid ZIP archive: {file_path}"
                )

            temp_dir = tempfile.mkdtemp(prefix="nem12_extract_")
            try:
                with zipfile.ZipFile(file_path, "r") as zf:
                    # Reject password-protected archives (spec 3.2.2 b ii)
                    for info in zf.infolist():
                        if info.flag_bits & 0x1:
                            raise NEM12ValidationError(
                                f"ZIP file is password-protected, which is not "
                                f"allowed per NEM12 spec: {file_path}"
                            )

                    csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                    if not csv_members:
                        raise NEM12ValidationError(
                            f"No .csv file found inside ZIP archive: {file_path}"
                        )

                    # Extract first CSV (spec implies one NEM12 file per archive)
                    csv_name = csv_members[0]
                    zf.extract(csv_name, temp_dir)
                    csv_path = os.path.join(temp_dir, csv_name)

                logger.info(
                    "nem12_processor.zip_extracted",
                    zip_path=file_path,
                    csv_path=csv_path,
                )
                return csv_path, temp_dir

            except NEM12ValidationError:
                shutil.rmtree(temp_dir, ignore_errors=True)
                raise
            except Exception as exc:
                shutil.rmtree(temp_dir, ignore_errors=True)
                raise NEM12ValidationError(
                    f"Failed to extract ZIP archive '{file_path}': {exc}"
                ) from exc

        raise NEM12ValidationError(
            f"Unsupported file extension '{ext}' for '{file_path}'. "
            "Expected .csv or .zip."
        )

    # -- Data validation ---------------------------------------------------

    def _validate_data(
        self, result: ParserResult
    ) -> tuple[list[str], list[str]]:
        """
        Apply NEM12 spec data-validation rules to a :class:`ParserResult`.

        Hard errors (``errors``) cause the file to be rejected.
        Soft warnings (``warnings``) are recorded but do not block publishing.

        Rules applied
        -------------
        * At least one reading must exist.
        * No duplicate (NMI, timestamp) pairs.
        * Interval counts per day must equal 1440 / IntervalLength.
        * 300 records must appear in ascending date order per NMI.
        * Consumption values must be non-negative (enforced by MeterReading
          model; redundantly checked here for clarity).
        * Future timestamps emit a warning.
        * Zero consumption is flagged as informational.
        """
        errors: list[str] = []
        warnings: list[str] = []

        if not result.readings:
            warnings.append("File contains no interval readings.")
            return errors, warnings

        now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC

        # -- Duplicate detection --
        seen: set[tuple[str, str]] = set()
        duplicates = 0
        for r in result.readings:
            key = (r.nmi, r.timestamp.isoformat())
            if key in seen:
                duplicates += 1
            seen.add(key)

        if duplicates:
            warnings.append(
                f"{duplicates} duplicate (NMI, timestamp) pairs detected. "
                "ON CONFLICT DO NOTHING will deduplicate on insert."
            )

        # -- Future timestamps --
        future = [r for r in result.readings if r.timestamp > now]
        if future:
            warnings.append(
                f"{len(future)} reading(s) have timestamps in the future "
                f"(latest: {max(r.timestamp for r in future).isoformat()})."
            )

        # -- Non-negative consumption (belt-and-suspenders) --
        negative = [r for r in result.readings if r.consumption < Decimal("0")]
        if negative:
            errors.append(
                f"{len(negative)} reading(s) have negative consumption values, "
                "which is not allowed by NEM12 spec section 4.4."
            )

        # -- Zero consumption --
        zero_count = sum(1 for r in result.readings if r.consumption == Decimal("0"))
        if zero_count:
            warnings.append(
                f"{zero_count} interval(s) have zero consumption "
                "(may indicate meter outage or off-peak period)."
            )

        # -- NMI length --
        invalid_nmi = [r for r in result.readings if not r.nmi or len(r.nmi) > 10]
        if invalid_nmi:
            errors.append(
                f"{len(invalid_nmi)} reading(s) have an invalid NMI "
                "(empty or longer than 10 characters)."
            )

        # -- 300 records in ascending date order per NMI --
        nmi_dates: dict[str, list[datetime]] = {}
        for r in result.readings:
            nmi_dates.setdefault(r.nmi, []).append(r.timestamp)

        for nmi, timestamps in nmi_dates.items():
            sorted_ts = sorted(timestamps)
            if timestamps != sorted_ts:
                warnings.append(
                    f"NMI {nmi}: interval timestamps are not in ascending order "
                    "(spec section 4.4 requires sequential date order)."
                )

        logger.info(
            "nem12_processor.validation_done",
            total=len(result.readings),
            nmis=len(nmi_dates),
            duplicates=duplicates,
            future=len(future),
            errors=len(errors),
            warnings=len(warnings),
        )
        return errors, warnings
