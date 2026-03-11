"""
Observability layer: structured logging + Prometheus metrics.

Architecture:
  - structlog provides JSON-structured log output with context binding
  - prometheus_client exposes /metrics endpoint for scraping (or push gateway)
  - MetricsCollector runs on an independent daemon thread; the main
    processing pipeline posts updates to it without blocking
  - The module is a singleton (get_metrics()) so all layers share counters
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

import structlog
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    start_http_server,
)

# ------------------------------------------------------------------
# Structlog configuration
# ------------------------------------------------------------------

# Configures structlog with ISO timestamps, log level, and either JSON or console rendering.
# Must be called exactly once at application startup before any loggers are used.
def configure_logging(log_level: str = "INFO", json_output: bool = True) -> None:
    """
    Configure structlog with timestamped, level-aware processors.

    Call once at application startup.
    """
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_output:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level.upper(), logging.INFO),
    )


# ------------------------------------------------------------------
# Prometheus metrics definitions
# ------------------------------------------------------------------

# Private singleton that instantiates and holds all Prometheus counters, histograms, and gauges.
# All metric objects are created once at first access via get_metrics() to avoid duplicate registration.
class _Metrics:
    """Singleton holder for all Prometheus metric objects."""

    def __init__(self) -> None:
        self.files_processed = Counter(
            "meter_files_processed_total",
            "Total NEM files processed",
            ["format", "status"],
        )
        self.readings_parsed = Counter(
            "meter_readings_parsed_total",
            "Total interval readings parsed from files",
            ["nmi"],
        )
        self.readings_inserted = Counter(
            "meter_readings_inserted_total",
            "Total readings successfully inserted into the database",
        )
        self.readings_skipped = Counter(
            "meter_readings_skipped_total",
            "Total readings skipped due to duplicate key",
        )
        self.readings_failed = Counter(
            "meter_readings_failed_total",
            "Total readings that failed to insert",
        )
        self.parse_duration = Histogram(
            "meter_parse_duration_seconds",
            "Time spent parsing a file",
            ["format"],
            buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120, 300],
        )
        self.db_write_duration = Histogram(
            "meter_db_write_duration_seconds",
            "Time spent writing a batch to the database",
            buckets=[0.01, 0.05, 0.1, 0.5, 1, 5, 10],
        )
        self.active_sessions = Gauge(
            "meter_active_sessions",
            "Number of in-progress parse sessions",
        )
        self.validation_errors = Counter(
            "meter_validation_errors_total",
            "Total validation errors encountered",
        )
        self.notifications_sent = Counter(
            "meter_notifications_sent_total",
            "Total notifications dispatched",
            ["level"],
        )


_singleton: _Metrics | None = None
_lock = threading.Lock()


# Returns the process-wide _Metrics singleton, creating it on first call using a double-checked lock.
# Safe to call from multiple threads; all callers share the same set of Prometheus metric objects.
def get_metrics() -> _Metrics:
    """Return the process-wide Metrics singleton."""
    global _singleton
    if _singleton is None:
        with _lock:
            if _singleton is None:
                _singleton = _Metrics()
    return _singleton


# ------------------------------------------------------------------
# MetricsCollector – background reporting thread
# ------------------------------------------------------------------

# Background daemon thread that periodically snapshots metric counters to the structured log.
# Optionally starts a Prometheus HTTP server on the configured port for scraper-based collection.
class MetricsCollector(threading.Thread):
    """
    Daemon thread that periodically logs aggregate metrics to structlog.

    This provides human-readable progress reporting in the logs without
    requiring a Prometheus scraper to be present.
    """

    def __init__(
        self,
        interval_seconds: float = 10.0,
        prometheus_port: int | None = None,
    ) -> None:
        super().__init__(daemon=True, name="MetricsCollector")
        self._interval = interval_seconds
        self._stop_event = threading.Event()
        self._logger = structlog.get_logger("observability.metrics")
        self._prometheus_port = prometheus_port

        if prometheus_port:
            start_http_server(prometheus_port)
            self._logger.info(
                "prometheus.started", port=prometheus_port
            )

    # Thread entry point: loops every interval_seconds, logging a metrics snapshot to structlog.
    # Exits cleanly when the stop event is set by calling stop().
    def run(self) -> None:
        metrics = get_metrics()
        while not self._stop_event.wait(self._interval):
            self._logger.info(
                "metrics.snapshot",
                readings_inserted=_safe_counter_value(
                    metrics.readings_inserted
                ),
                readings_skipped=_safe_counter_value(
                    metrics.readings_skipped
                ),
                readings_failed=_safe_counter_value(
                    metrics.readings_failed
                ),
                active_sessions=metrics.active_sessions._value.get(),
            )

    # Signals the run loop to exit on its next iteration by setting the stop event.
    # Returns immediately; callers should join() the thread if they need to wait for shutdown.
    def stop(self) -> None:
        self._stop_event.set()


# Reads the internal numeric value of a Prometheus Counter without raising on unexpected state.
# Returns 0.0 if the counter's internal value cannot be retrieved for any reason.
def _safe_counter_value(counter: Counter) -> float:
    """Extract numeric value from a Prometheus counter safely."""
    try:
        return counter._value.get()
    except Exception:
        return 0.0
