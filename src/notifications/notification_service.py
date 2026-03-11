"""
Notification service for processing events (errors, completions, warnings).

Design:
  - NotificationHandler is an abstract base; add channels (email, Slack,
    PagerDuty, SNS, etc.) by subclassing without touching the service.
  - NotificationService fans out to all registered handlers.
  - Log handler is always registered as a safety net.
  - Handlers run synchronously; for high-throughput scenarios replace with
    an async queue (e.g. asyncio.Queue or a message broker).
"""
from __future__ import annotations

import json
import threading
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

import structlog

from src.observability.metrics import get_metrics

logger = structlog.get_logger(__name__)


# Enum of notification severity levels used to route and filter messages across handlers.
# Values match the lowercase strings used in log output and webhook payloads.
class NotificationLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


# Abstract base class for all notification delivery channels (log, console, webhook, etc.).
# Subclasses must implement send() and must never let it raise an exception.
class NotificationHandler(ABC):
    """Abstract base for notification delivery channels."""

    # Delivers a single notification at the given level with optional context metadata.
    # Implementations must swallow all exceptions to avoid disrupting the pipeline.
    @abstractmethod
    def send(
        self,
        level: NotificationLevel,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Deliver a notification. Implementations must not raise."""


# Notification handler that writes every notification to the structlog structured logger.
# Acts as a guaranteed fallback; always registered by NotificationService on construction.
class LogNotificationHandler(NotificationHandler):
    """Writes all notifications to the structured log."""

    # Dispatches the notification to the appropriate structlog level function.
    # SUCCESS is mapped to info since structlog has no dedicated success level.
    def send(
        self,
        level: NotificationLevel,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        log_fn = {
            NotificationLevel.INFO: logger.info,
            NotificationLevel.WARNING: logger.warning,
            NotificationLevel.ERROR: logger.error,
            NotificationLevel.SUCCESS: logger.info,
        }.get(level, logger.info)

        log_fn(
            "notification",
            level=level.value,
            message=message,
            **(context or {}),
        )


# Notification handler that prints messages to stdout with a symbol and level prefix.
# Intended for interactive CLI use where human-readable output is more useful than JSON logs.
class ConsoleNotificationHandler(NotificationHandler):
    """Writes notifications to stdout in a human-friendly format."""

    # Formats the notification with a Unicode symbol, uppercased level label, and optional context.
    # Context dict is appended as a JSON string separated by a pipe character.
    def send(
        self,
        level: NotificationLevel,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        symbols = {
            NotificationLevel.INFO: "ℹ",
            NotificationLevel.WARNING: "⚠",
            NotificationLevel.ERROR: "✖",
            NotificationLevel.SUCCESS: "✔",
        }
        symbol = symbols.get(level, "•")
        ctx_str = ""
        if context:
            ctx_str = " | " + json.dumps(context, default=str)
        print(f"[{level.value.upper():7s}] {symbol} {message}{ctx_str}")


# Notification handler that POSTs WARNING and ERROR messages to an HTTP webhook URL.
# INFO and SUCCESS levels are silently dropped to avoid noisy webhook traffic.
class WebhookNotificationHandler(NotificationHandler):
    """
    Posts notifications to an HTTP webhook (Slack, Teams, custom).

    Requires the NOTIFICATION_WEBHOOK_URL environment variable.
    Only sends WARNING and ERROR levels to avoid noise.
    """

    def __init__(self, webhook_url: str) -> None:
        self._url = webhook_url

    # Sends a JSON POST to the configured webhook URL with level and message fields.
    # Failures are logged as warnings rather than raised so the pipeline is not interrupted.
    def send(
        self,
        level: NotificationLevel,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        if level not in (NotificationLevel.WARNING, NotificationLevel.ERROR):
            return

        try:
            import urllib.request
            payload = json.dumps(
                {
                    "text": f"[{level.value.upper()}] {message}",
                    "context": context or {},
                }
            ).encode()
            req = urllib.request.Request(
                self._url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as exc:
            logger.warning(
                "notification.webhook_failed",
                url=self._url,
                error=str(exc),
            )


# Thread-safe fan-out dispatcher that delivers each notification to all registered handlers.
# A LogNotificationHandler is always registered at construction as a guaranteed safety net.
class NotificationService:
    """
    Fan-out notification dispatcher.

    Thread-safe: handlers list is guarded by a lock.

    Usage::

        svc = NotificationService()
        svc.add_handler(ConsoleNotificationHandler())
        svc.notify(NotificationLevel.ERROR, "Parse failed", {"file": "x.csv"})
    """

    def __init__(self) -> None:
        self._handlers: list[NotificationHandler] = []
        self._lock = threading.Lock()
        # Log handler is always present
        self._handlers.append(LogNotificationHandler())

    # Appends a new handler to the list under the thread lock so registration is safe at any time.
    # The handler will receive all subsequent notify() calls from this service instance.
    def add_handler(self, handler: NotificationHandler) -> None:
        with self._lock:
            self._handlers.append(handler)

    # Dispatches a notification to every registered handler and increments the Prometheus counter.
    # Accepts either a NotificationLevel enum value or a plain string for convenience.
    def notify(
        self,
        level: NotificationLevel | str,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Dispatch notification to all registered handlers."""
        if isinstance(level, str):
            level = NotificationLevel(level.lower())

        get_metrics().notifications_sent.labels(level=level.value).inc()

        with self._lock:
            handlers = list(self._handlers)

        for handler in handlers:
            try:
                handler.send(level, message, context)
            except Exception as exc:
                # A misbehaving handler must not break the pipeline
                logger.error(
                    "notification.handler_error",
                    handler=type(handler).__name__,
                    error=str(exc),
                )

    # Factory method that returns a NotificationService with Console and Log handlers pre-registered.
    # Use this for quick setup in scripts or tests where a fully configured service is needed.
    @classmethod
    def default(cls) -> "NotificationService":
        """Return a service pre-configured with Console + Log handlers."""
        svc = cls()
        svc.add_handler(ConsoleNotificationHandler())
        return svc
