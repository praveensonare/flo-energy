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


class NotificationLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


class NotificationHandler(ABC):
    """Abstract base for notification delivery channels."""

    @abstractmethod
    def send(
        self,
        level: NotificationLevel,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Deliver a notification. Implementations must not raise."""


class LogNotificationHandler(NotificationHandler):
    """Writes all notifications to the structured log."""

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


class ConsoleNotificationHandler(NotificationHandler):
    """Writes notifications to stdout in a human-friendly format."""

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


class WebhookNotificationHandler(NotificationHandler):
    """
    Posts notifications to an HTTP webhook (Slack, Teams, custom).

    Requires the NOTIFICATION_WEBHOOK_URL environment variable.
    Only sends WARNING and ERROR levels to avoid noise.
    """

    def __init__(self, webhook_url: str) -> None:
        self._url = webhook_url

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

    def add_handler(self, handler: NotificationHandler) -> None:
        with self._lock:
            self._handlers.append(handler)

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

    @classmethod
    def default(cls) -> "NotificationService":
        """Return a service pre-configured with Console + Log handlers."""
        svc = cls()
        svc.add_handler(ConsoleNotificationHandler())
        return svc
