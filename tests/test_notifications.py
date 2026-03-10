"""
Unit tests for NotificationService.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.notifications.notification_service import (
    NotificationHandler,
    NotificationLevel,
    NotificationService,
)


class CapturingHandler(NotificationHandler):
    """Records all notifications for assertion."""

    def __init__(self):
        self.calls: list[tuple] = []

    def send(self, level, message, context=None):
        self.calls.append((level, message, context))


class RaisingHandler(NotificationHandler):
    """Simulates a broken handler."""

    def send(self, level, message, context=None):
        raise RuntimeError("Handler is broken")


class TestNotificationService:
    def test_dispatches_to_all_handlers(self):
        svc = NotificationService()
        h1 = CapturingHandler()
        h2 = CapturingHandler()
        svc.add_handler(h1)
        svc.add_handler(h2)
        svc.notify(NotificationLevel.INFO, "hello")
        assert len(h1.calls) == 1
        assert len(h2.calls) == 1

    def test_broken_handler_does_not_crash_service(self):
        svc = NotificationService()
        svc.add_handler(RaisingHandler())
        good = CapturingHandler()
        svc.add_handler(good)
        # Should not raise
        svc.notify(NotificationLevel.ERROR, "test")
        assert len(good.calls) == 1

    def test_accepts_string_level(self):
        svc = NotificationService()
        h = CapturingHandler()
        svc.add_handler(h)
        svc.notify("warning", "string level")
        assert h.calls[0][0] == NotificationLevel.WARNING

    def test_default_factory_has_handlers(self):
        svc = NotificationService.default()
        # Confirm it has at least two handlers (log + console)
        assert len(svc._handlers) >= 2

    def test_context_forwarded(self):
        svc = NotificationService()
        h = CapturingHandler()
        svc.add_handler(h)
        ctx = {"file": "test.csv", "count": 42}
        svc.notify(NotificationLevel.SUCCESS, "done", ctx)
        assert h.calls[0][2] == ctx
