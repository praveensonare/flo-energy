"""
Meter Reading Agent – orchestrates the full ETL pipeline using the Claude API.

Architecture:
  - Uses Claude Opus 4.6 with adaptive thinking for complex reasoning
  - Implements a manual agentic loop (not the SDK tool runner) for fine-grained
    control: logging every step, injecting observability, and handling errors
    gracefully without throwing exceptions to the caller
  - All domain logic lives in ToolRegistry; the agent only orchestrates
  - The system prompt tells Claude exactly what the task is and which tools
    to call in which order, acting as the "agent brain"

Agentic loop:
  User message → Claude (may call tools) → tool results → Claude → ...
  Loop exits when stop_reason == "end_turn" (no more tool calls needed).
"""
from __future__ import annotations

import json
import time
from typing import Any

import anthropic
import structlog

from src.agent.tools import TOOL_DEFINITIONS, ToolRegistry
from src.config import settings
from src.database.postgres_handler import PostgresHandler
from src.notifications.notification_service import (
    NotificationLevel,
    NotificationService,
)
from src.observability.metrics import MetricsCollector, configure_logging, get_metrics

logger = structlog.get_logger(__name__)

_MODEL = "claude-opus-4-6"
_MAX_TOKENS = 8192
_MAX_ITERATIONS = 20  # safety cap on the agentic loop

_SYSTEM_PROMPT = """You are a Meter Reading ETL Agent responsible for processing
Australian energy meter data files (NEM12 and NEM13 formats) and loading them
into a PostgreSQL database.

You have the following tools available and MUST use them in this order:

1. read_file      – Validate the file and detect its format
2. parse_file     – Parse the file and extract meter readings
3. validate_data  – Validate the parsed data for integrity
4. generate_sql   – Generate SQL INSERT statements
5. write_to_database – Write readings to the database
6. send_notification – Send a notification with the final result
7. get_processing_stats – (optional) Retrieve session statistics

Rules:
- Always call read_file first.  If the file is not found or unrecognised, call
  send_notification with level="error" and stop.
- If parse_file returns errors, call send_notification with level="warning"
  and continue (partial data is better than no data).
- If validate_data finds invalid records, log them and continue.
- Always call send_notification at the end with level="success" or "error".
- Use the session_id returned by read_file / parse_file for all subsequent calls.
- Be concise in your text responses; focus on tool calls.
"""


class MeterReadingAgent:
    """
    Agentic ETL pipeline powered by Claude Opus 4.6.

    Usage::

        agent = MeterReadingAgent.from_env()
        result = agent.process_file("/data/meters_nem12.csv")
        print(result["summary"])
    """

    def __init__(
        self,
        api_key: str | None = None,
        db_handler: PostgresHandler | None = None,
        notification_service: NotificationService | None = None,
        enable_sql_output: bool = False,
        sql_output_path: str | None = None,
        metrics_port: int | None = None,
    ) -> None:
        self._client = anthropic.Anthropic(
            api_key=api_key or settings.anthropic_api_key
        )
        self._tools = ToolRegistry(
            db_handler=db_handler,
            notification_service=notification_service,
        )
        self._enable_sql_output = enable_sql_output
        self._sql_output_path = sql_output_path

        # Start background metrics thread
        self._metrics_thread = MetricsCollector(
            interval_seconds=15,
            prometheus_port=metrics_port,
        )
        self._metrics_thread.start()

    @classmethod
    def from_env(cls) -> "MeterReadingAgent":
        """Construct agent from environment variables."""
        configure_logging(
            log_level=settings.log_level,
            json_output=settings.log_format == "json",
        )
        return cls(
            db_handler=PostgresHandler.from_env(),
            notification_service=NotificationService.default(),
            enable_sql_output=settings.enable_sql_output,
            sql_output_path=settings.sql_output_path,
            metrics_port=settings.metrics_port or None,
        )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def process_file(self, file_path: str) -> dict[str, Any]:
        """
        Run the full ETL pipeline for one file.

        Returns a summary dict with counts and any errors encountered.
        """
        logger.info("agent.process_start", file=file_path)
        start = time.monotonic()

        user_message = self._build_user_message(file_path)
        messages: list[anthropic.MessageParam] = [
            {"role": "user", "content": user_message}
        ]

        iteration = 0
        final_text = ""

        while iteration < _MAX_ITERATIONS:
            iteration += 1
            logger.debug("agent.loop_iteration", iteration=iteration)

            response = self._call_claude(messages)

            # Append full assistant response (preserves tool_use blocks)
            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                # Extract the final text response
                for block in response.content:
                    if hasattr(block, "text"):
                        final_text = block.text
                break

            if response.stop_reason == "tool_use":
                tool_results = self._execute_tool_calls(response.content)
                messages.append({"role": "user", "content": tool_results})
                continue

            # Unexpected stop reason
            logger.warning(
                "agent.unexpected_stop",
                stop_reason=response.stop_reason,
                iteration=iteration,
            )
            break

        elapsed = time.monotonic() - start
        get_metrics().files_processed.labels(format="NEM12", status="completed").inc()

        logger.info(
            "agent.process_complete",
            file=file_path,
            iterations=iteration,
            elapsed_s=round(elapsed, 2),
        )

        return {
            "file_path": file_path,
            "iterations": iteration,
            "elapsed_s": round(elapsed, 2),
            "summary": final_text,
            "messages": len(messages),
        }

    # ------------------------------------------------------------------
    # Claude API call (with streaming for long responses)
    # ------------------------------------------------------------------

    def _call_claude(
        self, messages: list[anthropic.MessageParam]
    ) -> anthropic.Message:
        """
        Call the Claude API with adaptive thinking enabled.

        Uses streaming + get_final_message() to avoid HTTP timeouts on
        large tool result payloads.
        """
        with self._client.messages.stream(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            thinking={"type": "adaptive"},
            system=_SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            messages=messages,
        ) as stream:
            # Stream any text deltas to the log for live progress visibility
            for text in stream.text_stream:
                logger.debug("agent.claude_text", text=text)
            return stream.get_final_message()

    # ------------------------------------------------------------------
    # Tool execution
    # ------------------------------------------------------------------

    def _execute_tool_calls(
        self, content: list[Any]
    ) -> list[dict[str, Any]]:
        """Execute all tool_use blocks and return tool_result messages."""
        tool_results: list[dict[str, Any]] = []

        for block in content:
            if block.type != "tool_use":
                continue

            logger.info(
                "agent.tool_call",
                tool=block.name,
                input=block.input,
            )

            result_content = self._tools.execute(block.name, block.input)

            # Parse to check status; log errors prominently
            try:
                parsed = json.loads(result_content)
                if parsed.get("status") == "error":
                    logger.error(
                        "agent.tool_error",
                        tool=block.name,
                        error=parsed.get("error"),
                    )
            except json.JSONDecodeError:
                pass

            logger.info(
                "agent.tool_result",
                tool=block.name,
                result_preview=result_content[:200],
            )

            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_content,
                }
            )

        return tool_results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_user_message(self, file_path: str) -> str:
        parts = [
            f"Please process the following meter data file: {file_path}",
            "",
            "Steps to follow:",
            "1. Read and validate the file",
            "2. Parse all meter readings",
            "3. Validate the data",
            "4. Generate SQL INSERT statements",
        ]
        if self._enable_sql_output and self._sql_output_path:
            parts.append(
                f"   - Write SQL to: {self._sql_output_path}"
            )
        parts += [
            "5. Write the readings to the PostgreSQL database",
            "6. Send a final notification with the outcome",
            "",
            "Report the total number of readings inserted, skipped, and any errors.",
        ]
        return "\n".join(parts)

    def shutdown(self) -> None:
        """Graceful shutdown: stop background threads."""
        self._metrics_thread.stop()
        logger.info("agent.shutdown")
