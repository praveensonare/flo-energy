"""
LangChain / LangGraph async meter reading agent.

Architecture
------------
* Uses ``langgraph.prebuilt.create_react_agent`` with ``ChatAnthropic`` as
  the reasoning model. LangGraph handles the tool-call / tool-result loop
  automatically, including parallel tool calls when the model requests them.

* ``process_file()`` is an ``async`` coroutine. Multiple files are processed
  concurrently via ``asyncio.gather()`` in ``process_files()``, with each file
  running in its own agent instance to avoid shared-state races.

* Tool execution is fully async (see ``langchain_tools.py``):
    - Blocking I/O (parse, DB writes) runs in a ``ThreadPoolExecutor``.
    - Session state is protected by ``asyncio.Lock``.
    - DB writes use ``atomic_bulk_insert`` for ACID guarantees.

* After ``parse_file``, the model is instructed to call ``validate_data`` AND
  ``generate_sql`` simultaneously. LangGraph processes them concurrently when
  the model returns both tool calls in a single response turn.

Agentic loop (managed by LangGraph):
    User message
      → Claude (emits tool calls)
        → tool results (async, possibly parallel)
          → Claude (emits more tool calls or final answer)
            → … until stop_reason == "end_turn"
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import structlog

try:
    from langchain_anthropic import ChatAnthropic
    from langgraph.prebuilt import create_react_agent
except ImportError as _err:
    raise ImportError(
        f"LangChain dependencies not installed: {_err}\n"
        "Run:  pip install -r requirements.txt"
    ) from _err

from src.agent.langchain_tools import ALL_TOOLS
from src.config import settings
from src.observability.metrics import MetricsCollector, configure_logging, get_metrics

logger = structlog.get_logger(__name__)

_MODEL = "claude-opus-4-6"

_SYSTEM_PROMPT = """\
You are a Meter Reading ETL Agent. Process Australian energy meter data files \
(NEM12 / NEM13 format) into PostgreSQL.

For each file, call tools in this exact order:
  1. read_file      – detect format; stop with error notification if file is missing
  2. parse_file     – extract all meter readings
  3. validate_data AND generate_sql – call BOTH simultaneously in one turn
  4. write_to_database – atomic insert (one transaction; all-or-nothing)
  5. send_notification – report final outcome (level: success / warning / error)

Rules:
- Always end with send_notification.
- If parse_file returns errors, send a warning notification and continue.
- validate_data and generate_sql are independent; call them in parallel.
- Be concise; focus on tool calls over explanatory text.
"""


class LangChainMeterAgent:
    """
    Async LangGraph agent for NEM12/NEM13 ETL.

    Usage::

        agent = LangChainMeterAgent.from_env()

        # Single file
        result = await agent.process_file("data/sample.csv")

        # Multiple files processed in parallel
        results = await agent.process_files(["a.csv", "b.csv", "c.csv"])

        agent.shutdown()
    """

    def __init__(
        self,
        api_key: str | None = None,
        metrics_port: int | None = None,
    ) -> None:
        model = ChatAnthropic(
            model=_MODEL,
            api_key=api_key or settings.anthropic_api_key,
            max_tokens=8192,
        )
        # create_react_agent builds the LangGraph StateGraph with the
        # tool-call / tool-result loop wired up automatically.
        self._agent = create_react_agent(
            model,
            ALL_TOOLS,
            prompt=_SYSTEM_PROMPT,
        )

        self._metrics_thread = MetricsCollector(
            interval_seconds=15,
            prometheus_port=metrics_port,
        )
        self._metrics_thread.start()

    @classmethod
    def from_env(cls) -> "LangChainMeterAgent":
        """Construct agent from .env / environment variables."""
        configure_logging(
            log_level=settings.log_level,
            json_output=settings.log_format == "json",
        )
        return cls(metrics_port=settings.metrics_port or None)

    # ------------------------------------------------------------------
    # Single file
    # ------------------------------------------------------------------

    async def process_file(self, file_path: str) -> dict[str, Any]:
        """
        Run the full ETL pipeline for one file asynchronously.

        Returns a dict with ``file_path``, ``elapsed_s``, and ``summary``.
        """
        logger.info("agent.process_start", file=file_path)
        start = time.monotonic()

        message = (
            f"Process this meter data file: {file_path}\n\n"
            "Steps:\n"
            "1. read_file → detect format\n"
            "2. parse_file → extract readings\n"
            "3. validate_data + generate_sql → run BOTH in parallel\n"
            "4. write_to_database → atomic insert\n"
            "5. send_notification → report outcome\n"
        )
        if settings.enable_sql_output and settings.sql_output_path:
            message += f"\nWrite SQL to: {settings.sql_output_path}"

        response = await self._agent.ainvoke(
            {"messages": [("user", message)]}
        )

        elapsed = time.monotonic() - start

        # Extract the final AI text from the message history
        summary = ""
        for msg in reversed(response["messages"]):
            content = getattr(msg, "content", "")
            if isinstance(content, str) and content.strip():
                summary = content
                break

        get_metrics().files_processed.labels(format="NEM", status="completed").inc()

        logger.info(
            "agent.process_complete",
            file=file_path,
            elapsed_s=round(elapsed, 2),
        )

        return {
            "file_path": file_path,
            "elapsed_s": round(elapsed, 2),
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Multiple files in parallel
    # ------------------------------------------------------------------

    async def process_files(self, file_paths: list[str]) -> list[dict[str, Any]]:
        """
        Process multiple files concurrently using ``asyncio.gather``.

        Each file runs its own independent agent loop. Race conditions between
        files are prevented because each file gets its own ``ParseSession``
        (keyed by session_id) in the shared session store, and the store is
        guarded by ``asyncio.Lock`` in ``langchain_tools.py``.

        Failed files return ``{"file_path": ..., "error": ...}`` so one bad
        file does not abort the others (``return_exceptions=True``).
        """
        logger.info("agent.batch_start", file_count=len(file_paths))

        tasks = [self.process_file(fp) for fp in file_paths]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        results: list[dict[str, Any]] = []
        for fp, outcome in zip(file_paths, raw_results):
            if isinstance(outcome, Exception):
                logger.error("agent.file_failed", file=fp, error=str(outcome))
                results.append({"file_path": fp, "error": str(outcome)})
            else:
                results.append(outcome)  # type: ignore[arg-type]

        logger.info(
            "agent.batch_complete",
            total=len(file_paths),
            failed=sum(1 for r in results if "error" in r),
        )
        return results

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        """Stop background metrics thread."""
        self._metrics_thread.stop()
        logger.info("agent.shutdown")
