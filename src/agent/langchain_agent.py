"""
LangChain / LangGraph async meter reading agent.
Replaces the old manual Anthropic agentic loop with create_react_agent.

Architecture
------------
* ``create_react_agent`` builds a LangGraph StateGraph that handles the
  tool-call / tool-result loop automatically, including parallel tool calls
  when Claude emits multiple tool_use blocks in a single response turn.
* ``process_file()`` is a coroutine; ``process_files()`` runs N files
  concurrently with ``asyncio.gather()``.  Each file gets its own agent
  invocation so session state is completely isolated.
* Blocking calls (parse, DB) are offloaded to a ThreadPoolExecutor inside
  each ``@tool`` function (see langchain_tools.py), keeping the event loop free.
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
(NEM12 / NEM13 format, supplied as CSV or ZIP) into PostgreSQL.

For each file call tools in this exact order:
  1. validate_filename – check NEM12 naming convention; warn if non-compliant but continue
  2. read_file         – detect format (NEM12/NEM13); stop with error if file missing
  3. parse_file        – extract all meter readings
  4. validate_data AND generate_sql – call BOTH simultaneously in one turn
  5. write_to_database – atomic insert (one transaction; all-or-nothing)
  6. send_notification – report final outcome (level: success / warning / error)

Rules:
- Always end with send_notification.
- If validate_filename fails, include a warning in the final notification but continue.
- If parse_file returns errors, send a warning notification and continue.
- validate_data and generate_sql are independent; always call them in parallel.
- Be concise; focus on tool calls over explanatory text.
"""


class LangChainMeterAgent:
    """
    Async LangGraph agent for NEM12/NEM13 ETL powered by Claude Opus.
    Handles both CSV and ZIP input files; processes multiple files concurrently.
    """

    def __init__(
        self,
        api_key: str | None = None,
        metrics_port: int | None = None,
    ) -> None:
        # Build the LLM client and wire it into a LangGraph ReAct agent with all tools.
        # api_key falls back to settings.anthropic_api_key when not explicitly provided.
        model = ChatAnthropic(
            model=_MODEL,
            api_key=api_key or settings.anthropic_api_key,
            max_tokens=8192,
        )
        self._agent = create_react_agent(
            model,
            ALL_TOOLS,
            prompt=_SYSTEM_PROMPT,
        )

        # MetricsCollector runs as a background daemon thread; stop() is called in shutdown().
        self._metrics_thread = MetricsCollector(
            interval_seconds=15,
            prometheus_port=metrics_port,
        )
        self._metrics_thread.start()

    @classmethod
    def from_env(cls) -> "LangChainMeterAgent":
        # Construct agent from .env / environment variables via pydantic-settings.
        # Configures structured logging before returning so the first log line is formatted.
        configure_logging(
            log_level=settings.log_level,
            json_output=settings.log_format == "json",
        )
        return cls(metrics_port=settings.metrics_port or None)

    # ------------------------------------------------------------------
    # Single-file processing
    # ------------------------------------------------------------------

    async def process_file(self, file_path: str) -> dict[str, Any]:
        """
        Run the full ETL pipeline for one file asynchronously.
        Returns a dict with file_path, elapsed_s, and the agent's final summary text.
        """
        logger.info("agent.process_start", file=file_path)
        start = time.monotonic()

        message = (
            f"Process this meter data file: {file_path}\n\n"
            "Steps:\n"
            "1. validate_filename → check NEM12 naming convention\n"
            "2. read_file → detect format (CSV or ZIP both supported)\n"
            "3. parse_file → extract readings\n"
            "4. validate_data + generate_sql → call BOTH in parallel\n"
            "5. write_to_database → atomic insert (table auto-created if missing)\n"
            "6. send_notification → report outcome\n"
        )
        if settings.enable_sql_output and settings.sql_output_path:
            message += f"\nWrite SQL to: {settings.sql_output_path}"

        response = await self._agent.ainvoke(
            {"messages": [("user", message)]}
        )
        elapsed = time.monotonic() - start

        # Walk the message history backwards to find the last non-empty AI text.
        summary = ""
        for msg in reversed(response["messages"]):
            content = getattr(msg, "content", "")
            if isinstance(content, str) and content.strip():
                summary = content
                break

        get_metrics().files_processed.labels(format="NEM", status="completed").inc()
        logger.info("agent.process_complete", file=file_path, elapsed_s=round(elapsed, 2))

        return {
            "file_path": file_path,
            "elapsed_s": round(elapsed, 2),
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Parallel multi-file processing
    # ------------------------------------------------------------------

    async def process_files(self, file_paths: list[str]) -> list[dict[str, Any]]:
        """
        Process multiple files concurrently using asyncio.gather.
        Each file runs its own agent loop; a failure in one does not abort the others.
        """
        logger.info("agent.batch_start", file_count=len(file_paths))

        tasks = [self.process_file(fp) for fp in file_paths]
        # return_exceptions=True isolates failures so one bad file doesn't kill the batch.
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        results: list[dict[str, Any]] = []
        for fp, outcome in zip(file_paths, raw_results):
            if isinstance(outcome, Exception):
                logger.error("agent.file_failed", file=fp, error=str(outcome))
                results.append({"file_path": fp, "error": str(outcome)})
            else:
                results.append(outcome)  # type: ignore[arg-type]

        failed = sum(1 for r in results if "error" in r)
        logger.info("agent.batch_complete", total=len(file_paths), failed=failed)
        return results

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        # Signal the background metrics thread to stop; call before process exit.
        self._metrics_thread.stop()
        logger.info("agent.shutdown")
