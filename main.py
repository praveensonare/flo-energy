"""
Meter Reading ETL Agent — LangChain / LangGraph agentic pipeline.

Usage:
    python main.py <file> [<file2> ...] [--output <sql_file>]

Environment variables (set in .env):
    ANTHROPIC_API_KEY  – Claude API key (required)
    DATABASE_URL       – PostgreSQL DSN  (or PGHOST / PGPORT / etc.)
    LOG_LEVEL          – DEBUG / INFO / WARNING / ERROR  (default: INFO)
    LOG_FORMAT         – json / console  (default: json)
    ENABLE_SQL_OUTPUT  – true / false    (default: false)
    SQL_OUTPUT_PATH    – path for SQL output file
    METRICS_PORT       – Prometheus port (0 = disabled)

Multiple files are processed concurrently via asyncio.gather().
Each file runs its own LangGraph agent loop; the DB write uses a single
PostgreSQL transaction per file (ACID-safe).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

from src.config import settings
from src.observability.metrics import configure_logging

configure_logging(
    log_level=settings.log_level,
    json_output=settings.log_format == "json",
)

import structlog  # noqa: E402 – must import after configure_logging

logger = structlog.get_logger(__name__)


async def _run(file_paths: list[str]) -> int:
    from src.agent.langchain_agent import LangChainMeterAgent

    if not settings.anthropic_api_key:
        print(
            "ERROR: ANTHROPIC_API_KEY is not set. Add it to .env and retry.",
            file=sys.stderr,
        )
        return 1

    agent = LangChainMeterAgent.from_env()
    try:
        results = await agent.process_files(file_paths)

        print("\n" + "=" * 60)
        for r in results:
            if "error" in r:
                print(f"FAILED  {r['file_path']}")
                print(f"  Error: {r['error']}")
            else:
                print(f"OK      {r['file_path']}  ({r['elapsed_s']:.2f}s)")
                if r.get("summary"):
                    print(f"  {r['summary']}")
        print("=" * 60)

        failed = sum(1 for r in results if "error" in r)
        return 1 if failed == len(results) else 0

    except KeyboardInterrupt:
        logger.info("main.interrupted")
        return 130
    except Exception as exc:
        logger.exception("main.fatal_error", error=str(exc))
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1
    finally:
        agent.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Meter Reading ETL Agent — processes NEM12/NEM13 files into PostgreSQL "
            "using a LangGraph async agent (Claude Opus)."
        )
    )
    parser.add_argument(
        "file_paths",
        nargs="+",
        help="One or more NEM12/NEM13 meter data files to process.",
    )
    parser.add_argument(
        "--output",
        metavar="SQL_FILE",
        help="Write generated SQL INSERT statements to this file.",
    )
    args = parser.parse_args()

    missing = [fp for fp in args.file_paths if not os.path.exists(fp)]
    if missing:
        for fp in missing:
            print(f"ERROR: File not found: {fp}", file=sys.stderr)
        return 1

    if args.output:
        os.environ["ENABLE_SQL_OUTPUT"] = "true"
        os.environ["SQL_OUTPUT_PATH"] = args.output

    return asyncio.run(_run(args.file_paths))


if __name__ == "__main__":
    sys.exit(main())
