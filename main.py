"""
Main entry point for the Meter Reading ETL Agent.

Usage:
    python main.py <file_path> [--sql-only] [--output <sql_file>]

Environment variables required:
    ANTHROPIC_API_KEY  – Claude API key
    DATABASE_URL       – PostgreSQL connection string  (or PGHOST/PGPORT/etc.)

Optional:
    LOG_LEVEL          – DEBUG / INFO / WARNING / ERROR (default: INFO)
    LOG_FORMAT         – json / console (default: json)
    ENABLE_SQL_OUTPUT  – true / false (default: false)
    SQL_OUTPUT_PATH    – path to write SQL file
    METRICS_PORT       – port for Prometheus /metrics endpoint (0 = disabled)
"""
from __future__ import annotations

import argparse
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Meter Reading ETL Agent – processes NEM12/NEM13 files into PostgreSQL"
    )
    parser.add_argument(
        "file_path",
        help="Path to the NEM12 or NEM13 meter data file",
    )
    parser.add_argument(
        "--sql-only",
        action="store_true",
        help="Generate SQL only; do not write to database",
    )
    parser.add_argument(
        "--output",
        metavar="SQL_FILE",
        help="Write generated SQL to this file",
    )
    parser.add_argument(
        "--no-agent",
        action="store_true",
        help=(
            "Run the pipeline directly without the Claude AI agent "
            "(useful when ANTHROPIC_API_KEY is not set)"
        ),
    )
    args = parser.parse_args()

    if not os.path.exists(args.file_path):
        print(f"ERROR: File not found: {args.file_path}", file=sys.stderr)
        return 1

    if args.no_agent:
        return _run_direct(args)
    else:
        return _run_agent(args)


def _run_agent(args: argparse.Namespace) -> int:
    """Run the full AI-powered agentic pipeline."""
    from src.agent.meter_reading_agent import MeterReadingAgent

    api_key = settings.anthropic_api_key
    if not api_key:
        print(
            "ERROR: ANTHROPIC_API_KEY is not set.  "
            "Use --no-agent to run without the AI agent.",
            file=sys.stderr,
        )
        return 1

    if args.output:
        os.environ["ENABLE_SQL_OUTPUT"] = "true"
        os.environ["SQL_OUTPUT_PATH"] = args.output

    agent = MeterReadingAgent.from_env()
    try:
        result = agent.process_file(args.file_path)
        print("\n" + "=" * 60)
        print("Agent Summary")
        print("=" * 60)
        print(result["summary"])
        print(f"\nTotal iterations: {result['iterations']}")
        print(f"Elapsed: {result['elapsed_s']:.2f}s")
        return 0
    except KeyboardInterrupt:
        logger.info("main.interrupted")
        return 130
    except Exception as exc:
        logger.exception("main.fatal_error", error=str(exc))
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1
    finally:
        agent.shutdown()


def _run_direct(args: argparse.Namespace) -> int:
    """
    Run the pipeline without the Claude AI agent.

    Useful for CI, local testing, and when the API key is unavailable.
    """
    from src.database.postgres_handler import PostgresHandler
    from src.notifications.notification_service import (
        NotificationLevel,
        NotificationService,
    )
    from src.parsers.parser_factory import ParserFactory

    notifier = NotificationService.default()

    try:
        # 1. Detect format
        fmt = ParserFactory.get_format(args.file_path)
        logger.info("main.direct.format_detected", format=fmt, file=args.file_path)

        # 2. Parse
        parser = ParserFactory.get_parser(args.file_path)
        result = parser.parse(args.file_path)

        logger.info(
            "main.direct.parsed",
            readings=len(result.readings),
            nmis=result.nmis,
            errors=len(result.errors),
        )

        if result.errors:
            for err in result.errors[:5]:
                print(f"  WARN: {err}")

        # 3. SQL output (optional)
        if args.output or args.sql_only:
            db = PostgresHandler.from_env()
            sql = db.generate_insert_sql(result.readings)
            out_path = args.output or "meter_readings.sql"
            with open(out_path, "w") as fh:
                fh.write(sql)
            print(f"SQL written to: {out_path}  ({len(result.readings)} statements)")

        # 4. Write to database (unless --sql-only)
        if not args.sql_only:
            db = PostgresHandler.from_env()
            stats = db.bulk_insert(result.readings)
            notifier.notify(
                NotificationLevel.SUCCESS,
                f"ETL complete: {stats['inserted']} inserted, "
                f"{stats['skipped']} skipped, {stats['failed']} failed",
                {"file": args.file_path, **stats},
            )
            print(
                f"\nComplete: inserted={stats['inserted']}  "
                f"skipped={stats['skipped']}  failed={stats['failed']}"
            )

        return 0

    except NotImplementedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        logger.exception("main.direct.error", error=str(exc))
        notifier.notify(NotificationLevel.ERROR, str(exc), {"file": args.file_path})
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
