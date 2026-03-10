#!/usr/bin/env python3
"""
process_nem12.py — NEM12 file processor (tech-assessment runner).

Reads one or more NEM12 files (CSV or ZIP), validates, parses, and
inserts the interval meter readings into PostgreSQL at localhost:5432.
The ``meter_readings`` table is created automatically if it does not exist;
existing data is preserved.

Usage::

    python process_nem12.py <file> [<file> …] [options]

    # Process the assessment sample file
    python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv"

    # Process multiple files
    python process_nem12.py data/*.csv data/*.zip

    # Dry-run: parse and validate without writing to DB
    python process_nem12.py data/nem12#assessment001#UNITEDDP#NEMMCO.csv --dry-run

    # Print generated INSERT SQL to stdout instead of inserting
    python process_nem12.py data/nem12#assessment001#UNITEDDP#NEMMCO.csv --sql-only

Configuration is read from ``.env`` in the project root.  Override any value
with environment variables (e.g. ``PGHOST=myserver python process_nem12.py …``).
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Load settings first so all downstream imports pick up .env values.
from src.config import settings  # noqa: E402  (intentional import order)

import structlog

from src.database.postgres_handler import PostgresHandler
from src.parsers.nem12_parser import NEM12Parser
from src.services.database_cache_service import DatabaseCacheService
from src.services.nem12_processor import NEM12ProcessingService, SharedReadingList

# ---------------------------------------------------------------------------
# Logging bootstrap
# ---------------------------------------------------------------------------

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(__import__("logging"), settings.log_level, 20)
    ),
)
logger = structlog.get_logger("process_nem12")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="process_nem12.py",
        description=(
            "Parse NEM12 interval meter data files and insert readings "
            "into PostgreSQL (localhost:5432 by default, configurable via .env)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "files",
        nargs="+",
        metavar="FILE",
        help="Path(s) to NEM12 .csv or .zip files.  Filenames must follow "
             "the NEM12 naming convention: NEM12#<ID>#<From>#<To>.csv|zip",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate files but do not write to the database.",
    )
    p.add_argument(
        "--sql-only",
        action="store_true",
        help="Print generated INSERT SQL to stdout instead of inserting to DB.",
    )
    p.add_argument(
        "--no-validate-filename",
        action="store_true",
        help=(
            "Skip NEM12 filename convention check.  Useful for running "
            "sample_nem12_full.csv or other files without spec-compliant names."
        ),
    )
    p.add_argument(
        "--warm-cache",
        action="store_true",
        help="Pre-load all NMIs found in the file(s) into the DB cache after insert.",
    )
    p.add_argument(
        "--query",
        metavar="NMI",
        help="After processing, query and display cached readings for this NMI.",
    )
    return p


# ---------------------------------------------------------------------------
# Core processing helpers
# ---------------------------------------------------------------------------

def _process_with_service(
    file_paths: list[str],
    db_svc: DatabaseCacheService,
    *,
    dry_run: bool,
    sql_only: bool,
    warm_cache: bool,
) -> dict:
    """
    Run the full pipeline for each file via :class:`NEM12ProcessingService`.

    Returns aggregate stats: {files, readings, inserted, skipped, failed, elapsed_s}
    """
    shared = SharedReadingList()
    svc = NEM12ProcessingService(shared_readings=shared)
    svc.start()

    t0 = time.monotonic()

    for fp in file_paths:
        svc.submit(fp)

    svc.join_queue()
    svc.stop()

    results = svc.get_results()
    elapsed = time.monotonic() - t0

    # Collect all validated readings from the service results
    all_readings = []
    for r in results:
        if r.success:
            all_readings.extend(r.readings)

    stats: dict = {
        "files_submitted": len(file_paths),
        "files_succeeded": sum(1 for r in results if r.success),
        "files_failed": sum(1 for r in results if not r.success),
        "readings_parsed": len(all_readings),
        "inserted": 0,
        "skipped": 0,
        "failed": 0,
        "elapsed_s": round(elapsed, 3),
    }

    if not all_readings:
        logger.warning("process.no_readings")
        _print_file_results(results)
        return stats

    if sql_only:
        # Generate and print INSERT SQL without touching the DB
        db_handler = PostgresHandler.from_env()
        sql = db_handler.generate_insert_sql(all_readings)
        print(sql)
        stats["inserted"] = len(all_readings)
        return stats

    if dry_run:
        logger.info(
            "process.dry_run",
            readings=len(all_readings),
            message="Dry-run: skipping database write.",
        )
        stats["skipped"] = len(all_readings)
    else:
        write_result = db_svc.write(all_readings)
        stats["inserted"] = write_result.inserted
        stats["skipped"] = write_result.skipped
        stats["failed"] = write_result.failed

        if warm_cache:
            nmis = list({r.nmi for r in all_readings})
            for nmi in nmis:
                db_svc.query_by_nmi(nmi)   # warms cache for this NMI
            logger.info("process.cache_warmed", nmis=nmis)

    _print_file_results(results)
    return stats


def _print_file_results(results) -> None:
    """Print per-file processing outcome to stdout."""
    print("\n" + "─" * 70)
    print(f"  {'FILE':<45}  {'STATUS':<8}  READINGS")
    print("─" * 70)
    for r in results:
        name = Path(r.file_path).name
        status = "OK" if r.success else "FAILED"
        print(f"  {name:<45}  {status:<8}  {len(r.readings)}")
        for err in r.errors[:3]:
            print(f"    ✗ {err}")
        for warn in r.warnings[:3]:
            print(f"    ⚠ {warn}")
    print("─" * 70)


def _print_stats(stats: dict) -> None:
    """Print aggregate stats."""
    print("\n  Summary")
    print(f"  ├─ Files submitted : {stats['files_submitted']}")
    print(f"  ├─ Files succeeded : {stats['files_succeeded']}")
    print(f"  ├─ Files failed    : {stats['files_failed']}")
    print(f"  ├─ Readings parsed : {stats['readings_parsed']}")
    print(f"  ├─ DB inserted     : {stats['inserted']}")
    print(f"  ├─ DB skipped      : {stats['skipped']}  (already present)")
    print(f"  ├─ DB failed       : {stats['failed']}")
    print(f"  └─ Elapsed         : {stats['elapsed_s']} s\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    logger.info(
        "process_nem12.start",
        db=settings.effective_database_url.split("@")[-1],  # host:port/db only
        files=len(args.files),
        dry_run=args.dry_run,
        sql_only=args.sql_only,
    )

    # ── Validate file existence up-front ──────────────────────────────
    missing = [f for f in args.files if not Path(f).exists()]
    if missing:
        for m in missing:
            print(f"Error: file not found: {m}", file=sys.stderr)
        return 1

    # ── Handle --no-validate-filename by temporarily bypassing the ─────
    # ── service's filename regex and using the parser directly.     ─────
    if args.no_validate_filename:
        file_paths = args.files
        parser = NEM12Parser()
        from src.models.meter_reading import MeterReading
        all_readings: list[MeterReading] = []
        for fp in file_paths:
            result = parser.parse(fp)
            if result.errors:
                logger.warning("parse.errors", file=fp, errors=result.errors[:3])
            all_readings.extend(result.readings)
            logger.info("parse.done", file=fp, readings=len(result.readings))

        if not all_readings:
            print("No readings parsed.")
            return 0

        if args.sql_only:
            db_handler = PostgresHandler.from_env()
            print(db_handler.generate_insert_sql(all_readings))
            return 0

        if not args.dry_run:
            db_svc = DatabaseCacheService.from_env()
            db_svc.initialize()
            wr = db_svc.write(all_readings)
            print(f"\nInserted: {wr.inserted}  Skipped: {wr.skipped}  Failed: {wr.failed}")

            if args.query:
                qr = db_svc.query_by_nmi(args.query)
                _print_query_result(args.query, qr)

            db_svc.close()
        else:
            print(f"Dry-run: would insert {len(all_readings)} readings.")

        return 0

    # ── Standard path: NEM12ProcessingService (validates filename) ────
    if args.sql_only or args.dry_run:
        # No DB connection needed for dry-run / SQL-only
        db_svc = None
    else:
        db_svc = DatabaseCacheService.from_env()
        db_svc.initialize()  # CREATE TABLE IF NOT EXISTS — safe on existing DB

    try:
        stats = _process_with_service(
            args.files,
            db_svc,  # type: ignore[arg-type]
            dry_run=args.dry_run,
            sql_only=args.sql_only,
            warm_cache=args.warm_cache,
        )

        _print_stats(stats)

        if args.query and db_svc is not None:
            qr = db_svc.query_by_nmi(args.query)
            _print_query_result(args.query, qr)

        return 0 if stats["files_failed"] == 0 else 1

    finally:
        if db_svc is not None:
            db_svc.close()


def _print_query_result(nmi: str, qr) -> None:
    print(f"\n  Query: NMI = {nmi}")
    print(f"  ├─ Total readings : {qr.total}")
    print(f"  ├─ From cache     : {qr.from_cache}")
    print(f"  └─ From DB        : {qr.from_db}")
    if qr.readings:
        print(f"\n  First 5 readings:")
        for r in qr.readings[:5]:
            print(f"    {r.timestamp.isoformat()}  {r.consumption} kWh")
    print()


if __name__ == "__main__":
    sys.exit(main())
