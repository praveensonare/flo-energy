# Flo Energy – NEM12 Meter Reading ETL

Parses Australian NEM12 interval meter data files (CSV or ZIP) and inserts the
readings into PostgreSQL.  Ships with two complementary entry points:

| Script | Purpose |
|---|---|
| `process_nem12.py` | Lightweight pipeline runner — parse → validate → insert (no AI required) |
| `main.py` | AI-powered agentic mode using Claude Opus 4.6 |

Built with **Python 3.12**, **psycopg2**, **pydantic-settings**, and packaged as a
**Docker** container.

---

## Quick Start

```bash
# 1. Clone and enter
git clone <repo-url> flo-energy && cd flo-energy

# 2. Configure (copy defaults — works out of the box for localhost:5432)
cp .env.example .env

# 3. Install Python dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 4. Start PostgreSQL (Docker)
docker compose up postgres -d

# 5. Process the assessment sample file
python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv"
```

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Configuration (.env)](#configuration-env)
3. [Running Locally](#running-locally)
4. [Running with Docker](#running-with-docker)
5. [Running with Docker Compose](#running-with-docker-compose)
6. [Architecture Overview](#architecture-overview)
7. [Project Structure](#project-structure)
8. [Environment Variables Reference](#environment-variables-reference)
9. [Database Schema](#database-schema)
10. [NEM12 Format Reference](#nem12-format-reference)
11. [Design Decisions Q&A](#design-decisions-qa)
12. [Future Work](#future-work)

---

## Prerequisites

### Local

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.12+ | Earlier versions untested |
| PostgreSQL | 15+ | Local install **or** Docker (see below) |
| gcc / libpq-dev | any | Only needed to compile psycopg2 from source |

> **macOS shortcut** — install Postgres.app; psycopg2-binary bundles the libs.
> **Linux** — `sudo apt install libpq-dev gcc` before `pip install`.

### Docker

| Requirement | Version |
|---|---|
| Docker Engine | 24+ |
| Docker Compose | v2 (plugin) |

---

## Configuration (.env)

All settings are read from a `.env` file in the project root via
**pydantic-settings** (`src/config.py`).  Environment variables always override
`.env` values.

```bash
# Copy the template — works out of the box for localhost:5432
cp .env.example .env
```

Key settings you may want to change:

```dotenv
# PostgreSQL connection (individual vars or single DATABASE_URL)
PGHOST=localhost
PGPORT=5432
PGDATABASE=meter_db
PGUSER=postgres
PGPASSWORD=postgres

# Logging
LOG_LEVEL=INFO          # DEBUG | INFO | WARNING | ERROR
LOG_FORMAT=console      # console | json

# Only required for AI-agent mode (main.py without --no-agent)
# ANTHROPIC_API_KEY=sk-ant-...
```

See [Environment Variables Reference](#environment-variables-reference) for the
full list.

---

## Running Locally

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Start PostgreSQL

**Option A — Docker (recommended for local dev):**

```bash
docker compose up postgres -d
# Postgres now available at localhost:5432, database=meter_db, user/pass=postgres
```

**Option B — native PostgreSQL:**

```bash
# macOS (Homebrew)
brew services start postgresql@15

# Ubuntu / Debian
sudo systemctl start postgresql

# Create the database (once)
createdb -U postgres meter_db
```

### 3. Process NEM12 files — `process_nem12.py`

This is the primary runner for the tech assessment.  It:
- Connects to `localhost:5432` (or as configured in `.env`)
- Creates `meter_readings` table if it does not exist (existing data is **never** deleted)
- Validates the filename against the NEM12 naming convention
- Parses and validates the file
- Inserts readings via write-through cache

```bash
# Process the assessment sample (2 NMIs, 384 readings)
python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv"

# Process a ZIP file
python process_nem12.py "data/nem12#0123456789ABCDEF#TESTMDP1#TESTRETAIL.zip"

# Process multiple files at once
python process_nem12.py data/nem12#*.csv

# Dry-run — parse and validate without writing to DB
python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv" --dry-run

# Print INSERT SQL to stdout instead of inserting
python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv" --sql-only

# Skip NEM12 filename validation (useful for ad-hoc files)
python process_nem12.py data/sample_nem12.csv --no-validate-filename

# Insert then query the cache for a specific NMI
python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv" \
    --query NEM1201009

# Pre-warm the in-memory cache after insert
python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv" \
    --warm-cache --query NEM1201009
```

Expected output (normal run):

```
──────────────────────────────────────────────────────────────────────
  FILE                                           STATUS    READINGS
──────────────────────────────────────────────────────────────────────
  nem12#assessment001#UNITEDDP#NEMMCO.csv        OK        384
──────────────────────────────────────────────────────────────────────

  Summary
  ├─ Files submitted : 1
  ├─ Files succeeded : 1
  ├─ Files failed    : 0
  ├─ Readings parsed : 384
  ├─ DB inserted     : 384
  ├─ DB skipped      : 0   (already present)
  ├─ DB failed       : 0
  └─ Elapsed         : 0.012 s
```

### 4. Run tests

```bash
pytest                                  # all tests + coverage report
pytest tests/test_nem12_parser.py       # single module
pytest -x -v                            # stop on first failure, verbose
pytest --cov=src --cov-report=html      # HTML coverage at htmlcov/
```

### 5. AI-powered agent mode — `main.py`

Requires an Anthropic API key.  The agent uses Claude Opus 4.6 with adaptive
thinking to orchestrate the full pipeline.

```bash
# Set your API key in .env:  ANTHROPIC_API_KEY=sk-ant-...

# Agent mode (AI orchestrated)
python main.py data/sample_nem12.csv

# Direct pipeline — no API key needed
python main.py data/sample_nem12.csv --no-agent

# Generate SQL only (no DB write)
python main.py data/sample_nem12.csv --no-agent --sql-only

# Save generated SQL to a file
python main.py data/sample_nem12.csv --no-agent --sql-only --output output/out.sql
```

---

## Running with Docker

### Build the image

```bash
docker build -t flo-energy .
```

The Dockerfile uses a **two-stage build**:
- `builder` — installs gcc + libpq-dev, compiles Python packages
- `runtime` — lean Python 3.12-slim image, no build tools, runs as non-root `appuser`

### Run `process_nem12.py` in Docker

```bash
# Dry-run against the assessment sample (no DB needed)
docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  --entrypoint python \
  flo-energy \
  process_nem12.py "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv" --dry-run

# Insert into a local Postgres (host.docker.internal resolves to the host machine)
docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  -e PGHOST=host.docker.internal \
  -e PGPORT=5432 \
  -e PGDATABASE=meter_db \
  -e PGUSER=postgres \
  -e PGPASSWORD=postgres \
  --entrypoint python \
  flo-energy \
  process_nem12.py "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv"

# Print SQL to stdout
docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  --entrypoint python \
  flo-energy \
  process_nem12.py "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv" --sql-only

# Save SQL to a local file
docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  -v "$(pwd)/output:/output" \
  --entrypoint python \
  flo-energy \
  process_nem12.py "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv" \
  --sql-only > output/assessment_inserts.sql
```

### Run `main.py` (AI agent) in Docker

```bash
# Direct pipeline — no API key needed
docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  -e DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5432/meter_db \
  flo-energy /data/sample_nem12.csv --no-agent

# AI agent mode
docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  -v "$(pwd)/output:/output" \
  -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  -e DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5432/meter_db \
  flo-energy /data/sample_nem12.csv
```

> **Linux note:** `host.docker.internal` requires `--add-host host.docker.internal:host-gateway`
> on Linux Docker.  Alternatively, use Docker Compose (see below) which uses a shared network.

---

## Running with Docker Compose

Docker Compose starts **PostgreSQL + the agent** in a shared network — no need
for `host.docker.internal`.

### Start only PostgreSQL (most common for local dev)

```bash
docker compose up postgres -d

# Verify it is healthy
docker compose ps
# postgres    running (healthy)
```

### Run `process_nem12.py` via Compose

```bash
# Assessment sample — insert into Compose Postgres
docker compose run --rm agent \
  python process_nem12.py "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv"

# Dry-run
docker compose run --rm agent \
  python process_nem12.py "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv" --dry-run

# SQL-only (print to console)
docker compose run --rm agent \
  python process_nem12.py "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv" --sql-only

# Full sample with all quality variants
docker compose run --rm agent \
  python process_nem12.py "/data/nem12#0123456789ABCDEF#TESTMDP1#TESTRETAIL.csv"
```

### Run `main.py` (AI agent) via Compose

```bash
# Direct pipeline — no API key
docker compose run --rm agent /data/sample_nem12.csv --no-agent

# AI agent (set ANTHROPIC_API_KEY in .env first)
docker compose run --rm \
  -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  agent /data/sample_nem12.csv
```

### Start everything (Postgres + agent default command)

```bash
docker compose up
# Runs: python main.py /data/sample_nem12.csv --no-agent   (default command)
```

### Tear down

```bash
docker compose down        # stop containers, keep volume
docker compose down -v     # stop containers AND delete data volume
```

---

## Architecture Overview

```
  Files on disk  (CSV / ZIP)
       │
       ▼
┌─────────────────────────────────────────────────────────────────┐
│  NEM12ProcessingService          (background thread)            │
│                                                                 │
│  1. Validate filename  →  NEM12#<ID>#<From>#<To>.csv|zip        │
│  2. Decompress ZIP     →  extract to temp dir, cleanup on done  │
│  3. Parse NEM12        →  NEM12Parser (streaming, O(batch_size))│
│  4. Validate data      →  duplicates, negative values, order    │
│  5. Publish            →  SharedReadingList (mutex + Condition) │
└──────────────────────────────┬──────────────────────────────────┘
                               │  thread-safe shared list
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  DatabaseCacheService         (write-through cache)             │
│                                                                 │
│  write(readings) ──▶ PostgreSQL (bulk insert, ON CONFLICT)      │
│                  ──▶ in-memory cache update (RLock)             │
│                                                                 │
│  query_by_nmi()  ──▶ cache hit? serve instantly                 │
│                  ──▶ cache miss? query DB, populate cache       │
│                                                                 │
│  Internal: dict[(nmi, ts), MeterReading]  +  bisect index       │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
                    ┌──────────────────┐
                    │   PostgreSQL     │
                    │  localhost:5432  │
                    │  meter_readings  │
                    └──────────────────┘
```

### AI Agent Mode (`main.py`)

```
MeterReadingAgent (Claude Opus 4.6)
  │
  └─ Agentic loop → tool_use blocks
       │
       ├── read_file          → detect format (NEM12 / NEM13)
       ├── parse_file         → stream readings from CSV
       ├── validate_data      → duplicates, negative, future timestamps
       ├── generate_sql       → INSERT statements (dry-run / audit)
       ├── write_to_database  → bulk insert with retry
       ├── send_notification  → log / console / webhook
       └── get_processing_stats → session summary
```

---

## Project Structure

```
flo-energy/
├── src/
│   ├── config.py                    # ← pydantic-settings, loads .env
│   ├── models/
│   │   └── meter_reading.py         # MeterReading, NEM12Block, ParseSession
│   ├── parsers/
│   │   ├── base_parser.py           # Abstract parser interface
│   │   ├── nem12_parser.py          # NEM12 streaming CSV parser
│   │   ├── nem13_parser.py          # NEM13 stub (future)
│   │   └── parser_factory.py        # Format auto-detection
│   ├── database/
│   │   └── postgres_handler.py      # ThreadedConnectionPool, batch inserts,
│   │                                #   fetch_by_nmi, fetch_nmis, fetch_date_range
│   ├── services/
│   │   ├── nem12_processor.py       # ← NEM12ProcessingService + SharedReadingList
│   │   └── database_cache_service.py# ← write-through cache over PostgresHandler
│   ├── notifications/
│   │   └── notification_service.py  # Log / Console / Webhook handlers
│   ├── observability/
│   │   └── metrics.py               # structlog + Prometheus metrics
│   └── agent/
│       ├── tools.py                 # Tool schemas + ToolRegistry
│       └── meter_reading_agent.py   # Claude Opus agentic loop
├── tests/                           # pytest unit tests
├── data/
│   ├── nem12#assessment001#UNITEDDP#NEMMCO.csv   # ← exact assessment example data
│   ├── nem12#…#TESTMDP1#TESTRETAIL.csv           # full-coverage sample (CSV)
│   ├── nem12#…#TESTMDP1#TESTRETAIL.zip           # full-coverage sample (ZIP)
│   └── sample_nem12_full.csv                     # 3 NMIs, all quality variants
├── process_nem12.py                 # ← primary runner (no AI required)
├── main.py                          # AI-agent runner
├── Dockerfile                       # Multi-stage build
├── docker-compose.yml               # Postgres + agent stack
├── .env.example                     # Config template — copy to .env
├── requirements.txt
└── README.md
```

---

## Environment Variables Reference

All variables are loaded from `.env` by `src/config.py`.  Set them in `.env`
or export them as shell variables (shell takes precedence).

| Variable | Default | Description |
|---|---|---|
| `PGHOST` | `localhost` | PostgreSQL host |
| `PGPORT` | `5432` | PostgreSQL port |
| `PGDATABASE` | `meter_db` | Database name |
| `PGUSER` | `postgres` | Database user |
| `PGPASSWORD` | `postgres` | Database password |
| `DATABASE_URL` | _(assembled from PG* vars)_ | Full DSN — overrides all PG* vars |
| `PG_MIN_CONNECTIONS` | `2` | Connection pool minimum |
| `PG_MAX_CONNECTIONS` | `10` | Connection pool maximum |
| `ANTHROPIC_API_KEY` | _(none)_ | Required for `main.py` agent mode |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `LOG_FORMAT` | `console` | `console` (human) or `json` (machine) |
| `ENABLE_SQL_OUTPUT` | `false` | Write SQL to file during agent run |
| `SQL_OUTPUT_PATH` | `output/meter_readings.sql` | Path for SQL output file |
| `METRICS_PORT` | `0` | Prometheus port (`0` = disabled) |
| `NOTIFICATION_WEBHOOK_URL` | _(none)_ | Slack / Teams webhook URL |
| `NEM12_QUEUE_MAX_SIZE` | `100` | Max files queued for processing |
| `NEM12_STOP_TIMEOUT_SECONDS` | `10.0` | Thread shutdown grace period |

---

## Database Schema

The table is created automatically on first run (`CREATE TABLE IF NOT EXISTS`).
Existing data is **never** modified or deleted.

```sql
CREATE TABLE meter_readings (
    id          UUID DEFAULT gen_random_uuid() NOT NULL,
    nmi         VARCHAR(10) NOT NULL,
    "timestamp" TIMESTAMP  NOT NULL,
    consumption NUMERIC    NOT NULL,
    CONSTRAINT meter_readings_pk
        PRIMARY KEY (id),
    CONSTRAINT meter_readings_unique_consumption
        UNIQUE (nmi, "timestamp")
);
```

Key design choices:
- **UUID primary key** — globally unique, safe for distributed inserts
- **`(nmi, timestamp)` unique constraint** — idempotency key; re-running the
  same file is safe (`ON CONFLICT DO NOTHING`)
- **NUMERIC consumption** — arbitrary precision; no floating-point rounding errors

---

## NEM12 Format Reference

### File naming convention (spec §3.2.2)

Files must be named:

```
NEM12#<UniqueID>#<From>#<To>.csv
NEM12#<UniqueID>#<From>#<To>.zip
```

| Component | Rules |
|---|---|
| `NEM12` | Literal, case-insensitive |
| `UniqueID` | 1–36 alphanumeric characters |
| `From` | Participant ID of the MDP (≤ 10 chars) |
| `To` | Participant ID of the recipient (≤ 10 chars) |

Example: `nem12#assessment001#UNITEDDP#NEMMCO.csv`

### Record types

| Record | Purpose | Handled |
|---|---|---|
| `100` | File header — version, datetime, from/to participant | ✅ |
| `200` | NMI block — NMI, interval length, UOM, meter serial | ✅ |
| `300` | Interval data — date + N consumption values + quality flag | ✅ |
| `400` | Per-interval quality overrides (used when 300 quality = `V`) | ✅ validated |
| `500` | B2B / service order details | ✅ parsed |
| `900` | End of file | ✅ |

### Timestamp convention

NEM12 uses **interval-ending** timestamps.  Interval `i` (1-indexed) on date
`D` with length `L` minutes maps to:

```
timestamp = D + i × L minutes
```

| Interval length | Intervals/day | Example (2005-03-01) |
|---|---|---|
| 30 min | 48 | Interval 1 → `2005-03-01 00:30:00`, Interval 48 → `2005-03-02 00:00:00` |
| 15 min | 96 | Interval 1 → `2005-03-01 00:15:00` |
| 5 min | 288 | Interval 1 → `2005-03-01 00:05:00` |

### Quality flags

| Flag | Meaning |
|---|---|
| `A` | Actual — direct meter reading |
| `S` | Substituted — replaced by MDP |
| `F` | Final substituted |
| `E` | Estimated |
| `V` | Variable — quality varies per interval; see 400 records |
| `N` | Null |

---

## Design Decisions Q&A

### Q1. Rationale for technology choices

| Technology | Rationale |
|---|---|
| **Python 3.12** | Rich data-processing ecosystem; `decimal`, `csv`, `threading` built-in; native `dataclasses` with `slots=True` for memory efficiency |
| **pydantic-settings** | Single `.env`-driven config with type validation; no scattered `os.getenv()` calls; environment variable override for CI/CD |
| **psycopg2 + ThreadedConnectionPool** | Battle-tested; `execute_values` bulk inserts are 10–100× faster than row-by-row; `ON CONFLICT DO NOTHING` makes the pipeline idempotent |
| **threading (not asyncio)** | NEM12ProcessingService and DatabaseCacheService are I/O-bound; Python threads work well here and compose naturally with the synchronous psycopg2 driver |
| **Write-through cache** | Eliminates repeat DB reads for NMIs already processed in the session; bisect-sorted timestamp index enables O(log n) range queries |
| **Claude Opus 4.6** | Adaptive thinking handles malformed files and ambiguous quality flags without hard-coded rules |
| **tenacity** | Declarative retry with exponential backoff for transient DB failures |
| **structlog** | JSON-structured logs with bound context; zero overhead when log level is above threshold |
| **Generator-based parsing** | `stream_readings()` processes arbitrarily large files with O(batch_size) memory |

### Q2. What I would do differently with more time

1. **Async pipeline** — `asyncio` + `asyncpg` for higher concurrency on I/O-bound DB writes
2. **Redis cache** — Replace the in-process dict with Redis for multi-instance deployments
3. **Alembic migrations** — Schema evolution without manual DDL
4. **Full 400-record support** — Apply per-interval quality overrides to each `MeterReading`
5. **NEM13 implementation** — Accumulation-to-interval conversion with meter rollover handling
6. **Scheduled ingestion** — `watchdog`-based directory monitor honouring `NextScheduledReadDate`
7. **OpenTelemetry** — Distributed tracing instead of ad-hoc timing
8. **Dead-letter queue** — Re-queue failed batches via Celery / SQS without reprocessing the file
9. **Integration tests** — `testcontainers` fixtures spin up a real Postgres for end-to-end tests
10. **LRU eviction** — Bound the in-memory cache size for long-running processes

### Q3. Rationale for design choices

| Decision | Why |
|---|---|
| **SharedReadingList with Condition** | Decouples parser thread from DB writer thread; `notify_all()` wakes consumers immediately without polling |
| **Write-through (not cache-aside)** | Cache is always consistent with DB; no stale reads for NMIs just written |
| **`ON CONFLICT DO NOTHING`** | Re-running the same file is safe; the `(nmi, timestamp)` constraint is the idempotency key |
| **Filename validation** | NEM12 spec §3.2.2 defines the naming convention as part of the file delivery standard; rejecting non-conformant filenames catches delivery errors early |
| **`ensure_schema()` non-destructive** | Safe to call on every start; never drops or truncates; returns `bool` so callers know if DDL ran |
| **Polymorphic parsers** | `ParserFactory.register()` adds NEM13/MSATS/CIM parsers without modifying the agent |
| **RLock for cache** | Re-entrant lock prevents deadlock when public methods call each other; released during network I/O |

---

## Observability

- **Structured logs** — JSON via structlog; every tool call, batch, and error carries bound context
- **Prometheus metrics** — expose at `:METRICS_PORT/metrics` when `METRICS_PORT` > 0:
  - `meter_files_processed_total`
  - `meter_readings_parsed_total`
  - `meter_readings_inserted_total`, `_skipped_total`, `_failed_total`
  - `meter_parse_duration_seconds` (histogram)
  - `meter_db_write_duration_seconds` (histogram)
  - `meter_active_sessions` (gauge)
- **Background MetricsCollector** — logs a snapshot every 15 s; daemon thread, cleaned up on exit

---

## Future Work

### NEM13 (accumulation meters)

| Record | Purpose |
|---|---|
| `100` | File header (`NEM13`) |
| `250` | NMI block (NMI, register, UOM, meter serial) |
| `350` | Register read (previous + current readings, dates, read type) |
| `550` | B2B details |
| `900` | End of file |

Consumption derivation:
```
consumption = (current_read − previous_read) × multiplier
```

Implementation plan:
1. `NEM13Parser.parse()` in `src/parsers/nem13_parser.py`
2. `NEM13Block` / `NEM13RegisterRead` models in `src/models/`
3. Register `NEM13Parser` in `ParserFactory._REGISTRY`
4. Tests in `tests/test_nem13_parser.py`
