# Flo Energy – Meter Reading ETL Agent

An Agentic AI solution that processes NEM12 (and stub for NEM13) electricity
meter data files and inserts the interval readings into a PostgreSQL database.

Built with **Python 3.12**, **Claude Opus 4.6** (Anthropic API), **psycopg2**,
and packaged as a **Docker** container.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│  MeterReadingAgent  (Claude Opus 4.6 – adaptive thinking)    │
│                                                              │
│  System prompt  ─▶  Agentic loop                            │
│                          │                                  │
│                    tool_use blocks                          │
│                          │                                  │
│              ┌───────────▼──────────────┐                   │
│              │      ToolRegistry        │                   │
│              │                          │                   │
│  read_file ──┤  parse_file             ├── NEM12Parser      │
│  validate  ──┤  generate_sql          ├── PostgresHandler   │
│  write_db ───┤  send_notification     ├── NotificationSvc   │
│  get_stats ──┤  get_processing_stats  ├── MetricsCollector  │
│              └──────────────────────────┘                   │
└──────────────────────────────────────────────────────────────┘
```

### Tool Sequence (orchestrated by the AI agent)

```
read_file → parse_file → validate_data → generate_sql
         → write_to_database → send_notification
```

Each tool is:
- **Declared** as a JSON schema (sent to the Claude API)
- **Implemented** as a Python callable in `ToolRegistry`
- **Reusable** by any agent or workflow that imports `ToolRegistry`

---

## Project Structure

```
flo-energy/
├── src/
│   ├── models/
│   │   └── meter_reading.py        # MeterReading, ParseSession, etc.
│   ├── parsers/
│   │   ├── base_parser.py          # Abstract base (polymorphism)
│   │   ├── nem12_parser.py         # NEM12 streaming parser
│   │   ├── nem13_parser.py         # NEM13 stub
│   │   └── parser_factory.py       # Format auto-detection
│   ├── database/
│   │   └── postgres_handler.py     # ThreadedConnectionPool + batch inserts
│   ├── notifications/
│   │   └── notification_service.py # Pluggable notification handlers
│   ├── observability/
│   │   └── metrics.py              # structlog + Prometheus
│   └── agent/
│       ├── tools.py                # Tool schemas + ToolRegistry
│       └── meter_reading_agent.py  # Claude agentic loop
├── tests/                          # pytest unit tests
├── data/
│   └── sample_nem12.csv            # Example NEM12 file
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── main.py
└── README.md
```

---

## Q&A – Design Decisions

### Q1. Rationale for technology choices

| Technology | Rationale |
|---|---|
| **Python 3.12** | Rich ecosystem for data processing; `dataclasses`, `decimal`, `csv` built-in; strong async/threading support |
| **Claude Opus 4.6** | Most capable model for complex multi-step orchestration; adaptive thinking handles ambiguous edge cases; tool_use API maps naturally to the ETL pipeline steps |
| **Anthropic SDK (manual loop)** | Fine-grained control over each iteration: custom logging, error injection, partial-success handling, without giving up tool calling power |
| **psycopg2 + ThreadedConnectionPool** | Battle-tested, efficient; `execute_values` for bulk inserts (10–100× faster than row-by-row); `ON CONFLICT DO NOTHING` makes the pipeline idempotent |
| **Pydantic / dataclasses** | Strong data validation at model boundaries; `slots=True` for memory efficiency with millions of readings |
| **structlog** | JSON-structured logs with bound context; integrates with standard `logging`; zero-overhead when log level is above threshold |
| **prometheus_client** | Industry-standard metrics exposition; background thread publishes `/metrics` without blocking the ETL |
| **tenacity** | Declarative retry logic for transient DB failures; exponential backoff prevents thundering herd |
| **ThreadPoolExecutor** | Parallel batch DB writes saturate connection pool; CPU-bound parsing remains on main thread |
| **Generator-based parsing** | `stream_readings()` processes arbitrarily large files (GB+) with O(batch_size) memory |

### Q2. What I would do differently with more time

1. **Async pipeline** – Replace `ThreadPoolExecutor` with `asyncio` + `asyncpg` for higher concurrency and lower overhead on I/O-bound DB writes.

2. **Redis session store** – Replace the in-memory `_sessions` dict with Redis so that multiple agent instances can share state, enabling horizontal scaling.

3. **Scheduled file ingestion** – Add a watcher thread (via `watchdog`) that monitors an S3 bucket or local directory and triggers the agent when new files arrive (the NEM12 spec defines a scheduled read date in the 200 record).

4. **Full NEM13 support** – Implement the accumulation-to-interval conversion logic (previous/current reads → derived consumption per time bucket).

5. **400-record quality overrides** – The NEM12 400 record allows per-interval quality/method overrides within a 300 record; currently skipped.

6. **Schema migrations** – Manage DB schema with Alembic instead of `ensure_schema()` one-off DDL.

7. **Retry queue** – Failed batches are counted but not re-queued; a dead-letter queue (Celery / SQS) would allow retrying without reprocessing the whole file.

8. **Integration tests** – Add pytest fixtures that spin up a real PostgreSQL container (via `testcontainers`) to validate end-to-end behaviour.

9. **OpenTelemetry traces** – Replace ad-hoc timing with OTEL spans so the full pipeline is traceable in Jaeger/Tempo.

10. **Multi-file concurrency** – Allow the agent to process a directory of files by spawning one session per file in a `ProcessPoolExecutor`.

### Q3. Rationale for design choices

| Decision | Why |
|---|---|
| **Manual agentic loop** | The agent retries on partial failure (e.g. DB down during writes) without restarting the parse step – impossible with a fire-and-forget tool runner |
| **Polymorphic parsers** | `ParserFactory.register()` lets users drop in new formats (NEM13, MSATS, CIM XML) at runtime without modifying the agent |
| **Session state dict** | Allows tools to pass large datasets (list of MeterReading objects) between steps without serialising to JSON in the tool result, which would bloat the Claude context window |
| **Adaptive thinking** | Claude can reason about malformed files, partial data, and ambiguous quality flags without being told every edge case upfront |
| **ON CONFLICT DO NOTHING** | Makes re-running on the same file safe; the `meter_readings_unique_consumption` constraint on `(nmi, timestamp)` is the idempotency key |
| **Background MetricsCollector thread** | Observability does not block the ETL path; daemon=True ensures it is killed when main thread exits |
| **Notification handler chain** | Each handler can fail independently without breaking others; new channels (PagerDuty, email) are added by subclassing `NotificationHandler` |

---

## Local Setup (without Docker)

### Prerequisites

- Python 3.12+
- PostgreSQL 15+ (local or remote)
- Anthropic API key (optional – use `--no-agent` to skip)

### Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Configure environment

```bash
cp .env.example .env
# Edit .env with your values
source .env  # or use direnv / python-dotenv
```

### Run tests

```bash
pytest                             # all tests with coverage
pytest tests/test_nem12_parser.py  # specific module
pytest -x                          # stop on first failure
```

### Run the agent (AI-powered)

```bash
# Set ANTHROPIC_API_KEY and DATABASE_URL in .env first
python main.py data/sample_nem12.csv
```

### Run without the AI agent (direct pipeline)

```bash
# No API key needed
python main.py data/sample_nem12.csv --no-agent

# Generate SQL only (no DB write)
python main.py data/sample_nem12.csv --no-agent --sql-only --output out.sql
```

---

## Docker Build and Run

### Build the image

```bash
docker build -t flo-energy-agent .
```

### Run with Docker (direct pipeline, no API key)

```bash
docker run --rm \
  -v $(pwd)/data:/data:ro \
  -e DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5432/meter_db \
  flo-energy-agent /data/sample_nem12.csv --no-agent
```

### Run with Docker (AI agent)

```bash
docker run --rm \
  -v $(pwd)/data:/data:ro \
  -v $(pwd)/output:/output \
  -e ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
  -e DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5432/meter_db \
  flo-energy-agent /data/sample_nem12.csv
```

---

## Docker Compose (full stack)

```bash
# 1. Start PostgreSQL
docker compose up postgres -d

# 2. Wait for healthcheck, then run the agent
docker compose run agent /data/sample_nem12.csv --no-agent

# 3. Run with AI agent (requires ANTHROPIC_API_KEY in .env)
docker compose run \
  -e ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
  agent /data/sample_nem12.csv

# 4. Tear down
docker compose down -v
```

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `ANTHROPIC_API_KEY` | Yes (agent mode) | – | Claude API key |
| `DATABASE_URL` | Yes | – | PostgreSQL DSN |
| `PGHOST` | Alt to DATABASE_URL | localhost | |
| `PGPORT` | Alt to DATABASE_URL | 5432 | |
| `PGDATABASE` | Alt to DATABASE_URL | meter_db | |
| `PGUSER` | Alt to DATABASE_URL | postgres | |
| `PGPASSWORD` | Alt to DATABASE_URL | postgres | |
| `LOG_LEVEL` | No | INFO | DEBUG/INFO/WARNING/ERROR |
| `LOG_FORMAT` | No | json | json/console |
| `ENABLE_SQL_OUTPUT` | No | false | Write SQL to file |
| `SQL_OUTPUT_PATH` | No | – | Path for SQL output |
| `METRICS_PORT` | No | 0 (off) | Prometheus port |

---

## Database Schema

```sql
CREATE TABLE meter_readings (
    id          UUID DEFAULT gen_random_uuid() NOT NULL,
    nmi         VARCHAR(10) NOT NULL,
    "timestamp" TIMESTAMP  NOT NULL,
    consumption NUMERIC    NOT NULL,
    CONSTRAINT meter_readings_pk PRIMARY KEY (id),
    CONSTRAINT meter_readings_unique_consumption UNIQUE (nmi, "timestamp")
);
```

---

## NEM12 Format Reference

| Record | Purpose | Key fields used |
|---|---|---|
| 100 | File header | Version identifier (`NEM12`) |
| 200 | NMI block header | NMI (field 2), interval length in minutes (field 9) |
| 300 | Interval data | Date (field 2), N consumption values (fields 3..N+2), quality flag |
| 400 | Quality overrides | Skipped (future work) |
| 500 | B2B details | Skipped |
| 900 | End of file | Terminates parsing |

**Timestamp convention**: NEM12 uses *interval-ending* timestamps.
Interval `i` (1-indexed) on date `D` with length `L` minutes:

```
timestamp_i = D + i × L minutes
```

Example (30 min, 2005-03-01):
- Interval 1 → `2005-03-01 00:30:00`
- Interval 48 → `2005-03-02 00:00:00`

---

## Future Work

### NEM13 Support

NEM13 covers **accumulation (non-interval) meters** – a single cumulative
reading per register per read date instead of many interval values.

**Key record types to implement:**

| Record | Purpose |
|---|---|
| 100 | File header (version `NEM13`) |
| 250 | NMI block header (NMI, register, UOM, meter serial) |
| 350 | Register read (previous + current readings, dates, read type) |
| 550 | B2B details |
| 900 | End of file |

**Consumption derivation:**
```
consumption = (current_read - previous_read) × multiplier
```
adjusted for meter roll-overs and read-type qualifiers (Actual/Estimated/Substituted).

Each `(nmi, interval_start, interval_end)` tuple maps to one row in
`meter_readings` with `timestamp = interval_end`.

**Implementation plan:**
1. Add `NEM13Parser.parse()` and `stream_readings()` in `src/parsers/nem13_parser.py`
2. Add `NEM13Block` / `NEM13RegisterRead` models to `src/models/`
3. Register `NEM13Parser` in `ParserFactory._REGISTRY`
4. Add integration tests in `tests/test_nem13_parser.py`

### Other Improvements

- MSATS/CIM XML format parsers via the same `BaseParser` interface
- Async pipeline with `asyncpg` for higher throughput
- Redis-backed session store for horizontal scaling
- Alembic database migrations
- OpenTelemetry distributed tracing
- Dead-letter queue for failed batches
- Watchdog-based scheduled file ingestion (honour the 200 record's `NextScheduledReadDate`)
- Full 400-record quality-override support

---

## Observability

- **Structured logs** (JSON via structlog) – every tool call, DB batch, and error is logged with context
- **Prometheus metrics** – exposed at `:8000/metrics` when `METRICS_PORT=8000`
  - `meter_files_processed_total`
  - `meter_readings_parsed_total`
  - `meter_readings_inserted_total`
  - `meter_readings_skipped_total`
  - `meter_readings_failed_total`
  - `meter_parse_duration_seconds`
  - `meter_db_write_duration_seconds`
- **Background thread** – `MetricsCollector` logs a snapshot every 15 seconds
