# Flo Energy — NEM12/NEM13 Meter Reading ETL

Takes Australian energy meter data files (NEM12/NEM13), parses every interval reading, validates it, and stores it in PostgreSQL — driven by a **LangGraph async agent** running on Claude Opus.

The agent figures out what to do. You just point it at a file.

---

## How it works

```
You: python main.py data/file.csv
        │
        ▼
  LangChainMeterAgent            ← asyncio event loop
        │
        ├─ read_file             detect NEM12 or NEM13 format
        ├─ parse_file            stream all interval readings line-by-line
        ├─ validate_data  ──┐    run in parallel (single LLM turn)
        ├─ generate_sql   ──┘
        ├─ write_to_database     auto-creates table if missing; one transaction
        └─ send_notification     log final outcome

Multiple files → asyncio.gather() → each file gets its own agent loop, all run concurrently
```

---

## Modules and how they connect

### `main.py`
Entry point. Parses CLI args, calls `asyncio.run(_run(file_paths))`. That's it — one function, no flags, no modes. Multiple files are passed straight to `process_files()`.

### `src/agent/langchain_agent.py`
The brain. Wraps `langgraph.prebuilt.create_react_agent` with `ChatAnthropic`. Exposes two methods:
- `process_file(path)` — single file, awaitable
- `process_files(paths)` — calls `asyncio.gather()` so N files run simultaneously, failures are isolated per file

### `src/agent/langchain_tools.py`
The hands. Seven `async @tool` functions that LangGraph calls when the model decides to act:
`read_file → parse_file → validate_data + generate_sql → write_to_database → send_notification`

All session state lives in a dict guarded by `asyncio.Lock` so concurrent file runs never stomp on each other. Blocking work (I/O, DB) is offloaded to `ThreadPoolExecutor` via `run_in_executor`.

### `src/parsers/`
`NEM12Parser` and `NEM13Parser` — pure parsing logic, no side effects. `ParserFactory` auto-detects the format from the file header. The `parse_file` tool calls these.

### `src/database/postgres_handler.py`
Manages a `ThreadedConnectionPool`. Key methods:
- `ensure_schema()` — `CREATE TABLE IF NOT EXISTS`, safe to call any time
- `atomic_bulk_insert()` — wraps every batch in **one** connection+transaction (ACID atomicity)
- `bulk_insert()` — threaded parallel batches, used by other services
- `generate_insert_sql()` — dry-run, no DB needed

`write_to_database` tool calls `ensure_schema()` first, then `atomic_bulk_insert()`. The table is created automatically on first run. If the table already exists, `ensure_schema()` is a no-op.

### `src/services/nem12_processor.py`
Background thread + queue + `SharedReadingList`. Used by `process_nem12.py` (the lightweight runner). Decouples the producer (file parsing) from the consumer (DB writes) using `threading.Condition`.

### `src/services/database_cache_service.py`
Write-through in-memory cache over PostgreSQL. Stores readings as `{(nmi, ts): MeterReading}` with a sorted timestamp index per NMI for fast range queries. Cache and DB stay consistent — if the DB write fails, the cache is not updated.

### `src/config.py`
Single `pydantic-settings` `Settings` class. Reads `.env` first, falls back to OS env vars. Everything in the app imports `from src.config import settings` — no `os.environ` calls scattered around.

### `src/observability/metrics.py`
`structlog` for structured logging + optional Prometheus `/metrics` endpoint. `MetricsCollector` runs as a background daemon thread.

### `src/notifications/notification_service.py`
Lightweight notification dispatcher. Logs to console by default; can post to a webhook URL (`NOTIFICATION_WEBHOOK_URL`). The `send_notification` tool calls this at the end of every file.

---

## Quick start (local)

```bash
# 1. Create virtual env and install deps
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Start PostgreSQL (Docker)
docker compose up postgres -d

# 3. Add your API key to .env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> .env

# 4. Run a CSV file
python main.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv"

# Run a ZIP file (contains a NEM12 CSV inside)
python main.py "data/nem12#0123456789ABCDEF#TESTMDP1#TESTRETAIL.zip"

# Multiple files at once — CSV and ZIP can be mixed (run in parallel)
python main.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv" "data/nem12#0123456789ABCDEF#TESTMDP1#TESTRETAIL.zip"

# Save SQL output too
python main.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv" --output output/inserts.sql
python main.py "data/nem12#0123456789ABCDEF#TESTMDP1#TESTRETAIL.zip" --output output/inserts.sql
```

No need to create the database table — it's created automatically on first run.

---

## Quick start (Docker)

```bash
# 1. Start Postgres
docker compose up postgres -d

# 2. Run the agent against a CSV file
docker compose run --rm \
  -e ANTHROPIC_API_KEY="sk-ant-..." \
  agent "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv"

# 3. Run the agent against a ZIP file (NEM12 CSV inside)
docker compose run --rm \
  -e ANTHROPIC_API_KEY="sk-ant-..." \
  agent "/data/nem12#0123456789ABCDEF#TESTMDP1#TESTRETAIL.zip"

# 4. Multiple files — CSV and ZIP can be mixed
docker compose run --rm \
  -e ANTHROPIC_API_KEY="sk-ant-..." \
  agent "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv" "/data/nem12#0123456789ABCDEF#TESTMDP1#TESTRETAIL.zip"

# 5. Save SQL output
docker compose run --rm \
  -e ANTHROPIC_API_KEY="sk-ant-..." \
  agent "/data/nem12#assessment001#UNITEDDP#NEMMCO.csv" --output /output/inserts.sql

# 6. Lightweight runner (no API key needed)
docker compose run --rm agent python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv"
```

---

## Configuration (`.env`)

| Variable | Default | What it does |
|---|---|---|
| `ANTHROPIC_API_KEY` | _(required)_ | Claude API key |
| `PGHOST` | `localhost` | Postgres host |
| `PGPORT` | `5432` | Postgres port |
| `PGDATABASE` | `meter_db` | Database name |
| `PGUSER` | `postgres` | DB user |
| `PGPASSWORD` | `postgres` | DB password |
| `DATABASE_URL` | _(assembled from above)_ | Full DSN — overrides all PG* vars |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `LOG_FORMAT` | `console` | `console` (human-readable) or `json` (structured) |
| `ENABLE_SQL_OUTPUT` | `false` | Write SQL to file in addition to DB insert |
| `SQL_OUTPUT_PATH` | `output/meter_readings.sql` | Where to write SQL |
| `METRICS_PORT` | `0` | Prometheus port (`0` = disabled) |

---

## ACID guarantees

Every file is written in a **single PostgreSQL transaction**:

| Property | How |
|---|---|
| **Atomicity** | All batches share one connection. Everything commits or everything rolls back. |
| **Consistency** | `UNIQUE(nmi, timestamp)` DB constraint. Model rejects negative consumption. |
| **Isolation** | Each file is its own transaction. Concurrent files don't interfere. |
| **Durability** | PostgreSQL WAL. Committed data survives crashes. |

---

## Race condition safety

- `asyncio.Lock` guards the session dict — two coroutines can't mutate the same session simultaneously
- `ON CONFLICT DO NOTHING` makes inserts idempotent — safe to retry or run twice
- `ThreadedConnectionPool` is thread-safe for concurrent batch workers inside `run_in_executor`

---

## Running tests

```bash
pytest                              # all tests
pytest tests/test_nem12_parser.py   # one module
pytest --cov=src                    # with coverage report
```

---

## Project structure

```
flo-energy/
├── main.py                          async entry point (LangGraph agent)
├── process_nem12.py                 lightweight runner, no API key needed
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .env                             all config lives here
├── data/                            sample NEM12 files
├── output/                          SQL output (created on demand)
└── src/
    ├── config.py                    pydantic-settings singleton
    ├── models/meter_reading.py      MeterReading, ParseSession, ValidationResult
    ├── parsers/
    │   ├── nem12_parser.py          streaming NEM12 CSV parser
    │   ├── nem13_parser.py          NEM13 parser
    │   └── parser_factory.py        auto-detects format from file header
    ├── database/
    │   └── postgres_handler.py      connection pool, ensure_schema, atomic_bulk_insert
    ├── services/
    │   ├── nem12_processor.py       thread-queue pipeline + SharedReadingList
    │   └── database_cache_service.py write-through in-memory cache
    ├── notifications/
    │   └── notification_service.py  console + webhook dispatcher
    ├── observability/
    │   └── metrics.py               structlog + Prometheus
    └── agent/
        ├── langchain_tools.py       async @tool functions — CSV and ZIP input supported
        └── langchain_agent.py       LangGraph ReAct agent, asyncio.gather
```
