# Flo Energy — NEM12/NEM13 Meter Reading ETL

Processes Australian energy meter data files (NEM12/NEM13) and stores readings
in PostgreSQL. Powered by a **LangGraph async agent** (Claude Opus) that
orchestrates parsing, validation, and database writes — concurrently, with ACID
guarantees.

---

## How it works

```
File(s) on disk
     │
     ▼
LangChainMeterAgent  ←── Claude Opus (LangGraph ReAct loop)
     │
     ├── read_file          detect NEM12 or NEM13 format
     ├── parse_file         stream all interval readings
     ├── validate_data  ┐   run in PARALLEL (asyncio.gather)
     ├── generate_sql   ┘
     ├── write_to_database  single PostgreSQL transaction (ACID)
     └── send_notification  report outcome
```

Multiple files are processed **concurrently** — each file gets its own agent
loop running in `asyncio.gather`.

---

## Quick start

```bash
# 1. Install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Start PostgreSQL
docker compose up postgres -d

# 3. Set your API key in .env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> .env

# 4. Run the agent
python main.py data/nem12#assessment001#UNITEDDP#NEMMCO.csv

# Process multiple files in parallel
python main.py data/file1.csv data/file2.csv data/file3.csv
```

---

## Configuration

Edit `.env` in the project root. All settings have working defaults for local
development.

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | _(required)_ | Claude API key |
| `PGHOST` | `localhost` | PostgreSQL host |
| `PGPORT` | `5432` | PostgreSQL port |
| `PGDATABASE` | `meter_db` | Database name |
| `PGUSER` | `postgres` | DB user |
| `PGPASSWORD` | `postgres` | DB password |
| `DATABASE_URL` | _(assembled from PG* vars)_ | Full DSN — overrides PG* vars |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `LOG_FORMAT` | `console` | `console` (human) or `json` (machine) |
| `ENABLE_SQL_OUTPUT` | `false` | Also write SQL to a file |
| `SQL_OUTPUT_PATH` | `output/meter_readings.sql` | SQL output path |
| `METRICS_PORT` | `0` | Prometheus `/metrics` port (0 = off) |

---

## Usage

```bash
# Single file
python main.py data/sample_nem12.csv

# Multiple files (processed in parallel)
python main.py data/file1.csv data/file2.csv

# Also save SQL to a file
python main.py data/sample_nem12.csv --output output/inserts.sql
```

For the lightweight runner (no AI, no API key needed):

```bash
python process_nem12.py "data/nem12#assessment001#UNITEDDP#NEMMCO.csv"
```

---

## Docker

```bash
# Start database only
docker compose up postgres -d

# Run agent via Compose
docker compose run --rm \
  -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  agent /data/sample_nem12.csv
```

---

## Architecture

### Async & parallel execution

- `main.py` calls `asyncio.run()` — single event loop, no threads needed at the top level.
- `LangChainMeterAgent.process_files()` uses `asyncio.gather()` — N files processed concurrently.
- Each `@tool` function is `async`. Blocking calls (parsing, DB) run in a `ThreadPoolExecutor` via `run_in_executor` to keep the event loop free.
- After `parse_file`, the LLM emits `validate_data` + `generate_sql` as a parallel tool call — LangGraph executes them concurrently.

### Race conditions

- Module-level `asyncio.Lock` in `langchain_tools.py` guards the session dict — no two coroutines mutate the same session state simultaneously.
- The PostgreSQL connection pool (`ThreadedConnectionPool`) is thread-safe for concurrent batch workers.
- `ON CONFLICT DO NOTHING` on `(nmi, timestamp)` makes concurrent inserts idempotent.

### ACID properties

| Property | How it's enforced |
|---|---|
| **Atomicity** | `atomic_bulk_insert` wraps all batches in **one** `psycopg2` connection+transaction. All commit together or all roll back. |
| **Consistency** | `UNIQUE(nmi, timestamp)` DB constraint + model-level validation (no negative consumption). |
| **Isolation** | PostgreSQL default `READ COMMITTED` isolation. Each file's transaction is independent. |
| **Durability** | PostgreSQL WAL — once committed, data survives crashes. |

### Database schema

```sql
CREATE TABLE meter_readings (
    id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    nmi         VARCHAR(10) NOT NULL,
    "timestamp" TIMESTAMP  NOT NULL,
    consumption NUMERIC    NOT NULL,
    CONSTRAINT meter_readings_unique_consumption UNIQUE (nmi, "timestamp")
);
```

---

## Project structure

```
flo-energy/
├── src/
│   ├── config.py                    pydantic-settings (.env → typed config)
│   ├── models/meter_reading.py      MeterReading, ParseSession, ValidationResult
│   ├── parsers/
│   │   ├── nem12_parser.py          Streaming NEM12 CSV parser
│   │   ├── nem13_parser.py          NEM13 stub
│   │   └── parser_factory.py        Auto-detect format
│   ├── database/postgres_handler.py Thread-safe pool, bulk_insert, atomic_bulk_insert
│   ├── services/
│   │   ├── nem12_processor.py       Background thread queue + SharedReadingList
│   │   └── database_cache_service.py Write-through in-memory cache
│   ├── notifications/               Log / console / webhook handlers
│   ├── observability/metrics.py     structlog + Prometheus
│   └── agent/
│       ├── langchain_tools.py       Async @tool functions (LangChain)
│       ├── langchain_agent.py       LangGraph ReAct agent (primary)
│       └── meter_reading_agent.py   Legacy raw-Anthropic agent (kept for reference)
├── tests/                           pytest unit tests
├── data/                            Sample NEM12 files
├── main.py                          Entry point (async, LangGraph agent)
├── process_nem12.py                 Lightweight runner (no AI required)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env                             All configuration lives here
```

---

## Running tests

```bash
pytest                              # all tests
pytest tests/test_nem12_parser.py   # single module
pytest --cov=src                    # with coverage
```

---

## NEM12 file naming

Files must follow the NEM12 spec naming convention:

```
NEM12#<UniqueID>#<From>#<To>.csv   (or .zip)
```

Example: `nem12#assessment001#UNITEDDP#NEMMCO.csv`

The lightweight runner (`process_nem12.py`) validates this convention. Use
`--no-validate-filename` to skip it for ad-hoc files.
