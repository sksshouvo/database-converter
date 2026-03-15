# Universal Database Converter CLI

A production-grade, modular console application that migrates databases between **MySQL**, **MSSQL**, and **PostgreSQL** with scheduling, parallel processing, resume capability, and structured logging.

---

## Features

- 🔌 **Multi-engine support** — MySQL, MSSQL, PostgreSQL
- 🗺️ **Schema mapping** — automatic type conversion across engines
- 🔗 **Dependency resolution** — FK-aware topological table ordering
- ⚡ **Parallel migration** — concurrent table processing via `ThreadPoolExecutor`
- ♻️ **Resume capability** — `last_processed_id` tracked per batch in SQLite
- 🕐 **Cron scheduling** — Linux/Mac cron + Windows Task Scheduler support
- ✅ **Post-migration validation** — row count + MD5 sample checksums
- 📋 **Structured logging** — `Rich`-formatted console + rotating file logs
- 🖥️ **Interactive CLI** — menu-driven Rich TUI

---

## Installation

```bash
git clone <repo-url>
cd database-converter
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

---

## Usage

### Interactive Menu
```bash
db-converter
# or
python cli/main.py
```

### Menu Options
1. **Create Migration** — configure source/destination connections and start a migration
2. **View Migration Jobs** — list all jobs with status and progress
3. **Schedule a Job** — set up recurring migration via cron expression
4. **Stop Scheduled Job** — disable a scheduled task
5. **View Job Status** — detailed status for a specific job
6. **Exit**

---

## Configuration

Environment variables (or `.env` file):

| Variable | Default | Description |
|---|---|---|
| `BATCH_SIZE` | `1000` | Rows per batch |
| `MAX_WORKERS` | `4` | Parallel threads |
| `LOG_LEVEL` | `INFO` | Logging level |
| `DB_PATH` | `storage/control_db.sqlite` | Control DB path |

---

## Architecture

```
cli/           — Entry point + interactive menu
services/      — Orchestration (migration, scheduler)
core/
  connectors/  — DB engine adapters (MySQL, MSSQL, PostgreSQL)
  schema_mapper/ — Type mapping, DDL generation, FK resolution
  data_migrator/ — Row streaming, transformation, parallel execution
  validators/  — Connection, schema, and post-migration data validation
  cron_manager/ — OS-aware cron/Task Scheduler integration
models/        — Dataclasses + SQLAlchemy ORM models
storage/       — SQLite control database
utils/         — Logger, config, batch processor
```

---

## Control DB Schema

SQLite database at `storage/control_db.sqlite` tracks:
- **`migration_jobs`** — per-table job with status, progress, and resume pointer
- **`scheduled_jobs`** — scheduled job configurations linked to OS cron entries

---

## Supported Type Mappings

| Source Type | MySQL Target | MSSQL Target | PostgreSQL Target |
|---|---|---|---|
| INT | INT | INT | INTEGER |
| BIGINT | BIGINT | BIGINT | BIGINT |
| VARCHAR | VARCHAR | NVARCHAR | VARCHAR |
| TEXT | LONGTEXT | NVARCHAR(MAX) | TEXT |
| DATETIME | DATETIME | DATETIME2 | TIMESTAMP |
| FLOAT | FLOAT | FLOAT | DOUBLE PRECISION |
| DECIMAL | DECIMAL | DECIMAL | NUMERIC |
| BOOLEAN | TINYINT(1) | BIT | BOOLEAN |
| BLOB | LONGBLOB | VARBINARY(MAX) | BYTEA |
| JSON | JSON | NVARCHAR(MAX) | JSONB |
