# customer_care_emails Pipelines — Stage 1 (PostgreSQL) and Stage 2 (MinIO)

This single README documents **two stages that share one repository, one Docker Compose stack and one dataset**:

| Part | What it does | Where to read |
|---|---|---|
| **Stage 1** | CSV → validate → transform → load into PostgreSQL | [Stage 1 — Local CSV to PostgreSQL with Apache Airflow](#stage-1--local-csv-to-postgresql-with-apache-airflow) (sections 1–15) |
| **Stage 2** | CSV → schema contract → partitioned Parquet in MinIO | [Stage 2 — Local CSV to Partitioned Parquet in MinIO](#stage-2--local-csv-to-partitioned-parquet-in-minio) |
| **Both** | Unit tests, in-pipeline data-quality checks, assumptions | [Testing and data quality (Stage 1 and Stage 2)](#testing-and-data-quality-stage-1-and-stage-2) |

Quick start for either stage: sections 1–5 (clone, `.env`, start the stack, open the Airflow UI). The Stage 1 DAG is `load_customer_care_emails_to_postgres`; the Stage 2 DAG is `csv_to_parquet_minio`.

## Development workflow (read this first)

The task text says to write the README and "open a PR for review". The intended method was to work on a **feature branch** and merge it into `main` through a **pull request**.

**That was not followed for Stage 1 and Stage 2.** Both stages were committed and pushed **directly to `main`**, so there is no PR history for them. This was a process mistake, not a technical one, and it is recorded here instead of hidden.

From **Stage 3 onward** the intended method is applied:

- Stage 3–6 live in a **separate repository** ([`de-clickhouse-warehouse`](https://github.com/jungmyung-lee/de-clickhouse-warehouse)), as the Task Bank requires.
- Every change is made on a branch named `type/short-description` (for example `feat/clickhouse-service`), then merged into `main` through a pull request.
- Commit messages use Conventional Commits: `feat:`, `fix:`, `docs:`, `chore:`, `test:`.
- One logical change per commit (atomic commits).

After Stage 1 and Stage 2 were pushed, every follow-up change in this repository (removing the Stage 3 files, the unit tests, the data quality checks and this README update) was made on a feature branch and merged through a pull request.

---

# Stage 1 — Local CSV to PostgreSQL with Apache Airflow

## Overview

This project implements Stage 1 of the Data Engineer Intern Task Bank.

The pipeline ingests the `customer_care_emails` CSV dataset, validates it against a schema definition, transforms the data, and loads it into a PostgreSQL table using Apache Airflow.

The entire environment runs locally with Docker Compose.

### Pipeline

```text
customer_care_emails.csv
        |
        v
     read_csv
        |
        v
  validate_schema
   (schema.yaml)
        |
        v
     transform
        |
        v
load_to_postgres
        |
        v
PostgreSQL
customer_care_emails
        |
        v
data_quality_check
(row count, duplicate keys,
 nulls, value ranges)
```

The DAG is designed to be idempotent. Re-running the pipeline does not create duplicate rows because the target table is truncated before the full dataset is loaded again.

---

## Dataset

Business users: see the [data dictionary](docs/customer_care_emails_data_dictionary.xlsx).

Dataset:

```text
customer_care_emails
```

Source:

```text
Hugging Face
rtweera/customer_care_emails
```

The dataset contains customer support email threads and metadata such as:

- sender and receiver
- email timestamp
- message body
- thread ID
- email type
- email status
- email criticality
- product type
- agent effectiveness
- agent efficiency
- customer satisfaction

Expected number of rows:

```text
2259
```

The dataset status recorded in `manifest.yaml` is:

```text
Completed/Completed
```

---

## Project Structure

```text
airflow/
├── dags/
│   ├── load_customer_care_emails_to_postgres.py   # Stage 1 DAG
│   ├── csv_to_parquet_minio.py                    # Stage 2 DAG
│   └── dq_checks.py                               # data-quality helpers (no Airflow imports)
│
├── scripts/
│   └── validate_schema.py
│
├── stage1/
│   └── customer_care_emails/
│       ├── customer_care_emails.csv
│       ├── ddl.sql
│       ├── manifest.yaml
│       └── schema.yaml
│
├── stage2/
│   └── customer_care_emails/
│       ├── manifest.yaml
│       └── schema_contract.yaml
│
├── tests/                                         # pytest unit tests (see Testing section)
├── pytest.ini
├── requirements-dev.txt
├── docker-compose.yaml
├── .env.example
├── .gitignore
└── README.md
```

### File responsibilities

`manifest.yaml`

Describes the dataset, source, owner, dataset code, analysis status, and target system.

`schema.yaml`

Defines the expected CSV structure including:

- column names
- data types
- nullability
- allowed values
- ranges
- composite primary key

`ddl.sql`

Creates the target PostgreSQL table and its constraints.

`load_customer_care_emails_to_postgres.py`

Defines the Airflow DAG that performs the ingestion pipeline.

`docker-compose.yaml`

Runs Apache Airflow, Redis, the Airflow metadata PostgreSQL database, and a separate target PostgreSQL database.

`.env.example`

Documents the environment variables required by Docker Compose without storing real credentials.

---

## Architecture

Two PostgreSQL services are used.

```text
                    Docker Compose
                          |
          +---------------+---------------+
          |                               |
          v                               v
   Airflow metadata DB              Target PostgreSQL
       `postgres`                   `target-postgres`
          |                               |
   Airflow internal state          customer_care_emails
   DAG runs / task state                table
```

The Airflow metadata database and the Stage 1 target database are intentionally separated.

Airflow connects to the target PostgreSQL service through the connection:

```text
target_postgres
```

The connection is configured through the environment variable:

```text
AIRFLOW_CONN_TARGET_POSTGRES
```

The DAG accesses it using:

```python
PostgresHook(postgres_conn_id="target_postgres")
```

No PostgreSQL credentials are hardcoded inside the DAG.

---

## Prerequisites

The following tools are required:

- Docker
- Docker Compose
- Git

This project was tested with:

```text
Apache Airflow 3.3.2
PostgreSQL 16
Redis 7.2
```

Python is used inside the Airflow containers for the pipeline.

---

## 1. Clone the Repository

```bash
git clone https://github.com/jungmyung-lee/airflow.git
cd airflow
```

---

## 2. Configure Environment Variables

Copy the provided environment template:

```bash
cp .env.example .env
```

The `.env` file should contain values for:

```text
AIRFLOW_UID
TARGET_POSTGRES_USER
TARGET_POSTGRES_PASSWORD
TARGET_POSTGRES_DB
FERNET_KEY
```

Example:

```text
AIRFLOW_UID=501

TARGET_POSTGRES_USER=stage1_user
TARGET_POSTGRES_PASSWORD=your_password
TARGET_POSTGRES_DB=stage1

FERNET_KEY=your_fernet_key
```

Do not commit the real `.env` file.

Generate a Fernet key with Docker (no local Python needed):

```bash
docker run --rm apache/airflow:3.3.2 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the generated value into:

```text
FERNET_KEY=
```

The final `=` character in a generated Fernet key is part of the key and should not be removed.

---

## 3. Validate the Docker Compose Configuration

Before starting the environment:

```bash
docker compose config --quiet
```

If the command produces no output, the Compose configuration is valid.

---

## 4. Start the Environment

Run:

```bash
docker compose up -d
```

Docker Compose starts:

```text
Airflow API server
Airflow scheduler
Airflow worker
Airflow DAG processor
Airflow triggerer
Airflow initialization container
Redis
Airflow metadata PostgreSQL
Target PostgreSQL
```

Check container status:

```bash
docker ps
```

The long-running Airflow, Redis, and PostgreSQL containers should eventually report a healthy status.

The `airflow-init` container may show:

```text
Exited (0)
```

This is normal because it performs initialization and then exits.

---

## 5. Open the Airflow UI

Open:

```text
http://localhost:8080
```

Log in with the default account created by `airflow-init`:

```text
username: airflow
password: airflow
```

To use different values, set `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` in `.env` before the first start. If the login page rejects the defaults, check the generated credentials with `docker compose logs airflow-apiserver`.

The DAG should appear as:

```text
load_customer_care_emails_to_postgres
```

---

## 6. Pipeline Tasks

The DAG has five tasks: `read_csv` → `validate_schema` → `transform` → `load_to_postgres` → `data_quality_check`.

The tasks run in this order:

```text
read_csv
    ↓
validate_schema
    ↓
transform
    ↓
load_to_postgres
    ↓
data_quality_check
```

### `read_csv`

Reads:

```text
customer_care_emails.csv
manifest.yaml
```

The task verifies that the dataset status is ready for processing.
 
The dataset status is read from `manifest.yaml`. It can be one word (`Done`) or two words for the first and second review (`Pending/Completed`, `Completed/Completed`). The last word is the current state: the dataset is ready only when it is `Completed` or `Done`. Letter case and surrounding spaces are ignored. Anything else, for example `pending` or `Completed/Pending`, stops the run before the CSV is read.

### `validate_schema`

Validates the incoming CSV against `schema.yaml`.

Checks include:

- exact column names
- column order
- required non-null values
- timestamp parsing
- numeric conversion
- allowed categorical values
- customer satisfaction range
- composite primary-key uniqueness

The composite primary key is:

```text
thread_id
timestamp
subject
```

If the incoming CSV does not conform to the schema, the DAG fails at this task.

### `transform`

Performs basic cleaning and normalization.

Examples include:

- trimming string values
- converting timestamps to UTC
- converting customer satisfaction to numeric values

The task creates an intermediate cleaned CSV during execution.

### `load_to_postgres`

The task:

1. connects to `target-postgres`
2. executes `ddl.sql`
3. creates the table if it does not already exist
4. truncates the existing target table
5. inserts the cleaned dataset

This truncate-and-reload strategy makes repeated DAG runs idempotent.

---

### `data_quality_check`

Runs after the load and checks the rows that are really in PostgreSQL, not the file. Details are in [Data quality checks](#data-quality-checks-in-the-pipeline).

---

## 7. Trigger the DAG

In the Airflow UI:

1. Open `load_customer_care_emails_to_postgres`.
2. Trigger a new DAG run.
3. Wait for all five tasks to complete.

A successful run should show:

```text
read_csv            Success
validate_schema     Success
transform           Success
load_to_postgres    Success
data_quality_check  Success
```

---

## 8. Verify the PostgreSQL Data

Open a `psql` session in the target PostgreSQL service (the service name does not depend on your folder name; use the user and database from your `.env`):

```bash
docker compose exec target-postgres psql -U stage1_user -d stage1
```

Check the table:

```sql
\dt
```

Verify the number of rows:

```sql
SELECT COUNT(*) FROM customer_care_emails;
```

Expected result:

```text
2259
```

Exit PostgreSQL:

```text
\q
```

Then exit the container:

```bash
exit
```

Alternatively, the row count can be checked directly:

```bash
docker compose exec target-postgres \
  psql -U stage1_user -d stage1 \
  -c "SELECT COUNT(*) FROM customer_care_emails;"
```

---

## 9. Idempotency Test

Trigger the DAG once and verify:

```text
2259 rows
```

Trigger the same DAG again.

Then run:

```sql
SELECT COUNT(*) FROM customer_care_emails;
```

The expected result should still be:

```text
2259
```

The row count should not become:

```text
4518
```

because the target table is truncated before every full reload.

This confirms that repeated pipeline runs do not create duplicate records.

---

## 10. Schema Validation Failure Test

The validation task is designed to stop malformed input before any data is loaded into PostgreSQL.

For testing, a deliberately malformed CSV was created by removing a required column.

For example, removing:

```text
receiver
```

causes the incoming CSV structure to no longer match `schema.yaml`.

The expected DAG result is:

```text
read_csv             Success
validate_schema      Failed
transform            Not executed
load_to_postgres     Not executed
```

This verifies that invalid input cannot continue to the transformation or loading stages.

Temporary malformed test CSV files should not be committed to the repository.

---

## 11. Database Schema

The PostgreSQL target table is:

```text
customer_care_emails
```

The table includes constraints for:

- composite primary key
- allowed email statuses
- allowed criticality values
- allowed agent effectiveness values
- allowed agent efficiency values
- customer satisfaction range

The DDL is stored in:

```text
stage1/customer_care_emails/ddl.sql
```

The DAG executes this DDL automatically, so the table does not need to be manually created before running the pipeline.

---

## 12. Docker Volumes

PostgreSQL data is stored in Docker named volumes.

The Compose environment uses separate volumes for:

```text
Airflow metadata PostgreSQL
Target PostgreSQL
```

This allows database data to survive normal container recreation.

Running:

```bash
docker compose down
```

removes the containers and network but preserves the named volumes.

Be careful with:

```bash
docker compose down -v
```

because `-v` removes named volumes and therefore deletes the persisted PostgreSQL data.

The environment can still be rebuilt from scratch because the DAG automatically recreates the target table and reloads the CSV.

---

## 13. Bind Mounts

The Compose file bind-mounts project directories into the Airflow containers.

Examples:

```text
./dags
→ /opt/airflow/dags
```

and:

```text
./stage1/customer_care_emails
→ /opt/airflow/data/customer_care_emails
```

This allows the Airflow containers to access the local DAG and dataset files.

Because these are bind mounts, local changes to the mounted files become visible inside the containers without rebuilding the Docker image.

---

## 14. Credential Management

Real credentials are stored only in:

```text
.env
```

The `.env` file is excluded from Git.

Only:

```text
.env.example
```

is committed.

The target PostgreSQL connection is created from environment variables rather than hardcoded values.

Conceptually:

```text
.env
  |
  v
docker-compose.yaml
  |
  v
AIRFLOW_CONN_TARGET_POSTGRES
  |
  v
Airflow
  |
  v
PostgresHook("target_postgres")
  |
  v
target-postgres
```

---

## 15. Files That Are Not Committed

Some files are created by Airflow, by Python, or by the pipeline itself while it runs. They are listed in `.gitignore` and are not part of the repository.

| File or folder | Why it is not committed |
|---|---|
| `.env` | Holds real passwords and keys. Only `.env.example` is committed. |
| `logs/` | Airflow task logs, created on every run. |
| `config/` | Airflow runtime configuration created by Docker. |
| `stage1/customer_care_emails/customer_care_emails_cleaned.csv` | A cleaned copy that the pipeline creates when it runs. |
| `venv/`, `.venv/` | Local Python environments. |
| `__pycache__/`, `*.pyc`, `.pytest_cache/` | Python and pytest caches. |
| `.DS_Store` | macOS folder metadata. |

**Malformed test CSV files are not committed either.** The failure tests in section 10 and in "Stage 2 Tests" use temporary malformed copies of the dataset (for example a CSV with a missing or renamed column). These copies exist only on the machine or container where the test is run and are never pushed to GitHub. To repeat a failure test, create the broken copy by following those sections.

---

## Troubleshooting

### DAG does not appear in Airflow

Check the DAG processor and scheduler:

```bash
docker ps
```

Then inspect logs if necessary:

```bash
docker compose logs airflow-dag-processor
```

or:

```bash
docker compose logs airflow-scheduler
```

Also verify that the DAG directory is mounted correctly:

```text
./dags:/opt/airflow/dags
```

---

### Environment variable warnings

If Docker Compose reports variables such as:

```text
FERNET_KEY is not set
TARGET_POSTGRES_USER is not set
TARGET_POSTGRES_PASSWORD is not set
TARGET_POSTGRES_DB is not set
```

verify that `.env` exists in the same project directory as `docker-compose.yaml`.

Run:

```bash
ls -al
```

and confirm:

```text
.env
.env.example
docker-compose.yaml
```

---

### PostgreSQL connection fails

Check the target database:

```bash
docker ps
```

`target-postgres` should be healthy.

Verify the Airflow connection from inside an Airflow container:

```bash
docker compose exec airflow-scheduler \
  airflow connections get target_postgres
```

The connection should point to:

```text
host: target-postgres
port: 5432
database: stage1
```

---

### `psql` reports that role `root` does not exist

Do not run only:

```bash
psql
```

inside the container.

The container shell user may be `root`, but the PostgreSQL database user is different.

Use:

```bash
psql -U stage1_user -d stage1
```

---

### PostgreSQL prompt changes from `stage1=#` to `stage1-#`

This means PostgreSQL is waiting for the SQL statement to finish.

For example:

```sql
SELECT COUNT(*) FROM customer_care_emails
```

does not have a semicolon.

Finish it with:

```sql
;
```

or enter the full query:

```sql
SELECT COUNT(*) FROM customer_care_emails;
```

Use:

```text
Ctrl+C
```

to cancel an unfinished statement.

---

### Python `__pycache__` directories appear

Airflow imports DAG files repeatedly, and Python may generate:

```text
__pycache__/
*.pyc
```

These are runtime cache files and are not project source code.

They should be excluded through `.gitignore`.

---

## Stopping the Environment

Stop all Compose services:

```bash
docker compose down
```

This preserves named database volumes.

To start the environment again:

```bash
docker compose up -d
```

---

## Verification Summary

The completed Stage 1 pipeline has been tested for:

- Docker Compose startup
- Airflow UI access
- DAG discovery
- CSV ingestion
- manifest status validation
- schema validation
- transformation
- PostgreSQL table creation
- PostgreSQL loading
- successful load of 2259 rows
- repeated execution without duplicate rows
- intentional malformed CSV failure at `validate_schema`
- target PostgreSQL connection through Airflow
- environment-variable-based credential handling
- clean rebuild with a fresh Docker Compose project
- unit tests for the business rules (see [Testing and data quality](#testing-and-data-quality-stage-1-and-stage-2))
- in-pipeline data-quality checks after the load

---

## Stage 1 Status

```text
Story 1 — Dataset manifest, schema and DDL        Complete
Story 2 — Dockerized Airflow + PostgreSQL         Complete
Story 3 — Airflow CSV → PostgreSQL DAG            Complete
Story 4 — Documentation and runbook               Complete
```

The Stage 1 pipeline is now reproducible from Docker Compose and ready for review.

---

# Testing and data quality (Stage 1 and Stage 2)

Two different safety nets exist, and they answer different questions:

| Safety net | Question it answers | When it runs |
|---|---|---|
| Unit tests (`pytest`) | "Is the **code** correct for each business rule, including edge cases?" | On demand, before a commit |
| Data-quality checks (`data_quality_check` task) | "Is the **data that was just written** complete and still valid?" | Every DAG run, after the write |

The schema checks that run *before* the load (`validate_schema`, `validate_contract`) are a third layer: they stop a bad **file** from being loaded at all.

## Unit tests

The tests are in `tests/` and need no running Docker stack and no database. The Airflow imports are replaced by stand-ins when Airflow is not installed (`tests/conftest.py`), so the real business logic in `dags/` is what gets tested.

Run them on your own machine:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -v
```

What is covered:

| File | Business rules tested |
|---|---|
| `tests/test_stage1_rules.py` | Manifest status gate: a status passes when its last part is `Completed` or `Done`, in any letter case (for example `Completed`, `done`, `Pending/Completed`); `pending`, `Completed/Pending`, a missing status and an empty CSV stop the run. `schema.yaml` validation: renamed column, value outside the allowed list, `customer_satisfaction` out of range, duplicate composite key. |
| `tests/test_stage2_contract.py` | The real dataset satisfies the contract (2259 rows). Missing / renamed / extra columns, empty and whitespace-only values, bad and mixed timestamp formats, `customer_satisfaction` boundaries (`-1.0` and `1.0` pass, `±1.01` fail), case-sensitive allowed values, duplicate keys (and the same thread with a different timestamp is **not** a duplicate). Type casting, UTC-based `email_date` partition (a `23:30-05:00` timestamp lands on the next UTC day), input not mutated, one Parquet file per day with no rows lost. |
| `tests/test_dq_checks.py` | The data-quality helpers: pass/fail results, error vs warning severity, a lost row, a missing partition and an unexpected partition are all detected. |

Edge cases are the point of these tests: boundary values, mixed formats and "almost duplicate" rows are where business rules usually break.

## Data quality checks in the pipeline

Both DAGs end with a `data_quality_check` task. It **fails the DAG run** when an `error` check fails and only **logs a warning** for `warn` checks. Every result is written to the task log as `DQ PASS`, `DQ WARN` or `DQ FAIL`.

**Stage 1 — PostgreSQL table `customer_care_emails`**

| Check | Severity | Why |
|---|---|---|
| `row_count_matches_csv` — table rows equal CSV rows (2259) | error | Catches a partial or failed load. |
| `duplicate_primary_keys` — no repeated `(thread_id, timestamp, subject)` | error | The composite key is the row identity. |
| `null_key_columns` — no NULL in `thread_id`, `timestamp`, `subject` | error | Key columns must always be present. |
| `satisfaction_out_of_range` — `customer_satisfaction` within -1.0 to 1.0 | error | Re-checks the business rule on the loaded data (the DDL `CHECK` constraint is the second guard). |
| `thread_status_varies` — threads whose emails have different `email_status` | warn | A thread can legitimately go from `ongoing` to `completed`, so this is a hint to look at, not a defect. |

**Stage 2 — MinIO Parquet files**

The task reads every Parquet file back from MinIO and compares it with the CSV:

| Check | Severity | Why |
|---|---|---|
| `partition_set` — the set of `email_date` folders equals the set of dates in the CSV | error | No missing or stray partition. |
| `total_rows` — sum of rows over all Parquet files equals CSV rows | error | No rows lost or duplicated while writing. |
| `per_partition_rows` — each date has the expected row count | error | Pinpoints which partition is wrong. |

**How to see it work**

1. Trigger the DAG and open the `data_quality_check` task log. You should see lines such as `DQ PASS  row_count_matches_csv  expected=2259 actual=2259`.
2. To see a failure, delete one row from the target table and re-run only `data_quality_check`:

   ```bash
   docker compose exec target-postgres psql -U stage1_user -d stage1 \
     -c "DELETE FROM customer_care_emails WHERE ctid = (SELECT ctid FROM customer_care_emails LIMIT 1);"
   ```

   The task fails with `Data quality check failed: row_count_matches_csv (expected=2259 actual=2258)`. Trigger the full DAG again to restore the table (the load is idempotent).

## Assumptions and open questions

These are decisions made where the Task Bank text left room for interpretation. They are listed so a reviewer can disagree with them.

1. **Dataset status gate.** `manifest.yaml` records `status: "Completed/Completed"`. We read a two-part status as the first and second review, so the last part is the current state. The `read_csv` task accepts a dataset only when that last part (after the final `/`) is `Completed` or `Done`, ignoring letter case and spaces, and raises an error for anything else (for example `pending` or `Completed/Pending`) before the CSV is read. Column names and the allowed values inside columns are still compared exactly.
2. **Two Task Bank entries for one dataset.** The dataset appears under code `102` (`Completed/Completed`) and code `129` (`Done`). Both are recorded in `manifest.yaml` (`dataset_code` and `alternate_entries`) and both status values are accepted. The two entries are treated as the same dataset.
3. **Row identity.** `thread_id` alone is not unique (a thread has several emails). The primary key is the composite `(thread_id, timestamp, subject)`; it has no duplicates in the 2259 rows.
4. **Timestamps.** The CSV mixes several timestamp formats (`parse_format: mixed`). All values are converted to UTC, and the Stage 2 partition `email_date` is the **UTC** date.
5. **Thread-level values.** In a few threads, `email_status`, `email_criticality` or `customer_satisfaction` differ between emails of the same thread. They are kept as-is (each row is one email) and only reported as a warning (see `thread_status_varies`).
6. **Allowed values** (`email_status`, `email_criticality`, `agent_effectivity`, `agent_efficiency`) are matched case-sensitively against the lists in the schema files.
7. **Full reload.** Each run replaces the previous result (Stage 1 truncates the table, Stage 2 deletes the old objects). This is what makes re-runs idempotent, but it also means there is no incremental load or history.
8. **Docker-only.** Everything needed to run the pipelines is in the Docker Compose stack. Local Python is needed only to run the unit tests.

---

# Stage 2 — Local CSV to Partitioned Parquet in MinIO

## Overview

Stage 2 reuses the Stage 1 dataset (`customer_care_emails`) but lands it in MinIO as partitioned Parquet instead of PostgreSQL. The DAG validates the CSV against a schema contract and fails before writing anything if the CSV does not conform.

```text
customer_care_emails.csv
        |
        v
validate_contract  ->  transform  ->  write_parquet_to_minio  ->  data_quality_check
(schema_contract.yaml)   (cast types,    (one Parquet file per date)   (read back from MinIO,
 FAIL on mismatch         add email_date)        |                      compare with the CSV)
                                                 v
                              MinIO bucket: customer-care-emails
                              customer_care_emails/email_date=YYYY-MM-DD/part-0.parquet
```

## Stage 2 Files

| Path | Purpose |
|---|---|
| `stage2/customer_care_emails/manifest.yaml` | Dataset name, source, owner, status, target |
| `stage2/customer_care_emails/schema_contract.yaml` | Contract enforced at runtime (columns, types, nullability, allowed values, ranges, primary key) |
| `dags/csv_to_parquet_minio.py` | The Stage 2 DAG |
| `dags/dq_checks.py` | Post-write data-quality helpers (shared with Stage 1) |
| `tests/` | Unit tests (shared with Stage 1) |
| `docker-compose.yaml` | Adds the `minio` and `minio-init` services and passes MinIO settings to Airflow |
| `.env.example` | Placeholders for MinIO credentials |

## Stage 2 Configuration

Add the MinIO credentials to your real `.env` (never commit it):

```text
MINIO_ROOT_USER=your_minio_user
MINIO_ROOT_PASSWORD=your_minio_password
```

`docker-compose.yaml` passes them to Airflow as `MINIO_ENDPOINT_URL`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` and `MINIO_BUCKET`. The DAG reads these from environment variables, so no credentials are stored in code.

## Stage 2 Run Steps

1. Start the stack:

   ```bash
   docker compose up -d
   ```

   The `minio-init` container creates the `customer-care-emails` bucket and exits with code 0.

2. Open the Airflow UI at `http://localhost:8080`, un-pause `csv_to_parquet_minio`, and click Trigger.

3. All four tasks (`validate_contract`, `transform`, `write_parquet_to_minio`, `data_quality_check`) should turn green.

4. Open the MinIO console at `http://localhost:9001`, log in with the MinIO credentials from `.env`, and browse `customer-care-emails/customer_care_emails/`. Each `email_date=YYYY-MM-DD` folder contains `part-0.parquet`.

To list the objects from the command line:

```bash
docker run --rm --network airflow_default \
  -e MC_HOST_local=http://<user>:<password>@minio:9000 \
  cgr.dev/chainguard/minio-client:latest ls --recursive local/customer-care-emails
```

## Stage 2 Tests

**Idempotency.** Trigger the DAG a second time. The DAG deletes the previous objects under `customer_care_emails/` before writing, so the object count and total size stay the same (24 objects, about 767 KiB for this dataset).

**Schema contract failure.** Create a copy of the CSV with a renamed column and run the validation task against it. Nothing is written to MinIO:

```bash
docker compose exec airflow-worker bash -c "sed '1s/thread_id/threadid/' /opt/airflow/data/customer_care_emails/customer_care_emails.csv > /tmp/broken.csv"

docker compose exec -e STAGE2_CSV_PATH=/tmp/broken.csv airflow-worker python -c "
import sys; sys.path.insert(0, '/opt/airflow/dags')
import csv_to_parquet_minio as m
m.validate_contract_task()
"
```

Expected result:

```text
AirflowFailException: Schema contract violated for /tmp/broken.csv:
 - Missing columns: ['thread_id']
 - Unexpected columns: ['threadid']
```

The broken CSV above is created only inside the container's `/tmp` for the test and is never committed to the repository. (For your reference)

The contract also rejects extra columns, empty values in non-nullable columns, invalid timestamps, non-numeric or out-of-range numbers, values outside `allowed_values`, duplicate primary keys, and structurally broken CSV files.

## Stage 2 Notes

- **MinIO image.** The task document references `minio/minio`, but that image can no longer be pulled from Docker Hub (access denied), and the Quay release tags require authentication. This project uses the publicly available Chainguard images `cgr.dev/chainguard/minio` and `cgr.dev/chainguard/minio-client` instead.
- **Distroless client image.** The Chainguard `minio-client` image has no shell, so `minio-init` uses the `MC_HOST_local` environment variable and runs `mc mb --ignore-existing` directly.
- **Partitioning.** Data is partitioned by `email_date`, derived from the UTC `timestamp` column.
- **Airflow version.** The stack runs Airflow 3.3.2, so the DAG imports `DAG` from `airflow.sdk`.
