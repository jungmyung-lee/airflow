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
```

The DAG is designed to be idempotent. Re-running the pipeline does not create duplicate rows because the target table is truncated before the full dataset is loaded again.

---

## Dataset

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
│   └── load_customer_care_emails_to_postgres.py
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

Generate a Fernet key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
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

The DAG should appear as:

```text
load_customer_care_emails_to_postgres
```

---

## 6. Pipeline Tasks

The DAG executes four tasks in order:

```text
read_csv
    ↓
validate_schema
    ↓
transform
    ↓
load_to_postgres
```

### `read_csv`

Reads:

```text
customer_care_emails.csv
manifest.yaml
```

The task verifies that the dataset status is ready for processing.

Accepted statuses currently include:

```text
Completed/Completed
Done
```

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

## 7. Trigger the DAG

In the Airflow UI:

1. Open `load_customer_care_emails_to_postgres`.
2. Trigger a new DAG run.
3. Wait for all four tasks to complete.

A successful run should show:

```text
read_csv            Success
validate_schema     Success
transform           Success
load_to_postgres    Success
```

---

## 8. Verify the PostgreSQL Data

Enter the target PostgreSQL container:

```bash
docker exec -it airflow-target-postgres-1 bash
```

Connect to PostgreSQL:

```bash
psql -U stage1_user -d stage1
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

---

## Stage 1 Status

```text
Story 1 — Dataset manifest, schema and DDL        Complete
Story 2 — Dockerized Airflow + PostgreSQL         Complete
Story 3 — Airflow CSV → PostgreSQL DAG            Complete
Story 4 — Documentation and runbook               Complete
```

The Stage 1 pipeline is now reproducible from Docker Compose and ready for review.
