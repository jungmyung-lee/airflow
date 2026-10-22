from pathlib import Path
import logging

import pandas as pd
import yaml

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =========================================================
# Configuration
# =========================================================

DATA_DIR = Path("/opt/airflow/data/customer_care_emails")

CSV_PATH = DATA_DIR / "customer_care_emails.csv"
SCHEMA_PATH = DATA_DIR / "schema.yaml"
MANIFEST_PATH = DATA_DIR / "manifest.yaml"
DDL_PATH = DATA_DIR / "ddl.sql"

TRANSFORMED_CSV_PATH = (
    DATA_DIR / "customer_care_emails_cleaned.csv"
)

POSTGRES_CONN_ID = "target_postgres"
TARGET_TABLE = "customer_care_emails"

READY_STATUSES = {
    "Completed/Completed",
    "Done",
}

logger = logging.getLogger(__name__)


# =========================================================
# Task 1: read_csv
# =========================================================

def read_csv():
    """
    Read the source CSV and check the dataset status
    recorded in manifest.yaml.
    """

    # Read manifest.yaml
    with open(
        MANIFEST_PATH,
        "r",
        encoding="utf-8",
    ) as file:
        manifest = yaml.safe_load(file)

    status = manifest.get("status")

    if not status:
        raise ValueError(
            "manifest.yaml does not contain a dataset status."
        )

    if status not in READY_STATUSES:
        raise ValueError(
            f"Dataset status '{status}' is not ready for ingestion."
        )

    # Read CSV
    df = pd.read_csv(CSV_PATH)

    if df.empty:
        raise ValueError(
            "The source CSV is empty."
        )

    logger.info(
        "Dataset status: %s",
        status,
    )

    logger.info(
        "CSV rows: %s",
        len(df),
    )

    logger.info(
        "CSV columns: %s",
        list(df.columns),
    )

    return {
        "rows": len(df),
        "columns": list(df.columns),
        "status": status,
    }


# =========================================================
# Task 2: validate_schema
# =========================================================

def validate_schema():
    """
    Validate customer_care_emails.csv
    against schema.yaml.

    Checks:
    - columns
    - nullable rules
    - timestamp format
    - numeric values
    - allowed values
    - composite primary key uniqueness
    """

    df = pd.read_csv(CSV_PATH)

    with open(
        SCHEMA_PATH,
        "r",
        encoding="utf-8",
    ) as file:
        schema = yaml.safe_load(file)

    # -----------------------------------------------------
    # 1. Column validation
    # -----------------------------------------------------

    expected_columns = [
        column["name"]
        for column in schema["columns"]
    ]

    actual_columns = list(df.columns)

    if expected_columns != actual_columns:
        raise ValueError(
            "Column mismatch.\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {actual_columns}"
        )

    # -----------------------------------------------------
    # 2. Nullable validation
    # -----------------------------------------------------

    for column_definition in schema["columns"]:

        column_name = column_definition["name"]

        nullable = column_definition.get(
            "nullable",
            True,
        )

        if not nullable:

            if df[column_name].isna().any():

                null_count = int(
                    df[column_name]
                    .isna()
                    .sum()
                )

                raise ValueError(
                    f"Column '{column_name}' contains "
                    f"{null_count} null value(s), "
                    f"but nullable=false."
                )

    # -----------------------------------------------------
    # 3. Type and value validation
    # -----------------------------------------------------

    for column_definition in schema["columns"]:

        column_name = column_definition["name"]

        column_type = column_definition.get(
            "type"
        )

        # -----------------------------
        # Timestamp validation
        # -----------------------------

        if column_type == "timestamp":

            try:

                pd.to_datetime(
                    df[column_name],
                    format="mixed",
                    utc=True,
                    errors="raise",
                )

            except Exception as error:

                raise ValueError(
                    f"Column '{column_name}' "
                    "contains an invalid timestamp."
                ) from error

        # -----------------------------
        # Float validation
        # -----------------------------

        elif column_type == "float":

            try:

                numeric_values = pd.to_numeric(
                    df[column_name],
                    errors="raise",
                )

            except Exception as error:

                raise ValueError(
                    f"Column '{column_name}' "
                    "contains a non-numeric value."
                ) from error

            minimum = column_definition.get(
                "minimum"
            )

            maximum = column_definition.get(
                "maximum"
            )

            if minimum is not None:

                if (
                    numeric_values < minimum
                ).any():

                    raise ValueError(
                        f"Column '{column_name}' "
                        f"contains a value below "
                        f"minimum {minimum}."
                    )

            if maximum is not None:

                if (
                    numeric_values > maximum
                ).any():

                    raise ValueError(
                        f"Column '{column_name}' "
                        f"contains a value above "
                        f"maximum {maximum}."
                    )

        # -----------------------------
        # allowed_values validation
        # -----------------------------

        allowed_values = column_definition.get(
            "allowed_values"
        )

        if allowed_values is not None:

            actual_values = set(
                df[column_name]
                .dropna()
                .astype(str)
                .unique()
            )

            invalid_values = (
                actual_values
                - set(allowed_values)
            )

            if invalid_values:

                raise ValueError(
                    f"Column '{column_name}' "
                    f"contains invalid values: "
                    f"{sorted(invalid_values)}"
                )

    # -----------------------------------------------------
    # 4. Composite primary key validation
    # -----------------------------------------------------

    primary_key = schema["primary_key"]

    if (
        df[primary_key]
        .isna()
        .any()
        .any()
    ):

        raise ValueError(
            f"Primary key columns "
            f"{primary_key} contain null values."
        )

    duplicate_count = int(
        df.duplicated(
            subset=primary_key,
            keep=False,
        ).sum()
    )

    if duplicate_count > 0:

        raise ValueError(
            f"Composite primary key "
            f"{primary_key} is not unique. "
            f"Duplicate rows: {duplicate_count}"
        )

    logger.info(
        "Schema validation passed."
    )

    logger.info(
        "Validated rows: %s",
        len(df),
    )

    logger.info(
        "Validated columns: %s",
        len(df.columns),
    )

    return True


# =========================================================
# Task 3: transform
# =========================================================

def transform():
    """
    Clean and transform the CSV.

    Transformations:
    - strip whitespace from text
    - convert timestamp to UTC
    - convert customer_satisfaction to numeric
    """

    df = pd.read_csv(CSV_PATH)

    # -----------------------------------------------------
    # Clean text columns
    # -----------------------------------------------------

    text_columns = [
        "subject",
        "sender",
        "receiver",
        "message_body",
        "thread_id",
        "email_types",
        "email_status",
        "email_criticality",
        "product_types",
        "agent_effectivity",
        "agent_efficiency",
    ]

    for column in text_columns:

        df[column] = (
            df[column]
            .astype(str)
            .str.strip()
        )

    # -----------------------------------------------------
    # Timestamp -> UTC
    # -----------------------------------------------------

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="mixed",
        utc=True,
        errors="raise",
    )

    df["timestamp"] = (
        df["timestamp"]
        .dt.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
    )

    # -----------------------------------------------------
    # customer_satisfaction -> numeric
    # -----------------------------------------------------

    df["customer_satisfaction"] = (
        pd.to_numeric(
            df["customer_satisfaction"],
            errors="raise",
        )
    )

    # -----------------------------------------------------
    # Save cleaned CSV
    # -----------------------------------------------------

    df.to_csv(
        TRANSFORMED_CSV_PATH,
        index=False,
    )

    logger.info(
        "Transformed CSV written to %s",
        TRANSFORMED_CSV_PATH,
    )

    logger.info(
        "Transformed rows: %s",
        len(df),
    )

    return str(
        TRANSFORMED_CSV_PATH
    )


# =========================================================
# Task 4: load_to_postgres
# =========================================================

def load_to_postgres():
    """
    Load transformed CSV into PostgreSQL.

    Idempotency strategy:

    CREATE TABLE IF NOT EXISTS
        ->
    TRUNCATE existing rows
        ->
    INSERT transformed rows

    Re-running the DAG therefore does not
    create duplicate rows.
    """

    df = pd.read_csv(
        TRANSFORMED_CSV_PATH
    )

    if df.empty:

        raise ValueError(
            "Transformed CSV is empty."
        )

    # Convert timestamp back to datetime
    # before sending it to PostgreSQL.

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        utc=True,
        errors="raise",
    )

    columns = [
        "subject",
        "sender",
        "receiver",
        "timestamp",
        "message_body",
        "thread_id",
        "email_types",
        "email_status",
        "email_criticality",
        "product_types",
        "agent_effectivity",
        "agent_efficiency",
        "customer_satisfaction",
    ]

    # -----------------------------------------------------
    # Convert DataFrame rows to Python tuples
    # -----------------------------------------------------

    rows = []

    for row in df[
        columns
    ].itertuples(
        index=False,
        name=None,
    ):

        converted_row = list(row)

        # pandas Timestamp
        # ->
        # Python datetime

        converted_row[3] = (
            converted_row[3]
            .to_pydatetime()
        )

        rows.append(
            tuple(converted_row)
        )

    # -----------------------------------------------------
    # Connect to target PostgreSQL
    # -----------------------------------------------------

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    # -----------------------------------------------------
    # Run ddl.sql
    # -----------------------------------------------------

    with open(
        DDL_PATH,
        "r",
        encoding="utf-8",
    ) as file:

        ddl_sql = file.read()

    hook.run(
        ddl_sql
    )

    # -----------------------------------------------------
    # Remove previous data
    # -----------------------------------------------------

    hook.run(
        f'TRUNCATE TABLE "{TARGET_TABLE}";'
    )

    # -----------------------------------------------------
    # Insert cleaned rows
    # -----------------------------------------------------

    hook.insert_rows(
        table=TARGET_TABLE,
        rows=rows,
        target_fields=columns,
        commit_every=500,
    )

    logger.info(
        "Successfully loaded %s rows "
        "into PostgreSQL table '%s'.",
        len(rows),
        TARGET_TABLE,
    )

    return len(rows)


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="load_customer_care_emails_to_postgres",
    description=(
        "Stage 1 pipeline: "
        "customer_care_emails CSV "
        "to PostgreSQL"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "stage1",
        "customer_care_emails",
        "postgres",
    ],
) as dag:

    read_csv_task = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv,
    )

    validate_schema_task = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_to_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    (
        read_csv_task
        >> validate_schema_task
        >> transform_task
        >> load_to_postgres_task
    )