"""Stage 2: customer_care_emails CSV -> schema-contract validation -> Parquet -> MinIO.

Flow
    validate_contract  : reads the CSV and checks it against schema_contract.yaml.
                         Any violation fails the DAG BEFORE anything is written.
    transform          : casts types, adds the partition column, writes a staging Parquet file.
    write_parquet_to_minio : uploads one Parquet file per partition to MinIO.

Layout in the bucket
    <bucket>/customer_care_emails/email_date=YYYY-MM-DD/part-0.parquet

Secrets are NOT in this file. MinIO settings come from environment variables that
docker-compose passes in from .env (MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY,
MINIO_SECRET_KEY, MINIO_BUCKET).
"""
from __future__ import annotations

import io
import os
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DATASET = "customer_care_emails"
PARTITION_COL = "email_date"

# Paths as seen from INSIDE the Airflow containers (see the volumes in docker-compose.yaml).
CSV_PATH = os.environ.get(
    "STAGE2_CSV_PATH", f"/opt/airflow/data/{DATASET}/customer_care_emails.csv"
)
CONTRACT_PATH = os.environ.get(
    "STAGE2_CONTRACT_PATH", f"/opt/airflow/contracts/{DATASET}/schema_contract.yaml"
)
STAGING_DIR = Path(os.environ.get("STAGE2_STAGING_DIR", "/tmp/stage2_staging"))

MAX_REPORTED = 20  # cap on how many problems are listed in the failure message


# --------------------------------------------------------------------------- #
# Pure helpers (no Airflow, no network) so they can be unit-tested               #
# --------------------------------------------------------------------------- #
def load_contract(path: str) -> dict:
    import yaml

    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_csv_as_text(path: str):
    """Read every column as text so type problems surface in validation, not in the reader."""
    import pandas as pd

    return pd.read_csv(path, dtype=str, keep_default_na=False)


def check_contract(df, contract: dict) -> list[str]:
    """Return a list of human-readable contract violations (empty list = valid)."""
    import pandas as pd

    problems: list[str] = []
    cols = contract["columns"]
    expected = [c["name"] for c in cols]

    # 1. Column names: missing / unexpected (a renamed column shows up as both).
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected]
    if missing:
        problems.append(f"Missing columns: {missing}")
    if extra:
        problems.append(f"Unexpected columns: {extra}")
    if missing:
        # Without the columns the row-level checks below are meaningless.
        return problems

    def sample(mask) -> str:
        return str(list(df.index[mask][:5] + 2))  # +2 = CSV line number (header is line 1)

    # 2. Row-level checks per column.
    for spec in cols:
        name = spec["name"]
        series = df[name]
        empty = series.str.strip() == ""

        if not spec.get("nullable", True) and empty.any():
            problems.append(
                f"{name}: {int(empty.sum())} empty value(s) but nullable=false "
                f"(e.g. CSV lines {sample(empty)})"
            )

        present = series[~empty]
        ctype = spec["type"]

        if ctype == "timestamp":
            parsed = pd.to_datetime(
                present, format=spec.get("parse_format") or None, utc=True, errors="coerce"
            )
            bad = parsed.isna()
            if bad.any():
                problems.append(
                    f"{name}: {int(bad.sum())} value(s) are not valid timestamps "
                    f"(e.g. {list(present[bad][:3])})"
                )
        elif ctype in ("float", "integer"):
            num = pd.to_numeric(present, errors="coerce")
            bad = num.isna()
            if bad.any():
                problems.append(
                    f"{name}: {int(bad.sum())} value(s) are not {ctype} "
                    f"(e.g. {list(present[bad][:3])})"
                )
            ok = num.dropna()
            if "minimum" in spec and (ok < spec["minimum"]).any():
                problems.append(f"{name}: value(s) below minimum {spec['minimum']}")
            if "maximum" in spec and (ok > spec["maximum"]).any():
                problems.append(f"{name}: value(s) above maximum {spec['maximum']}")

        allowed = spec.get("allowed_values")
        if allowed:
            bad = ~present.isin(allowed)
            if bad.any():
                problems.append(
                    f"{name}: {int(bad.sum())} value(s) not in {allowed} "
                    f"(e.g. {sorted(set(present[bad]))[:3]})"
                )

    # 3. Primary key uniqueness.
    pk = contract.get("primary_key") or []
    if pk and all(c in df.columns for c in pk):
        dup = df.duplicated(subset=pk, keep=False)
        if dup.any():
            problems.append(f"primary_key {pk}: {int(dup.sum())} row(s) share a duplicate key")

    return problems


def transform_frame(df, contract: dict):
    """Cast to the contract types and add the partition column."""
    import pandas as pd

    out = df.copy()
    for spec in contract["columns"]:
        name, ctype = spec["name"], spec["type"]
        if ctype == "timestamp":
            out[name] = pd.to_datetime(out[name], format=spec.get("parse_format") or None, utc=True)
        elif ctype == "float":
            out[name] = pd.to_numeric(out[name]).astype("float64")
        elif ctype == "integer":
            out[name] = pd.to_numeric(out[name]).astype("int64")
        else:
            out[name] = out[name].astype("string")
    out[PARTITION_COL] = out["timestamp"].dt.strftime("%Y-%m-%d")
    return out


def build_partitions(df) -> list[tuple[str, bytes]]:
    """Return [(object_key, parquet_bytes), ...], one entry per partition value."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    parts = []
    for value, chunk in df.groupby(PARTITION_COL, sort=True):
        table = pa.Table.from_pandas(chunk.drop(columns=[PARTITION_COL]), preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        parts.append((f"{DATASET}/{PARTITION_COL}={value}/part-0.parquet", buf.getvalue()))
    return parts


# --------------------------------------------------------------------------- #
# Airflow task callables                                                       #
# --------------------------------------------------------------------------- #
def validate_contract_task():
    import pandas as pd

    try:
        df = read_csv_as_text(CSV_PATH)
    except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeDecodeError) as exc:
        # A structurally broken file (ragged rows, empty file, bad encoding) is a contract failure too.
        raise AirflowFailException(f"CSV could not be parsed ({CSV_PATH}): {exc}") from exc
    contract = load_contract(CONTRACT_PATH)
    problems = check_contract(df, contract)
    if problems:
        shown = "\n - ".join(problems[:MAX_REPORTED])
        raise AirflowFailException(f"Schema contract violated for {CSV_PATH}:\n - {shown}")
    print(f"Contract OK: {len(df)} rows, {len(df.columns)} columns")


def transform_task(run_id: str) -> str:
    df = read_csv_as_text(CSV_PATH)
    contract = load_contract(CONTRACT_PATH)
    out = transform_frame(df, contract)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    path = STAGING_DIR / f"{DATASET}_{run_id.replace(':', '_').replace('+', '_')}.parquet"
    out.to_parquet(path, index=False)
    print(f"Staged {len(out)} rows -> {path}")
    return str(path)  # small string via XCom, not the data itself


def write_parquet_to_minio_task(ti):
    import boto3
    import pandas as pd
    from botocore.exceptions import ClientError

    staged = ti.xcom_pull(task_ids="transform")
    df = pd.read_parquet(staged)

    bucket = os.environ["MINIO_BUCKET"]
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        raise AirflowFailException(f"Bucket '{bucket}' is not reachable: {exc}") from exc

    # Idempotent reload: remove the previous output of this dataset, then write fresh.
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{DATASET}/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})

    parts = build_partitions(df)
    for key, payload in parts:
        s3.put_object(Bucket=bucket, Key=key, Body=payload)
    print(f"Wrote {len(parts)} partition file(s) to s3://{bucket}/{DATASET}/")


# --------------------------------------------------------------------------- #
# DAG                                                                          #
# --------------------------------------------------------------------------- #
with DAG(
    dag_id="csv_to_parquet_minio",
    description="customer_care_emails: CSV -> contract check -> partitioned Parquet in MinIO",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stage2", "minio", "parquet"],
) as dag:
    validate_contract = PythonOperator(
        task_id="validate_contract", python_callable=validate_contract_task
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        op_kwargs={"run_id": "{{ run_id }}"},
    )
    write_parquet_to_minio = PythonOperator(
        task_id="write_parquet_to_minio", python_callable=write_parquet_to_minio_task
    )

    validate_contract >> transform >> write_parquet_to_minio
