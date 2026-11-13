"""Business rules of the Stage 1 DAG: the manifest status gate and schema.yaml validation.

The DAG reads its files from module-level paths, so each test points those paths
at a temporary folder holding a copy of the real files (no Docker needed).
"""
import shutil

import pytest

import load_customer_care_emails_to_postgres as s1
from helpers import STAGE1_DIR


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    for name in ("customer_care_emails.csv", "schema.yaml", "manifest.yaml", "ddl.sql"):
        shutil.copy(STAGE1_DIR / name, tmp_path / name)
    monkeypatch.setattr(s1, "DATA_DIR", tmp_path)
    monkeypatch.setattr(s1, "CSV_PATH", tmp_path / "customer_care_emails.csv")
    monkeypatch.setattr(s1, "SCHEMA_PATH", tmp_path / "schema.yaml")
    monkeypatch.setattr(s1, "MANIFEST_PATH", tmp_path / "manifest.yaml")
    monkeypatch.setattr(s1, "DDL_PATH", tmp_path / "ddl.sql")
    return tmp_path


def set_status(workdir, status):
    path = workdir / "manifest.yaml"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace('status: "Completed/Completed"', f'status: "{status}"'), encoding="utf-8")


def edit_csv(workdir, old, new):
    path = workdir / "customer_care_emails.csv"
    text = path.read_text(encoding="utf-8")
    assert old in text, "test setup: text to replace not found"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


# ----------------------------------------------------------- status gate (read_csv)
def test_ready_statuses_are_exactly_the_two_documented_values():
    assert s1.READY_STATUSES == {"Completed/Completed", "Done"}


def test_completed_status_is_accepted(workdir):
    result = s1.read_csv()
    assert result["rows"] == 2259 and result["status"] == "Completed/Completed"


def test_done_status_is_accepted(workdir):
    set_status(workdir, "Done")
    assert s1.read_csv()["status"] == "Done"


@pytest.mark.parametrize("status", ["pending", "Pending", "In progress", "Draft"])
def test_not_ready_status_stops_the_pipeline(workdir, status):
    set_status(workdir, status)
    with pytest.raises(ValueError, match="not ready for ingestion"):
        s1.read_csv()


def test_missing_status_stops_the_pipeline(workdir):
    (workdir / "manifest.yaml").write_text("dataset_name: customer_care_emails\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not contain a dataset status"):
        s1.read_csv()


def test_empty_csv_stops_the_pipeline(workdir):
    header = (workdir / "customer_care_emails.csv").read_text(encoding="utf-8").splitlines()[0]
    (workdir / "customer_care_emails.csv").write_text(header + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        s1.read_csv()


# ----------------------------------------------------------- schema validation
def test_real_dataset_passes_schema_validation(workdir):
    s1.validate_schema()  # must not raise


def test_value_outside_allowed_list_fails_validation(workdir):
    edit_csv(workdir, ",ongoing,", ",pending,")
    with pytest.raises(ValueError):
        s1.validate_schema()


def test_satisfaction_out_of_range_fails_validation(workdir):
    import pandas as pd

    df = pd.read_csv(workdir / "customer_care_emails.csv")
    df.loc[0, "customer_satisfaction"] = 1.5
    df.to_csv(workdir / "customer_care_emails.csv", index=False)
    with pytest.raises(ValueError):
        s1.validate_schema()


def test_duplicate_primary_key_fails_validation(workdir):
    import pandas as pd

    df = pd.read_csv(workdir / "customer_care_emails.csv")
    pd.concat([df, df.iloc[[0]]]).to_csv(workdir / "customer_care_emails.csv", index=False)
    with pytest.raises(ValueError):
        s1.validate_schema()


def test_renamed_column_fails_validation(workdir):
    import pandas as pd

    df = pd.read_csv(workdir / "customer_care_emails.csv").rename(columns={"sender": "from"})
    df.to_csv(workdir / "customer_care_emails.csv", index=False)
    with pytest.raises(ValueError):
        s1.validate_schema()
