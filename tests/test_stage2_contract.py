"""Business rules of the Stage 2 schema contract (check_contract / transform_frame / build_partitions)."""
import pytest

import csv_to_parquet_minio as s2
from helpers import REAL_CSV, STAGE2_DIR, frame, good_row

CONTRACT = s2.load_contract(str(STAGE2_DIR / "schema_contract.yaml"))


def problems(*rows):
    return s2.check_contract(frame(*rows), CONTRACT)


def test_real_dataset_satisfies_contract():
    df = s2.read_csv_as_text(str(REAL_CSV))
    assert len(df) == 2259
    assert s2.check_contract(df, CONTRACT) == []


def test_valid_row_has_no_problems():
    assert problems(good_row()) == []


def test_missing_column_is_reported():
    df = frame(good_row()).drop(columns=["thread_id"])
    out = s2.check_contract(df, CONTRACT)
    assert any("Missing columns" in p and "thread_id" in p for p in out)


def test_renamed_column_is_reported_as_missing_and_unexpected():
    df = frame(good_row()).rename(columns={"sender": "from"})
    out = " | ".join(s2.check_contract(df, CONTRACT))
    assert "Missing columns" in out and "Unexpected columns" in out


def test_empty_and_whitespace_only_values_count_as_null():
    assert any("nullable=false" in p for p in problems(good_row(subject="")))
    assert any("nullable=false" in p for p in problems(good_row(subject="   ")))


def test_unparseable_timestamp_is_rejected():
    assert any("not valid timestamps" in p for p in problems(good_row(timestamp="yesterday")))


def test_mixed_timestamp_formats_are_accepted():
    rows = [
        good_row(thread_id="a", timestamp="2023-10-26 10:02:34+00:00"),
        good_row(thread_id="b", timestamp="2023-10-26T10:02:34Z"),
        good_row(thread_id="c", timestamp="2023-10-26 10:02:34.123456+00:00"),
    ]
    assert problems(*rows) == []


def test_satisfaction_boundaries_are_inclusive():
    assert problems(good_row(thread_id="a", customer_satisfaction="-1.0"),
                    good_row(thread_id="b", customer_satisfaction="1.0")) == []


def test_satisfaction_outside_range_is_rejected():
    assert any("above maximum" in p for p in problems(good_row(customer_satisfaction="1.01")))
    assert any("below minimum" in p for p in problems(good_row(customer_satisfaction="-1.01")))


def test_satisfaction_not_a_number_is_rejected():
    assert any("not float" in p for p in problems(good_row(customer_satisfaction="n/a")))


def test_value_outside_allowed_list_is_rejected():
    assert any("email_status" in p for p in problems(good_row(email_status="pending")))
    assert any("email_criticality" in p for p in problems(good_row(email_criticality="urgent")))
    assert any("agent_efficiency" in p for p in problems(good_row(agent_efficiency="great")))


def test_allowed_values_are_case_sensitive():
    assert any("email_status" in p for p in problems(good_row(email_status="Completed")))


def test_duplicate_composite_key_is_rejected():
    out = problems(good_row(), good_row(message_body="different body, same key"))
    assert any("primary_key" in p and "2 row(s)" in p for p in out)


def test_same_thread_different_timestamp_is_not_a_duplicate():
    out = problems(good_row(), good_row(timestamp="2023-10-27 09:00:00+00:00"))
    assert out == []


# ---------------------------------------------------------------- transform_frame
def test_transform_casts_types_and_adds_partition_column():
    out = s2.transform_frame(frame(good_row()), CONTRACT)
    assert str(out["customer_satisfaction"].dtype) == "float64"
    assert str(out["timestamp"].dtype).startswith("datetime64")
    assert out["email_date"].tolist() == ["2023-10-26"]


def test_partition_date_is_taken_in_utc():
    # 23:30 at UTC-05:00 is already the next day in UTC.
    out = s2.transform_frame(frame(good_row(timestamp="2023-10-26 23:30:00-05:00")), CONTRACT)
    assert out["email_date"].tolist() == ["2023-10-27"]


def test_transform_does_not_modify_its_input():
    df = frame(good_row())
    s2.transform_frame(df, CONTRACT)
    assert "email_date" not in df.columns
    assert df["customer_satisfaction"].tolist() == ["0.5"]


# ---------------------------------------------------------------- build_partitions
def test_one_parquet_file_per_day_and_no_rows_lost():
    pq = pytest.importorskip("pyarrow.parquet")
    import io

    df = s2.transform_frame(
        frame(
            good_row(thread_id="a", timestamp="2023-10-26 10:00:00+00:00"),
            good_row(thread_id="b", timestamp="2023-10-26 11:00:00+00:00"),
            good_row(thread_id="c", timestamp="2023-10-27 09:00:00+00:00"),
        ),
        CONTRACT,
    )
    parts = dict(s2.build_partitions(df))
    assert sorted(parts) == [
        "customer_care_emails/email_date=2023-10-26/part-0.parquet",
        "customer_care_emails/email_date=2023-10-27/part-0.parquet",
    ]
    rows = {k: pq.read_table(io.BytesIO(v)).num_rows for k, v in parts.items()}
    assert sum(rows.values()) == 3
    # the partition column lives in the object key, not inside the file
    assert "email_date" not in pq.read_table(io.BytesIO(parts[sorted(parts)[0]])).column_names
