"""The in-pipeline data-quality helpers (dags/dq_checks.py)."""
import logging

import pytest

from dq_checks import DataQualityError, check_equal, check_zero, compare_partitions, report


def test_check_equal_pass_and_fail():
    assert check_equal("rows", 10, 10).passed
    assert not check_equal("rows", 10, 9).passed


def test_check_zero_pass_and_fail():
    assert check_zero("dups", 0).passed
    assert not check_zero("dups", 3).passed


def test_report_raises_when_an_error_check_fails():
    with pytest.raises(DataQualityError, match="rows"):
        report([check_equal("rows", 10, 9)])


def test_report_does_not_raise_for_failed_warning(caplog):
    caplog.set_level(logging.INFO)
    report([check_zero("thread_status_varies", 7, severity="warn"), check_equal("rows", 1, 1)])
    assert any("DQ WARN" in r.message for r in caplog.records)


def test_report_logs_passes(caplog):
    caplog.set_level(logging.INFO)
    report([check_equal("rows", 1, 1)])
    assert any("DQ PASS" in r.message for r in caplog.records)


def test_matching_partitions_pass():
    results = compare_partitions({"2023-10-26": 2, "2023-10-27": 1}, {"2023-10-26": 2, "2023-10-27": 1})
    assert all(r.passed for r in results)


def test_lost_row_is_detected():
    results = compare_partitions({"2023-10-26": 2}, {"2023-10-26": 1})
    failed = {r.name for r in results if not r.passed}
    assert failed == {"total_rows", "per_partition_rows"}


def test_missing_partition_is_detected():
    results = compare_partitions({"2023-10-26": 2, "2023-10-27": 1}, {"2023-10-26": 2})
    assert "partition_set" in {r.name for r in results if not r.passed}


def test_unexpected_partition_is_detected():
    results = compare_partitions({"2023-10-26": 2}, {"2023-10-26": 2, "1970-01-01": 1})
    assert "partition_set" in {r.name for r in results if not r.passed}
