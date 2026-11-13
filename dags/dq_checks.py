"""Data-quality checks that run INSIDE the pipelines, after the data has been written.

Why a separate module?
    The input checks (schema.yaml / schema_contract.yaml) prove the *file* is good.
    These checks prove the *result* is good: what landed in PostgreSQL / MinIO is
    complete and still satisfies the business rules. No Airflow imports here, so the
    logic is unit-tested in tests/test_dq_checks.py.

Severity
    "error": the task (and the DAG run) fails.
    "warn" : logged loudly, the run still succeeds (a signal to look, not a defect).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when at least one error-severity check fails."""


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str
    severity: str = "error"  # "error" | "warn"


def check_equal(name: str, expected, actual, severity: str = "error") -> CheckResult:
    ok = expected == actual
    return CheckResult(name, ok, f"expected={expected} actual={actual}", severity)


def check_zero(name: str, count: int, severity: str = "error") -> CheckResult:
    return CheckResult(name, count == 0, f"offending rows/groups={count}", severity)


def compare_partitions(expected: dict[str, int], actual: dict[str, int]) -> list[CheckResult]:
    """Compare {partition_value: row_count} between the source data and what is in storage."""
    results = [
        check_equal("partition_set", sorted(expected), sorted(actual)),
        check_equal("total_rows", sum(expected.values()), sum(actual.values())),
    ]
    wrong = {k: (expected[k], actual[k]) for k in expected if k in actual and expected[k] != actual[k]}
    results.append(CheckResult("per_partition_rows", not wrong, f"mismatches (expected, actual)={wrong}"))
    return results


def report(results: list[CheckResult]) -> None:
    """Log every result; raise DataQualityError if any error-severity check failed."""
    for r in results:
        if r.passed:
            logger.info("DQ PASS  %-28s %s", r.name, r.detail)
        elif r.severity == "warn":
            logger.warning("DQ WARN  %-28s %s", r.name, r.detail)
        else:
            logger.error("DQ FAIL  %-28s %s", r.name, r.detail)
    failed = [r for r in results if not r.passed and r.severity == "error"]
    if failed:
        raise DataQualityError("Data quality check failed: " + "; ".join(f"{r.name} ({r.detail})" for r in failed))
