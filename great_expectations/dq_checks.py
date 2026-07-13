"""
Lightweight Delta table data-quality checker, used by the Airflow
`data_quality_gate` task between Silver and Gold.

Kept dependency-light (pure PySpark) so it runs anywhere, but the checks
mirror what you'd express as Great Expectations Expectations
(expect_column_values_to_not_be_null, expect_column_values_to_be_between,
expect_column_values_to_be_unique, etc.) -- see README for how to port
these 1:1 into a GE Expectation Suite if the team standardizes on it.
"""
from dataclasses import dataclass
from pyspark.sql import DataFrame, functions as F


@dataclass
class DQResult:
    check_name: str
    passed: bool
    detail: str


def expect_not_null(df: DataFrame, column: str) -> DQResult:
    null_count = df.filter(F.col(column).isNull()).count()
    return DQResult(
        f"not_null:{column}",
        null_count == 0,
        f"{null_count} null rows in {column}",
    )


def expect_unique(df: DataFrame, column: str) -> DQResult:
    total = df.count()
    distinct = df.select(column).distinct().count()
    return DQResult(
        f"unique:{column}",
        total == distinct,
        f"{total - distinct} duplicate values in {column}",
    )


def expect_between(df: DataFrame, column: str, min_val: float, max_val: float) -> DQResult:
    out_of_range = df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()
    return DQResult(
        f"between:{column}",
        out_of_range == 0,
        f"{out_of_range} rows outside [{min_val}, {max_val}] for {column}",
    )


def expect_freshness(df: DataFrame, ts_column: str, max_lag_minutes: int) -> DQResult:
    latest = df.agg(F.max(ts_column).alias("latest")).collect()[0]["latest"]
    if latest is None:
        return DQResult(f"freshness:{ts_column}", False, "no rows found")
    lag_minutes = (F.current_timestamp().cast("long") - F.lit(latest).cast("long")) / 60
    lag_val = df.select(lag_minutes.alias("lag")).limit(1).collect()[0]["lag"]
    return DQResult(
        f"freshness:{ts_column}",
        lag_val <= max_lag_minutes,
        f"latest row is {lag_val:.1f} min old (max allowed {max_lag_minutes})",
    )


def run_silver_orders_suite(df: DataFrame) -> list[DQResult]:
    return [
        expect_not_null(df, "order_id"),
        expect_not_null(df, "customer_id"),
        expect_unique(df, "order_id"),
        expect_between(df, "order_value_usd", 0, 10000),
        expect_freshness(df, "event_ts", max_lag_minutes=60),
    ]


def assert_suite_passes(results: list[DQResult]) -> None:
    failures = [r for r in results if not r.passed]
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.check_name} — {r.detail}")
    if failures:
        raise AssertionError(f"{len(failures)} data quality check(s) failed: "
                              f"{[f.check_name for f in failures]}")
