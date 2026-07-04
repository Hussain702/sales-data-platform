from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Dict, Any, List
from utils.logger import get_logger

logger = get_logger(__name__)


class DQchecks:
    def __init__(self, tbl_name: str):
        self.tbl_name = tbl_name
        self.checks: List[Dict[str, Any]] = []
        self.passed = True

    def add(self, check_name: str, passed: bool, details: str = ""):
        status = "passed" if passed else "failed"

        self.checks.append({
            "check": check_name,
            "status": status,
            "details": details
        })

        if not passed:
            self.passed = False

        logger.info(
            f"[DQ] {self.tbl_name} | {check_name}: {status} | {details}"
        )

    def summary(self) -> str:
        lines = [
            f"\n{'=' * 60}",
            f"DQ Report — {self.tbl_name}",
            f"{'=' * 60}"
        ]

        for c in self.checks:
            lines.append(
                f"[{c['status']}] {c['check']}: {c['details']}"
            )

        overall = "ALL PASSED" if self.passed else "FAILURES DETECTED"

        lines.append(f"\nOverall: {overall}")
        lines.append("=" * 60)

        return "\n".join(lines)


def check_row_count(df: DataFrame, dq: DQchecks) -> DQchecks:
    min_count = 1

    count = df.count()

    passed = count >= min_count

    dq.add(
        "row_count_check",
        passed,
        f"rows={count}, min={min_count}"
    )

    return dq


def check_nulls(df: DataFrame, cols: list, dq: DQchecks) -> DQchecks:
    threshold = 0.05

    total = df.count()

    if total == 0:
        dq.add("null_check", False, "No rows to check")
        return dq

    for c in cols:
        null_count = df.filter(col(c).isNull()).count()

        null_rate = null_count / total

        passed = null_rate < threshold

        dq.add(
            f"null_check_{c}",
            passed,
            f"null_rate={null_rate:.2%}, threshold={threshold:.2%}"
        )

    return dq


def check_duplicates(df: DataFrame, cols: list, dq: DQchecks) -> DQchecks:
    total = df.count()

    if total == 0:
        dq.add("duplicate_check", False, "No rows to check")
        return dq

    for c in cols:
        dup_count = (
            df.groupBy(c)
              .count()
              .filter(col("count") > 1)
              .count()
        )

        dup_rate = dup_count / total

        passed = dup_rate == 0

        dq.add(
            f"duplicate_check_{c}",
            passed,
            f"duplicate_rate={dup_rate:.2%}"
        )

    return dq

def check_referential_integrity( dq: DQResult,
                                fact_table: str, dim_table: str,
                                fact_key: str, dim_key: str) -> DQResult:
    """Every FK in fact_table must exist in dim_table."""
    fact_df = spark.read.format("delta").table(fact_table)
    dim_df  = spark.read.format("delta").table(dim_table)

    orphans = (
        fact_df.select(fact_key)
               .distinct()
               .join(dim_df.select(dim_key), fact_df[fact_key] == dim_df[dim_key], "left_anti")
               .count()
    )
    passed = orphans == 0
    dq.add(
        f"ref_integrity_{fact_table}_{fact_key}→{dim_table}_{dim_key}",
        passed,
        f"orphan_keys={orphans}"
    )
    return result


def check_value_range(df: DataFrame, dq: DQchecks,
                      column: str, min_val=None, max_val=None) -> DQchecks:
    """Values in column must fall within [min_val, max_val]."""
    condition = F.lit(True)
    if min_val is not None:
        condition = condition & (col(column) >= min_val)
    if max_val is not None:
        condition = condition & (col(column) <= max_val)
    out_of_range = df.filter(~condition).count()
    passed = out_of_range == 0
    dq.add(
        f"range_check_{column}", passed,
        f"out_of_range={out_of_range}, min={min_val}, max={max_val}"
    )
    return dq


def check_regex_pattern(df: DataFrame, dq: DQchecks,
                        column: str, pattern: str) -> DQchecks:
    """Values in column must match the given regex pattern."""
    invalid = df.filter(~F.col(column).rlike(pattern)).count()
    passed  = invalid == 0
    dq.add(
        f"regex_check_{column}", passed,
        f"invalid_count={invalid}, pattern={pattern}"
    )
    return dq



def standard_checks(
    df: DataFrame,
    table_name: str,
    non_null_cols: list,
    dedup_cols: list
) -> DQchecks:

    dq = DQchecks(table_name)

    check_row_count(df, dq)
    check_nulls(df, non_null_cols, dq)
    check_duplicates(df, dedup_cols, dq)

    print(dq.summary())

    return dq