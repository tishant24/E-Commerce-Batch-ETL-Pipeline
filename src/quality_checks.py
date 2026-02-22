from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from src.config import ETLConfig
from src.logger_setup import get_logger

logger = get_logger("QualityChecks")


EXPECTED_COLUMNS = [
    "invoice_id", "stock_code", "description", "quantity",
    "unit_price", "total_amount", "invoice_date", "invoice_year",
    "invoice_month", "invoice_day", "customer_id", "country",
    "is_return", "price_bucket"
]


def check_row_count(df: DataFrame, raw_count: int):
    """
    Compare processed row count to raw count.
    Fail if more than 80% of records were dropped (likely a pipeline bug).
    """
    processed_count = df.count()
    drop_pct = (raw_count - processed_count) / raw_count if raw_count > 0 else 0
    status = "PASS" if drop_pct < 0.80 else "FAIL"

    logger.info(f"Row Count Check | Raw: {raw_count:,} | Processed: {processed_count:,} | Drop%: {drop_pct:.1%} | {status}")
    return {
        "check_name": "row_count_validation",
        "metric_value": float(processed_count),
        "threshold": float(raw_count * 0.20),
        "status": status,
        "detail": f"Raw={raw_count:,} | Processed={processed_count:,} | Drop={drop_pct:.1%}"
    }


def check_null_percentage(df: DataFrame):
    """
    For each column, compute what percentage of values are null.
    Flag any column that exceeds the threshold (default 10%).
    """
    total_rows = df.count()
    results = []

    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = null_count / total_rows if total_rows > 0 else 0
        status = "PASS" if null_pct <= ETLConfig.MAX_NULL_THRESHOLD else "FAIL"

        results.append({
            "check_name": f"null_pct_{col_name}",
            "metric_value": round(null_pct * 100, 2),
            "threshold": ETLConfig.MAX_NULL_THRESHOLD * 100,
            "status": status,
            "detail": f"{col_name}: {null_count:,} nulls out of {total_rows:,} rows"
        })
        logger.info(f"Null Check [{col_name}]: {null_pct:.1%} | {status}")

    return results


def check_duplicates(df: DataFrame):
    """
    Count how many duplicate (invoice_id, stock_code) combinations exist
    after transformations. Should be near zero after dedup step.
    """
    total_count = df.count()
    dedup_count = df.dropDuplicates(ETLConfig.DEDUP_COLUMNS).count()
    dup_count = total_count - dedup_count
    dup_pct = dup_count / total_count if total_count > 0 else 0
    status = "PASS" if dup_pct <= ETLConfig.MAX_DUPLICATE_THRESHOLD else "FAIL"

    logger.info(f"Duplicate Check | Duplicates: {dup_count:,} | Pct: {dup_pct:.1%} | {status}")
    return {
        "check_name": "duplicate_count_check",
        "metric_value": float(dup_count),
        "threshold": float(ETLConfig.MAX_DUPLICATE_THRESHOLD * 100),
        "status": status,
        "detail": f"{dup_count:,} duplicates found ({dup_pct:.1%})"
    }


def check_schema(df: DataFrame):
    """
    Validate that all expected columns are present in the processed DataFrame.
    Also check for unexpected extra columns.
    """
    actual_cols = set(df.columns)
    expected_cols = set(EXPECTED_COLUMNS)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    status = "PASS" if not missing else "FAIL"

    detail_parts = []
    if missing:
        detail_parts.append(f"Missing: {sorted(missing)}")
    if extra:
        detail_parts.append(f"Extra: {sorted(extra)}")
    if not missing and not extra:
        detail_parts.append("Schema matches exactly")

    detail = " | ".join(detail_parts)
    logger.info(f"Schema Check | {status} | {detail}")

    return {
        "check_name": "schema_validation",
        "metric_value": float(len(actual_cols)),
        "threshold": float(len(expected_cols)),
        "status": status,
        "detail": detail
    }


def check_business_rules(df: DataFrame):
    """
    Custom business rule validations:
      - total_amount must always be positive (quantity * price > 0)
      - invoice_year should be within a reasonable range
    """
    total = df.count()

    negative_total = df.filter(F.col("total_amount") <= 0).count()
    neg_pct = negative_total / total if total > 0 else 0
    neg_status = "PASS" if neg_pct == 0 else "WARN"

    invalid_year = df.filter(
        (F.col("invoice_year") < 2009) | (F.col("invoice_year") > 2025)
    ).count()
    year_status = "PASS" if invalid_year == 0 else "FAIL"

    logger.info(f"Business Rule [total_amount > 0]: {neg_status} | Invalid: {negative_total:,}")
    logger.info(f"Business Rule [invoice_year range]: {year_status} | Invalid: {invalid_year:,}")

    return [
        {
            "check_name": "business_rule_total_amount_positive",
            "metric_value": float(negative_total),
            "threshold": 0.0,
            "status": neg_status,
            "detail": f"{negative_total:,} rows with total_amount <= 0"
        },
        {
            "check_name": "business_rule_invoice_year_range",
            "metric_value": float(invalid_year),
            "threshold": 0.0,
            "status": year_status,
            "detail": f"{invalid_year:,} rows outside year range 2009-2025"
        }
    ]


def build_report(spark: SparkSession, all_results: list) -> DataFrame:
    """
    Turn the list of check result dicts into a structured Spark DataFrame.
    This is the official Data Quality Report that can be saved to disk or MySQL.
    """
    report_schema = StructType([
        StructField("check_name",    StringType(), nullable=False),
        StructField("metric_value",  DoubleType(), nullable=True),
        StructField("threshold",     DoubleType(), nullable=True),
        StructField("status",        StringType(), nullable=False),
        StructField("detail",        StringType(), nullable=True),
    ])

    report_df = spark.createDataFrame(all_results, schema=report_schema)
    return report_df


def run_all_checks(spark: SparkSession, df: DataFrame, raw_count: int):
    """
    Orchestrate all quality checks and return the full validation report DataFrame.
    """
    logger.info("Running Data Quality Framework...")
    df.cache()

    results = []

    results.append(check_row_count(df, raw_count))
    results.extend(check_null_percentage(df))
    results.append(check_duplicates(df))
    results.append(check_schema(df))
    results.extend(check_business_rules(df))

    df.unpersist()

    report_df = build_report(spark, results)

    fail_count = sum(1 for r in results if r["status"] == "FAIL")
    warn_count = sum(1 for r in results if r["status"] == "WARN")
    overall_status = "PASSED" if fail_count == 0 else "FAILED"

    logger.info(f"Quality Report: {len(results)} checks | FAIL: {fail_count} | WARN: {warn_count} | Overall: {overall_status}")

    return report_df, overall_status
