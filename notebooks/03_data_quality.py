# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Batch ETL Pipeline
# MAGIC ### Notebook 03: Data Quality Framework
# MAGIC
# MAGIC **What is a Data Quality Framework?**
# MAGIC
# MAGIC In production, you cannot just load data and hope it's correct.
# MAGIC You need AUTOMATED checks that run every time the pipeline runs.
# MAGIC If checks fail, the pipeline should STOP and ALERT — not silently pass bad data.
# MAGIC
# MAGIC **Our DQ checks:**
# MAGIC 1. Row count validation (did we lose/gain rows unexpectedly?)
# MAGIC 2. Null percentage check (per critical column)
# MAGIC 3. Duplicate count check
# MAGIC 4. Schema validation (all expected columns present with right types)
# MAGIC 5. Business rule validation (value range checks)
# MAGIC
# MAGIC **Output:** A structured validation report DataFrame (like a test results table)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType, TimestampType
)
from datetime import datetime
import json

# Load the cleaned DataFrame from Notebook 02
df = spark.table("orders_clean")
print(f"Loaded 'orders_clean' with {df.count():,} rows, {len(df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Data Quality Check Engine
# MAGIC
# MAGIC We build a class that runs all checks and returns results as a DataFrame.
# MAGIC Each check produces a row: check_name, status (PASS/FAIL/WARN), value, threshold, message.
# MAGIC
# MAGIC **Why as a DataFrame?**
# MAGIC - You can save it to a table for audit history
# MAGIC - You can alert based on it (email if any FAIL rows exist)
# MAGIC - Recruiters love seeing structured output — it shows production thinking

# COMMAND ----------

class DataQualityChecker:
    """
    A modular Data Quality Framework for PySpark DataFrames.

    Design principles:
    - Each check is independent — one failure doesn't block others
    - All results collected in a list, then turned into a DataFrame
    - Thresholds are configurable — easy to adjust without code changes
    - PASS / WARN / FAIL status makes severity clear at a glance
    """

    def __init__(self, df, df_name: str = "orders_clean"):
        self.df       = df
        self.df_name  = df_name
        self.results  = []
        self.run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.row_count = df.count()  # cache this — used multiple times

        print(f"DQ Checker initialized for '{df_name}'")
        print(f"  Run time: {self.run_time}")
        print(f"  Total rows: {self.row_count:,}")
        print(f"  Total columns: {len(df.columns)}")
        print("-" * 60)

    def _add_result(self, check_name, status, actual_value,
                    threshold, message, check_category):
        """Internal method — adds one check result to our results list."""
        self.results.append({
            "check_id":       len(self.results) + 1,
            "run_timestamp":  self.run_time,
            "dataset_name":   self.df_name,
            "check_category": check_category,
            "check_name":     check_name,
            "status":         status,          # PASS / WARN / FAIL
            "actual_value":   float(actual_value),
            "threshold":      float(threshold) if threshold is not None else -1.0,
            "message":        message
        })
        icon = {"PASS": "✅", "WARN": "⚠️ ", "FAIL": "❌"}.get(status, "❓")
        print(f"  {icon} [{status}] {check_name}: {message}")

    # ------------------------------------------------------------------
    # CHECK 1: Row Count Validation
    # ------------------------------------------------------------------
    def check_row_count(self, min_expected: int = 400_000):
        """
        Validates that we have at least the minimum expected number of rows.
        We expect ~500K raw → after cleaning, at least 400K should survive.
        """
        print("\n[CHECK 1] Row Count Validation")

        if self.row_count >= min_expected:
            self._add_result(
                check_name="row_count_minimum",
                status="PASS",
                actual_value=self.row_count,
                threshold=min_expected,
                message=f"Row count {self.row_count:,} >= minimum {min_expected:,}",
                check_category="Volume"
            )
        elif self.row_count >= min_expected * 0.8:
            self._add_result(
                check_name="row_count_minimum",
                status="WARN",
                actual_value=self.row_count,
                threshold=min_expected,
                message=f"Row count {self.row_count:,} is between 80%-100% of expected {min_expected:,}",
                check_category="Volume"
            )
        else:
            self._add_result(
                check_name="row_count_minimum",
                status="FAIL",
                actual_value=self.row_count,
                threshold=min_expected,
                message=f"CRITICAL: Row count {self.row_count:,} is < 80% of expected {min_expected:,}",
                check_category="Volume"
            )

    # ------------------------------------------------------------------
    # CHECK 2: Null Percentage per Column
    # ------------------------------------------------------------------
    def check_null_percentages(self, columns_thresholds: dict = None):
        """
        For each critical column, check what % of rows have nulls.
        Threshold = max allowed null % for that column.

        columns_thresholds: dict of {column_name: max_null_percent}
        Example: {"order_id": 0.0, "category": 5.0}
        """
        print("\n[CHECK 2] Null Percentage Checks")

        if columns_thresholds is None:
            columns_thresholds = {
                "order_id":     0.0,   # Must never be null
                "customer_id":  0.0,   # Must never be null
                "order_date":   0.0,   # Must never be null
                "total_amount": 2.0,   # Allow up to 2% null
                "category":     5.0,   # Allow up to 5% null
                "country":      5.0,   # Allow up to 5% null
                "unit_price":   0.0,   # Must never be null
                "quantity":     0.0,   # Must never be null
            }

        for col_name, max_null_pct in columns_thresholds.items():
            if col_name not in self.df.columns:
                print(f"  ⚠️  Column '{col_name}' not found — skipping")
                continue

            null_count  = self.df.filter(F.col(col_name).isNull()).count()
            null_pct    = (null_count / self.row_count) * 100 if self.row_count > 0 else 0

            if null_pct <= max_null_pct:
                status  = "PASS"
                msg     = f"'{col_name}': {null_pct:.2f}% nulls (max allowed: {max_null_pct}%)"
            elif null_pct <= max_null_pct * 2:
                status  = "WARN"
                msg     = f"'{col_name}': {null_pct:.2f}% nulls approaching threshold {max_null_pct}%"
            else:
                status  = "FAIL"
                msg     = f"'{col_name}': {null_pct:.2f}% nulls EXCEEDS threshold {max_null_pct}%"

            self._add_result(
                check_name=f"null_pct_{col_name}",
                status=status,
                actual_value=round(null_pct, 4),
                threshold=max_null_pct,
                message=msg,
                check_category="Completeness"
            )

    # ------------------------------------------------------------------
    # CHECK 3: Duplicate Count Check
    # ------------------------------------------------------------------
    def check_duplicates(self, key_columns: list = None, max_dup_pct: float = 0.1):
        """
        Checks if there are any duplicate rows based on key columns.
        After our dedup step, this should be 0%.
        max_dup_pct: maximum allowed duplicate percentage (0.1 = 0.1%)
        """
        print("\n[CHECK 3] Duplicate Count Check")

        if key_columns is None:
            key_columns = ["order_id"]

        total_rows  = self.row_count
        unique_rows = self.df.dropDuplicates(key_columns).count()
        dup_count   = total_rows - unique_rows
        dup_pct     = (dup_count / total_rows) * 100 if total_rows > 0 else 0

        if dup_pct == 0:
            status = "PASS"
            msg    = f"No duplicates found on {key_columns} — {unique_rows:,} unique rows"
        elif dup_pct <= max_dup_pct:
            status = "WARN"
            msg    = f"{dup_count:,} duplicates ({dup_pct:.3f}%) on {key_columns} — within tolerance"
        else:
            status = "FAIL"
            msg    = f"{dup_count:,} duplicates ({dup_pct:.3f}%) on {key_columns} EXCEEDS {max_dup_pct}%"

        self._add_result(
            check_name="duplicate_check",
            status=status,
            actual_value=round(dup_pct, 4),
            threshold=max_dup_pct,
            message=msg,
            check_category="Uniqueness"
        )

    # ------------------------------------------------------------------
    # CHECK 4: Schema Validation
    # ------------------------------------------------------------------
    def check_schema(self, expected_schema: dict = None):
        """
        Validates that all expected columns exist with the expected data types.
        This catches upstream source changes (e.g., someone renamed a column).

        expected_schema: dict of {column_name: expected_dtype_string}
        """
        print("\n[CHECK 4] Schema Validation")

        if expected_schema is None:
            expected_schema = {
                "order_id":       "string",
                "customer_id":    "string",
                "product_id":     "string",
                "category":       "string",
                "quantity":       "int",
                "unit_price":     "double",
                "total_amount":   "double",
                "order_date":     "timestamp",
                "status":         "string",
                "is_premium":     "boolean",
                "order_year":     "int",
                "order_month":    "int",
                "revenue_band":   "string",
            }

        actual_schema = {field.name: field.dataType.simpleString()
                         for field in self.df.schema.fields}

        missing_cols   = []
        wrong_type_cols = []

        for col_name, expected_type in expected_schema.items():
            if col_name not in actual_schema:
                missing_cols.append(col_name)
            elif expected_type not in actual_schema[col_name]:
                wrong_type_cols.append(
                    f"{col_name} (expected: {expected_type}, got: {actual_schema[col_name]})"
                )

        if not missing_cols and not wrong_type_cols:
            self._add_result(
                check_name="schema_validation",
                status="PASS",
                actual_value=len(expected_schema),
                threshold=len(expected_schema),
                message=f"All {len(expected_schema)} expected columns present with correct types",
                check_category="Schema"
            )
        else:
            issues = []
            if missing_cols:
                issues.append(f"Missing columns: {missing_cols}")
            if wrong_type_cols:
                issues.append(f"Wrong types: {wrong_type_cols}")

            self._add_result(
                check_name="schema_validation",
                status="FAIL",
                actual_value=len(missing_cols) + len(wrong_type_cols),
                threshold=0,
                message=" | ".join(issues),
                check_category="Schema"
            )

    # ------------------------------------------------------------------
    # CHECK 5: Business Rule Validations
    # ------------------------------------------------------------------
    def check_business_rules(self):
        """
        Validates domain-specific business rules.
        These are the rules that Spark filters alone can't enforce at schema level.
        """
        print("\n[CHECK 5] Business Rule Validations")

        # Rule 5a: All quantities must be positive
        invalid_qty = self.df.filter(F.col("quantity") <= 0).count()
        self._add_result(
            check_name="quantity_positive",
            status="PASS" if invalid_qty == 0 else "FAIL",
            actual_value=invalid_qty,
            threshold=0,
            message=f"Rows with quantity <= 0: {invalid_qty:,}",
            check_category="BusinessRule"
        )

        # Rule 5b: All prices must be positive
        invalid_price = self.df.filter(F.col("unit_price") <= 0).count()
        self._add_result(
            check_name="unit_price_positive",
            status="PASS" if invalid_price == 0 else "FAIL",
            actual_value=invalid_price,
            threshold=0,
            message=f"Rows with unit_price <= 0: {invalid_price:,}",
            check_category="BusinessRule"
        )

        # Rule 5c: order_year should be between 2020 and current year
        current_year = datetime.now().year
        invalid_year = self.df.filter(
            (F.col("order_year") < 2020) | (F.col("order_year") > current_year)
        ).count()
        self._add_result(
            check_name="order_year_range",
            status="PASS" if invalid_year == 0 else "WARN",
            actual_value=invalid_year,
            threshold=0,
            message=f"Rows with order_year outside [2020, {current_year}]: {invalid_year:,}",
            check_category="BusinessRule"
        )

        # Rule 5d: discount_pct should be between 0 and 100
        invalid_discount = self.df.filter(
            (F.col("discount_pct") < 0) | (F.col("discount_pct") > 100)
        ).count()
        self._add_result(
            check_name="discount_pct_range",
            status="PASS" if invalid_discount == 0 else "FAIL",
            actual_value=invalid_discount,
            threshold=0,
            message=f"Rows with discount_pct outside [0, 100]: {invalid_discount:,}",
            check_category="BusinessRule"
        )

        # Rule 5e: status must be from known set
        valid_statuses = ["completed", "pending", "cancelled", "refunded", "processing"]
        invalid_status = self.df.filter(
            ~F.col("status").isin(valid_statuses)
        ).count()
        self._add_result(
            check_name="valid_order_status",
            status="PASS" if invalid_status == 0 else "WARN",
            actual_value=invalid_status,
            threshold=0,
            message=f"Rows with unknown status: {invalid_status:,} (allowed: {valid_statuses})",
            check_category="BusinessRule"
        )

    # ------------------------------------------------------------------
    # Generate Report DataFrame
    # ------------------------------------------------------------------
    def generate_report(self) -> "DataFrame":
        """
        Converts all check results into a Spark DataFrame.
        This is the final DQ report — save it, display it, act on it.
        """
        if not self.results:
            print("No checks have been run yet. Call check_*() methods first.")
            return None

        report_schema = StructType([
            StructField("check_id",       IntegerType(), False),
            StructField("run_timestamp",  StringType(),  False),
            StructField("dataset_name",   StringType(),  False),
            StructField("check_category", StringType(),  False),
            StructField("check_name",     StringType(),  False),
            StructField("status",         StringType(),  False),
            StructField("actual_value",   DoubleType(),  True),
            StructField("threshold",      DoubleType(),  True),
            StructField("message",        StringType(),  True),
        ])

        # Convert Python dicts to list of tuples matching schema order
        rows = [
            (r["check_id"], r["run_timestamp"], r["dataset_name"],
             r["check_category"], r["check_name"], r["status"],
             r["actual_value"], r["threshold"], r["message"])
            for r in self.results
        ]

        df_report = spark.createDataFrame(rows, schema=report_schema)
        return df_report

    def get_summary(self) -> dict:
        """Returns a summary dict: total checks, passed, warned, failed."""
        statuses = [r["status"] for r in self.results]
        return {
            "total_checks": len(self.results),
            "passed":       statuses.count("PASS"),
            "warned":       statuses.count("WARN"),
            "failed":       statuses.count("FAIL"),
            "overall":      "FAIL" if "FAIL" in statuses else
                            "WARN" if "WARN" in statuses else "PASS"
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run All DQ Checks

# COMMAND ----------

print("=" * 60)
print("RUNNING DATA QUALITY FRAMEWORK")
print("=" * 60)

dq = DataQualityChecker(df, df_name="orders_clean")

# Run all checks
dq.check_row_count(min_expected=400_000)
dq.check_null_percentages()
dq.check_duplicates(key_columns=["order_id"], max_dup_pct=0.1)
dq.check_schema()
dq.check_business_rules()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ Report — Structured Output

# COMMAND ----------

print("\n" + "=" * 60)
print("DATA QUALITY REPORT")
print("=" * 60)

df_dq_report = dq.generate_report()
df_dq_report.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ Summary

# COMMAND ----------

summary = dq.get_summary()
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Total Checks Run : {summary['total_checks']}")
print(f"  ✅ PASSED         : {summary['passed']}")
print(f"  ⚠️  WARNED         : {summary['warned']}")
print(f"  ❌ FAILED         : {summary['failed']}")
print(f"  Overall Status   : {summary['overall']}")
print("=" * 60)

if summary["overall"] == "FAIL":
    print("\n🚨 DQ FRAMEWORK DETECTED FAILURES — STOPPING PIPELINE")
    # In production: raise an exception or send an alert
    # raise Exception("DQ checks failed — pipeline aborted")
    print("   (In production, this would raise an exception and alert the team)")
elif summary["overall"] == "WARN":
    print("\n⚠️  DQ warnings detected. Proceeding but alerting data team.")
else:
    print("\n✅ All DQ checks passed. Proceeding to write stage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save DQ Report to Persistent Table
# MAGIC
# MAGIC In production, you'd save this to a Delta table.
# MAGIC On Community Edition, we save as a temp view for now.

# COMMAND ----------

df_dq_report.createOrReplaceTempView("dq_report")

# Save to DBFS as JSON for logging
dq_report_path = "/dbfs/FileStore/ecommerce_pipeline/dq_report/"
import os
os.makedirs(dq_report_path, exist_ok=True)

df_dq_report.coalesce(1).write.mode("overwrite").json(
    "dbfs:/FileStore/ecommerce_pipeline/dq_report/"
)
print(f"DQ report saved to: dbfs:/FileStore/ecommerce_pipeline/dq_report/")

# Summary stats for display
print("\nDQ Results by Category:")
spark.sql("""
    SELECT check_category,
           COUNT(*) AS total_checks,
           SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) AS passed,
           SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) AS warned,
           SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS failed
    FROM dq_report
    GROUP BY check_category
    ORDER BY check_category
""").show()

# COMMAND ----------

# Export summary for next notebook
dbutils.notebook.exit(json.dumps(summary))
