"""
Unit Tests for ETL Transformations
===================================
Tests the core transformation logic without needing a real Databricks cluster.
Run locally: pytest tests/test_transformations.py -v

Note: These tests use a local SparkSession.
On Databricks, the same logic runs distributed.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, BooleanType
)


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    return SparkSession.builder \
        .appName("ETL_Unit_Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


@pytest.fixture(scope="session")
def sample_schema():
    return StructType([
        StructField("order_id",     StringType(),  True),
        StructField("customer_id",  StringType(),  True),
        StructField("category",     StringType(),  True),
        StructField("quantity",     IntegerType(), True),
        StructField("unit_price",   DoubleType(),  True),
        StructField("total_amount", DoubleType(),  True),
        StructField("order_date",   StringType(),  True),
        StructField("order_status", StringType(),  True),
    ])


@pytest.fixture(scope="session")
def sample_df(spark, sample_schema):
    """Sample DataFrame with known data for testing."""
    data = [
        ("ORD-001", "CUST-A", "Electronics", 2, 500.0, 1000.0, "2022-06-15 10:30:00", "completed"),
        ("ORD-002", "CUST-B", None,           1, 200.0,  200.0, "2022-07-20 14:00:00", "pending"),
        ("ORD-001", "CUST-A", "Electronics", 2, 500.0, 1000.0, "2022-06-15 10:30:00", "completed"),  # duplicate
        ("ORD-003", "CUST-C", "Clothing",   -1, 150.0, -150.0, "2022-08-10 09:15:00", "completed"),  # invalid qty
        ("ORD-004", "CUST-D", "Books",        3,  -50.0, None,  "2022-09-01 12:00:00", "refunded"),  # invalid price
        (None,       "CUST-E", "Sports",       1, 300.0,  300.0, "2022-10-05 16:00:00", "completed"), # null order_id
    ]
    return spark.createDataFrame(data, schema=sample_schema)


class TestNullHandling:
    def test_fill_category_null(self, spark, sample_schema, sample_df):
        """fillna should replace null categories with 'Unknown'."""
        df_filled = sample_df.fillna({"category": "Unknown"})
        null_count = df_filled.filter(F.col("category").isNull()).count()
        assert null_count == 0, "All null categories should be filled"

        unknown_count = df_filled.filter(F.col("category") == "Unknown").count()
        assert unknown_count == 1, "Exactly one row should have 'Unknown' category"

    def test_drop_null_order_id(self, sample_df):
        """dropna on order_id should remove rows with null order_id."""
        df_dropped = sample_df.dropna(subset=["order_id"])
        assert df_dropped.count() == 5, "One null order_id row should be dropped"
        null_ids = df_dropped.filter(F.col("order_id").isNull()).count()
        assert null_ids == 0, "No null order_ids should remain"


class TestDeduplication:
    def test_dedup_on_order_id(self, sample_df):
        """dropDuplicates on order_id should remove duplicate ORD-001."""
        df_deduped = sample_df.dropDuplicates(["order_id"])
        # ORD-001 appears twice — after dedup should appear once
        ord_001_count = df_deduped.filter(F.col("order_id") == "ORD-001").count()
        assert ord_001_count == 1, "ORD-001 should appear exactly once after dedup"

    def test_total_row_count_after_dedup(self, sample_df):
        """After dedup, row count should decrease by number of duplicates."""
        original_count  = sample_df.count()           # 6 rows
        deduped_count   = sample_df.dropDuplicates(["order_id"]).count()
        assert deduped_count < original_count, "Dedup should reduce row count"

    def test_dedup_preserves_unique_rows(self, sample_df):
        """Unique order_ids should not be affected by dedup."""
        df_deduped = sample_df.dropDuplicates(["order_id"])
        for order_id in ["ORD-002", "ORD-003", "ORD-004"]:
            count = df_deduped.filter(F.col("order_id") == order_id).count()
            assert count == 1, f"{order_id} should still exist after dedup"


class TestColumnCasting:
    def test_timestamp_cast(self, sample_df):
        """order_date string should cast to timestamp without nulls."""
        df_casted = sample_df.withColumn(
            "order_date_ts",
            F.to_timestamp(F.col("order_date"), "yyyy-MM-dd HH:mm:ss")
        )
        null_ts = df_casted.filter(F.col("order_date_ts").isNull()).count()
        assert null_ts == 0, "No nulls expected after timestamp cast with correct format"

    def test_boolean_cast(self, spark):
        """String 'true'/'false' should cast correctly to Boolean."""
        test_data = [("ORD-X", "true"), ("ORD-Y", "false"), ("ORD-Z", None)]
        df = spark.createDataFrame(test_data, ["order_id", "is_premium_str"])
        df_casted = df.withColumn(
            "is_premium",
            F.when(F.lower(F.col("is_premium_str")) == "true", True)
             .otherwise(False)
             .cast(BooleanType())
        )
        row = df_casted.filter(F.col("order_id") == "ORD-X").first()
        assert row["is_premium"] == True, "String 'true' should cast to Boolean True"

        row = df_casted.filter(F.col("order_id") == "ORD-Y").first()
        assert row["is_premium"] == False, "String 'false' should cast to Boolean False"


class TestDerivedColumns:
    def test_year_extraction(self, sample_df):
        """order_year should be correctly extracted from order_date."""
        df = sample_df.withColumn(
            "order_date_ts", F.to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss")
        ).withColumn("order_year", F.year("order_date_ts"))

        row = df.filter(F.col("order_id") == "ORD-001").first()
        assert row["order_year"] == 2022, "Year should be 2022 for 2022-06-15"

    def test_month_extraction(self, sample_df):
        """order_month should be correctly extracted."""
        df = sample_df.withColumn(
            "order_date_ts", F.to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss")
        ).withColumn("order_month", F.month("order_date_ts"))

        row = df.filter(F.col("order_id") == "ORD-001").first()
        assert row["order_month"] == 6, "Month should be 6 for 2022-06-15"

    def test_revenue_band_low(self, spark):
        """Total amount < 500 should be 'Low' revenue band."""
        df = spark.createDataFrame(
            [("ORD-T1", 200.0), ("ORD-T2", 1500.0), ("ORD-T3", 3500.0), ("ORD-T4", 6000.0)],
            ["order_id", "total_amount"]
        )
        df = df.withColumn(
            "revenue_band",
            F.when(F.col("total_amount") < 500, "Low")
             .when(F.col("total_amount") < 2000, "Medium")
             .when(F.col("total_amount") < 5000, "High")
             .otherwise("Premium")
        )
        bands = {row["order_id"]: row["revenue_band"] for row in df.collect()}
        assert bands["ORD-T1"] == "Low",     "200 should be Low"
        assert bands["ORD-T2"] == "Medium",  "1500 should be Medium"
        assert bands["ORD-T3"] == "High",    "3500 should be High"
        assert bands["ORD-T4"] == "Premium", "6000 should be Premium"


class TestBusinessRuleFiltering:
    def test_filter_negative_quantity(self, sample_df):
        """Rows with quantity <= 0 should be removed."""
        df_filtered = sample_df.filter(F.col("quantity") >= 1)
        invalid = df_filtered.filter(F.col("quantity") <= 0).count()
        assert invalid == 0, "No rows with quantity <= 0 should remain"

    def test_filter_negative_price(self, sample_df):
        """Rows with unit_price <= 0 should be removed."""
        df_filtered = sample_df.filter(F.col("unit_price") > 0)
        invalid = df_filtered.filter(F.col("unit_price") <= 0).count()
        assert invalid == 0, "No rows with unit_price <= 0 should remain"

    def test_valid_orders_preserved(self, sample_df):
        """Valid orders (ORD-001, ORD-002) should survive all filters."""
        df_filtered = sample_df \
            .filter(F.col("quantity") >= 1) \
            .filter(F.col("unit_price") > 0) \
            .dropna(subset=["order_id"])

        valid_ids = [row["order_id"] for row in df_filtered.select("order_id").collect()]
        assert "ORD-001" in valid_ids, "ORD-001 is valid and should be preserved"
        assert "ORD-002" in valid_ids, "ORD-002 is valid and should be preserved"
        assert "ORD-003" not in valid_ids, "ORD-003 has invalid quantity — should be removed"
        assert "ORD-004" not in valid_ids, "ORD-004 has invalid price — should be removed"


class TestColumnRenaming:
    def test_rename_columns(self, sample_df):
        """Renamed columns should exist with new names, old names removed."""
        df_renamed = sample_df \
            .withColumnRenamed("order_status", "status") \
            .withColumnRenamed("category", "product_category")

        assert "status" in df_renamed.columns, "Renamed column 'status' should exist"
        assert "order_status" not in df_renamed.columns, "Old name 'order_status' should not exist"
        assert "product_category" in df_renamed.columns, "Renamed 'product_category' should exist"
