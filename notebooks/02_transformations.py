# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Batch ETL Pipeline
# MAGIC ### Notebook 02: Data Transformations & Cleaning
# MAGIC
# MAGIC **What happens here:**
# MAGIC 1. Null handling — fill or drop based on business rules
# MAGIC 2. Deduplication — keep only one copy of each order
# MAGIC 3. Column casting — convert strings to proper types
# MAGIC 4. Derived columns — extract year, month, order day of week
# MAGIC 5. Column renaming — clean naming conventions
# MAGIC 6. Filter invalid records — remove business-rule violations
# MAGIC 7. Cache the cleaned DataFrame for reuse

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, BooleanType, IntegerType, DoubleType
import logging

# We assume df_raw is available from Notebook 01
# On Databricks: use %run ./01_data_ingestion to chain notebooks
# Or re-load from temp view registered in Notebook 01

df_raw = spark.table("orders_raw")

print(f"Starting transformations on {df_raw.count():,} raw records...")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1: Null Handling
# MAGIC
# MAGIC **Strategy per column:**
# MAGIC - `category` → fill with "Unknown" (can't drop, too many nulls)
# MAGIC - `payment_method` → fill with "Unknown"
# MAGIC - `customer_country` → fill with "Unknown"
# MAGIC - `product_name` → fill with "Unknown Product"
# MAGIC - `discount_percent` → fill with 0.0 (no discount = 0)
# MAGIC - `is_premium_customer` → fill with "false" (default non-premium)
# MAGIC - `order_id` → DROP rows (we cannot process an order without an ID)
# MAGIC - `quantity` → DROP rows (we cannot process orders with no quantity)
# MAGIC - `unit_price` → DROP rows (price is mandatory for revenue calculation)

# COMMAND ----------

print("Step 1: Handling NULL values...")

# Before: check null counts per column
print("\nNull counts BEFORE cleaning:")
null_counts_before = df_raw.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_raw.columns
])
null_counts_before.show(truncate=False)

# Step 1a: Fill nulls for non-critical columns
df_nulls_filled = df_raw \
    .fillna({
        "category":            "Unknown",
        "payment_method":      "Unknown",
        "customer_country":    "Unknown",
        "product_name":        "Unknown Product",
        "discount_percent":    0.0,
        "is_premium_customer": "false",
        "shipping_cost":       0.0,
    })

# Step 1b: Drop rows where critical columns are null
before_drop = df_nulls_filled.count()
df_no_nulls = df_nulls_filled.dropna(
    subset=["order_id", "customer_id", "quantity", "unit_price", "order_date"]
)
after_drop = df_no_nulls.count()

print(f"\nRecords dropped due to null critical columns: {before_drop - after_drop:,}")
print(f"Records remaining: {after_drop:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2: Deduplication
# MAGIC
# MAGIC **Why duplicates exist in real data?**
# MAGIC - Double-click submissions on checkout forms
# MAGIC - Network retries that resend the same API request
# MAGIC - Kafka consumers processing the same message twice
# MAGIC
# MAGIC **Strategy:** Keep only the first occurrence of each `order_id`.
# MAGIC In production, you'd keep the record with the latest `updated_at` timestamp.
# MAGIC
# MAGIC We use `dropDuplicates(["order_id"])` — dedup based on ONE column.
# MAGIC This is smarter than `distinct()` which needs ALL columns to match.

# COMMAND ----------

print("Step 2: Deduplication...")

before_dedup = df_no_nulls.count()
df_deduped = df_no_nulls.dropDuplicates(["order_id"])
after_dedup = df_deduped.count()

print(f"  Duplicate rows removed: {before_dedup - after_dedup:,}")
print(f"  Records after dedup: {after_dedup:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3: Column Casting
# MAGIC
# MAGIC We loaded dates as `StringType` to avoid schema mismatch.
# MAGIC Now we properly cast them to `TimestampType`.
# MAGIC
# MAGIC Also:
# MAGIC - Cast `is_premium_customer` from string "true"/"false" → Boolean
# MAGIC - Cast `quantity` is already IntegerType from schema
# MAGIC
# MAGIC **Why cast timestamps carefully?**
# MAGIC Spark's `to_timestamp()` is lenient — if format doesn't match, it returns null
# MAGIC instead of failing. We check for nulls AFTER casting to catch issues.

# COMMAND ----------

print("Step 3: Casting columns to proper types...")

df_casted = df_deduped \
    .withColumn(
        "order_date",
        F.to_timestamp(F.col("order_date"), "yyyy-MM-dd HH:mm:ss")
    ) \
    .withColumn(
        "delivery_date",
        F.to_timestamp(F.col("delivery_date"), "yyyy-MM-dd HH:mm:ss")
    ) \
    .withColumn(
        "is_premium_customer",
        F.when(F.lower(F.col("is_premium_customer")) == "true", True)
         .otherwise(False)
         .cast(BooleanType())
    )

# Verify: how many nulls did the timestamp cast produce?
ts_nulls = df_casted.filter(F.col("order_date").isNull()).count()
print(f"  Rows with null order_date after cast: {ts_nulls:,}")

if ts_nulls > 0:
    df_casted = df_casted.dropna(subset=["order_date"])
    print(f"  Dropped {ts_nulls:,} rows with unparseable dates")

print(f"  Records after casting: {df_casted.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 4: Derived Columns
# MAGIC
# MAGIC Extracting year/month/day is extremely common in analytics.
# MAGIC These derived columns make GROUP BY and partitioning much easier later.
# MAGIC
# MAGIC We also:
# MAGIC - Calculate `delivery_days` = how long delivery took
# MAGIC - Create `revenue_band` = categorize orders by total amount

# COMMAND ----------

print("Step 4: Creating derived columns...")

df_derived = df_casted \
    .withColumn("order_year",       F.year("order_date")) \
    .withColumn("order_month",      F.month("order_date")) \
    .withColumn("order_day",        F.dayofmonth("order_date")) \
    .withColumn("order_quarter",    F.quarter("order_date")) \
    .withColumn("day_of_week",      F.dayofweek("order_date")) \
    .withColumn("order_month_name", F.date_format("order_date", "MMMM")) \
    .withColumn(
        "delivery_days",
        F.datediff(F.col("delivery_date"), F.col("order_date"))
    ) \
    .withColumn(
        "revenue_band",
        F.when(F.col("total_amount") < 500,   "Low")
         .when(F.col("total_amount") < 2000,  "Medium")
         .when(F.col("total_amount") < 5000,  "High")
         .otherwise("Premium")
    ) \
    .withColumn(
        "effective_price",
        F.round(
            F.col("unit_price") * (1 - F.col("discount_percent") / 100),
            2
        )
    )

new_cols = ["order_year", "order_month", "order_day", "order_quarter",
            "day_of_week", "order_month_name", "delivery_days",
            "revenue_band", "effective_price"]
print(f"  New columns added: {new_cols}")
df_derived.select(["order_id", "order_date"] + new_cols).show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 5: Column Renaming
# MAGIC
# MAGIC Standardizing column names makes it easier for downstream consumers.
# MAGIC Convention we use: `snake_case`, no abbreviations, self-explanatory names.

# COMMAND ----------

print("Step 5: Renaming columns to standard convention...")

df_renamed = df_derived \
    .withColumnRenamed("is_premium_customer", "is_premium") \
    .withColumnRenamed("discount_percent",    "discount_pct") \
    .withColumnRenamed("customer_country",    "country") \
    .withColumnRenamed("order_status",        "status")

print("  Renamed columns:")
print("    is_premium_customer → is_premium")
print("    discount_percent    → discount_pct")
print("    customer_country    → country")
print("    order_status        → status")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 6: Filter Invalid Records
# MAGIC
# MAGIC Business rules for valid orders:
# MAGIC 1. `quantity` must be >= 1 (you can't order 0 or negative items)
# MAGIC 2. `unit_price` must be > 0 (price can't be zero or negative)
# MAGIC 3. `delivery_days` must be >= 0 (delivery can't be before order date)
# MAGIC 4. `total_amount` must be > 0
# MAGIC
# MAGIC We log how many records each filter removes — important for audit trail.

# COMMAND ----------

print("Step 6: Filtering invalid records by business rules...")

counts = {}

counts["before_filter"] = df_renamed.count()

# Filter: quantity >= 1
df_valid_qty = df_renamed.filter(F.col("quantity") >= 1)
counts["invalid_quantity"] = counts["before_filter"] - df_valid_qty.count()

# Filter: unit_price > 0
df_valid_price = df_valid_qty.filter(F.col("unit_price") > 0)
counts["invalid_price"] = df_valid_qty.count() - df_valid_price.count()

# Filter: delivery_days >= 0
df_valid_delivery = df_valid_price.filter(
    F.col("delivery_days").isNull() | (F.col("delivery_days") >= 0)
)
counts["invalid_delivery"] = df_valid_price.count() - df_valid_delivery.count()

# Filter: total_amount > 0 (or null, we'll compute it)
df_valid_amount = df_valid_delivery.filter(
    F.col("total_amount").isNull() | (F.col("total_amount") > 0)
)
counts["invalid_amount"] = df_valid_delivery.count() - df_valid_amount.count()

# Recalculate total_amount where null using quantity * unit_price
df_clean = df_valid_amount.withColumn(
    "total_amount",
    F.when(F.col("total_amount").isNull(),
           F.round(F.col("quantity") * F.col("unit_price"), 2))
    .otherwise(F.col("total_amount"))
)

counts["final"] = df_clean.count()

print("\nFilter audit log:")
print(f"  Records before filter:         {counts['before_filter']:,}")
print(f"  Removed (invalid quantity):    {counts['invalid_quantity']:,}")
print(f"  Removed (invalid price):       {counts['invalid_price']:,}")
print(f"  Removed (invalid delivery):    {counts['invalid_delivery']:,}")
print(f"  Removed (invalid amount):      {counts['invalid_amount']:,}")
print(f"  Final clean records:           {counts['final']:,}")
print(f"  Retention rate: {counts['final']/counts['before_filter']*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Final Schema Preview
# MAGIC
# MAGIC Let's confirm the cleaned DataFrame looks right before passing to quality checks.

# COMMAND ----------

print("Final cleaned DataFrame schema:")
df_clean.printSchema()

print("\nSample data after all transformations:")
df_clean.show(5, truncate=True)

print(f"\nColumn count: {len(df_clean.columns)}")
print(f"Row count: {df_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Cache the Clean DataFrame
# MAGIC
# MAGIC **Why cache()?**
# MAGIC Spark is lazy — it recomputes the entire transformation chain every time
# MAGIC you call an action (.count(), .show(), .write()).
# MAGIC
# MAGIC `cache()` stores the result in memory so the next action is fast.
# MAGIC
# MAGIC **When to cache?**
# MAGIC - When you'll use the same DataFrame multiple times
# MAGIC - After expensive transformations (like our dedup + filter chain)
# MAGIC - Before writing to multiple destinations
# MAGIC
# MAGIC **When NOT to cache?**
# MAGIC - DataFrame used only once
# MAGIC - DataFrame is too large to fit in memory (use persist with disk instead)

# COMMAND ----------

print("Caching cleaned DataFrame...")
df_clean.cache()

# This triggers the actual computation and populates the cache
trigger_count = df_clean.count()
print(f"Cached! DataFrame has {trigger_count:,} rows stored in memory.")
print("Subsequent operations on df_clean will be much faster.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9: View Execution Plan
# MAGIC
# MAGIC `explain()` shows how Spark will actually execute your code.
# MAGIC
# MAGIC **Logical Plan** = what you asked for (your transformation steps)
# MAGIC **Physical Plan** = how Spark will actually execute it
# MAGIC
# MAGIC Look for:
# MAGIC - `InMemoryTableScan` → reading from cache ✅
# MAGIC - `Exchange` → a shuffle (data moving between nodes — expensive)
# MAGIC - `BroadcastHashJoin` → efficient join (no shuffle needed)

# COMMAND ----------

print("Execution plan for the cleaned DataFrame:")
print("(Simplified view — look for InMemoryTableScan showing cache is used)\n")

# Use False for simplified plan (True = verbose)
df_clean.explain(extended=False)

# COMMAND ----------

# Register for use in downstream notebooks
df_clean.createOrReplaceTempView("orders_clean")
print("Temp view 'orders_clean' registered for downstream notebooks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation Summary
# MAGIC
# MAGIC | Transformation | Technique Used | Business Reason |
# MAGIC |----------------|----------------|-----------------|
# MAGIC | Null handling | fillna + dropna | Preserve valid rows, fill sensible defaults |
# MAGIC | Deduplication | dropDuplicates(["order_id"]) | One order = one row |
# MAGIC | Type casting | to_timestamp(), cast(BooleanType()) | Correct types for analytics |
# MAGIC | Derived columns | year(), month(), datediff() | Enables time-series analysis |
# MAGIC | Column renaming | withColumnRenamed() | Consistent naming convention |
# MAGIC | Business filter | filter() with conditions | Remove physically impossible records |
# MAGIC | Cache | cache() | Speed up repeated reads |
# MAGIC
# MAGIC **Next:** Notebook 03 — Data Quality Framework
