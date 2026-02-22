# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Batch ETL Pipeline
# MAGIC ### Notebook 06: Spark Optimizations
# MAGIC
# MAGIC **What we cover:**
# MAGIC 1. repartition() vs coalesce() — when to use each
# MAGIC 2. cache() and persist() — memory vs disk
# MAGIC 3. explain() — reading execution plans
# MAGIC 4. Broadcast Join — how to eliminate shuffle for small tables
# MAGIC 5. Adaptive Query Execution (AQE) — Spark 3's auto-optimizer
# MAGIC
# MAGIC These concepts are frequently asked in Data Engineering interviews.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import DataFrame
import time

df_orders = spark.table("orders_processed")
print(f"Working with {df_orders.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization 1: repartition() Deep Dive
# MAGIC
# MAGIC **repartition(n):**
# MAGIC - Performs a full SHUFFLE (moves data across all workers)
# MAGIC - Produces EXACTLY n partitions with roughly equal data
# MAGIC - Use when you need to INCREASE partition count or redistribute data evenly
# MAGIC - Expensive operation — do it once, not repeatedly
# MAGIC
# MAGIC **coalesce(n):**
# MAGIC - NO shuffle — just merges adjacent partitions
# MAGIC - Produces AT MOST n partitions (can't guarantee exact count)
# MAGIC - Use only to DECREASE partition count
# MAGIC - Cheap — no data movement across network

# COMMAND ----------

print("=== repartition() vs coalesce() Demo ===\n")

# Check initial partitions
initial_parts = df_orders.rdd.getNumPartitions()
print(f"Initial partition count: {initial_parts}")

# repartition — full shuffle, evenly distributed
start = time.time()
df_repartitioned = df_orders.repartition(8)
# Note: getNumPartitions() is instant (doesn't trigger computation)
repartition_parts = df_repartitioned.rdd.getNumPartitions()
print(f"\nAfter repartition(8): {repartition_parts} partitions")
print("  → Full shuffle: data distributed evenly across 8 partitions")
print("  → Good for: before expensive GROUP BY, JOIN operations")

# coalesce — no shuffle, just merges
df_coalesced = df_orders.coalesce(2)
coalesce_parts = df_coalesced.rdd.getNumPartitions()
print(f"\nAfter coalesce(2): {coalesce_parts} partitions")
print("  → No shuffle: just merges nearby partitions")
print("  → Good for: final write to reduce small output files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization 2: cache() and persist()
# MAGIC
# MAGIC **cache()** = shortcut for `persist(StorageLevel.MEMORY_AND_DISK)`
# MAGIC
# MAGIC **Storage levels:**
# MAGIC | Level | Memory | Disk | Serialized | Use When |
# MAGIC |-------|--------|------|------------|----------|
# MAGIC | MEMORY_ONLY | ✅ | ❌ | No | Small DF, fast RAM available |
# MAGIC | MEMORY_AND_DISK | ✅ | ✅ | No | Default — good balance |
# MAGIC | DISK_ONLY | ❌ | ✅ | Yes | Very large DF, memory constrained |
# MAGIC | OFF_HEAP | ✅ off-heap | ❌ | Yes | Avoid GC pauses |

# COMMAND ----------

from pyspark import StorageLevel

print("=== cache() and persist() Demo ===\n")

# cache() for a DataFrame we'll use multiple times
df_orders.cache()

# First action after cache — triggers computation AND populates cache
start = time.time()
count1 = df_orders.count()
elapsed1 = time.time() - start
print(f"First count() (builds cache): {count1:,} rows in {elapsed1:.2f}s")

# Second action — reads from cache (much faster)
start = time.time()
count2 = df_orders.count()
elapsed2 = time.time() - start
print(f"Second count() (from cache): {count2:,} rows in {elapsed2:.2f}s")
print(f"Speedup: {elapsed1/max(elapsed2,0.001):.1f}x faster from cache")

# Unpersist when done — free memory for other operations
df_orders.unpersist()
print("\ndf_orders unpersisted — memory freed")

# Custom persist level example
df_orders_persisted = df_orders.persist(StorageLevel.MEMORY_AND_DISK)
_ = df_orders_persisted.count()  # trigger caching
print(f"Persisted with MEMORY_AND_DISK: {df_orders_persisted.rdd.getNumPartitions()} partitions")
df_orders_persisted.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization 3: Understanding explain()
# MAGIC
# MAGIC `explain()` shows Spark's execution plan.
# MAGIC Reading it helps you understand:
# MAGIC - WHY a query is slow (look for expensive Exchange/Shuffle nodes)
# MAGIC - WHETHER your cache is being used (look for InMemoryTableScan)
# MAGIC - HOW joins are being executed (BroadcastHashJoin vs SortMergeJoin)

# COMMAND ----------

print("=== explain() — Reading the Execution Plan ===\n")

# Simple aggregation plan
query = df_orders \
    .filter(F.col("order_year") == 2022) \
    .groupBy("category") \
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue")
    )

print("Plan for: filter(year=2022) → groupBy(category) → agg(count, sum)")
print("-" * 60)
query.explain(extended=False)
print()
print("Key things to look for in the plan:")
print("  FileScan → Reading source data")
print("  Filter → WHERE clause (pushed down = efficient)")
print("  HashAggregate → groupBy + agg (2-phase: partial then final)")
print("  Exchange → SHUFFLE (most expensive operation in Spark)")
print("  Sort → ORDER BY")
print("  InMemoryTableScan → Reading from cache (efficient)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization 4: Broadcast Join
# MAGIC
# MAGIC **The problem with regular joins:**
# MAGIC When joining two large tables, Spark does a SortMergeJoin:
# MAGIC 1. Sort both tables by join key
# MAGIC 2. Merge them — requires SHUFFLING data across all workers
# MAGIC Shuffle = most expensive operation in Spark (massive network I/O)
# MAGIC
# MAGIC **Broadcast Join solution:**
# MAGIC If one table is small (< 10MB by default, configurable):
# MAGIC 1. Send the SMALL table to every worker (broadcast)
# MAGIC 2. Each worker does the join locally — NO SHUFFLE!
# MAGIC This can be 10-100x faster than SortMergeJoin.

# COMMAND ----------

print("=== Broadcast Join Demo ===\n")

# Create a small lookup table (category metadata)
# In real pipelines: product catalog, country codes, channel configs
category_data = [
    ("Electronics",    "Technology",   0.08, True),
    ("Clothing",       "Fashion",      0.05, True),
    ("Books",          "Education",    0.00, False),
    ("Home & Garden",  "Lifestyle",    0.05, True),
    ("Sports",         "Fitness",      0.05, True),
    ("Toys",           "Kids",         0.05, True),
    ("Beauty",         "Personal Care",0.05, False),
    ("Automotive",     "Vehicles",     0.08, True),
    ("Food",           "Grocery",      0.00, True),
    ("Unknown",        "Uncategorized",0.05, False),
]

category_schema = StructType([
    StructField("category",        StringType(),  True),
    StructField("department",      StringType(),  True),
    StructField("tax_rate",        DoubleType(),  True),
    StructField("needs_shipping",  BooleanType(), True),
])

from pyspark.sql.types import BooleanType

df_category_lookup = spark.createDataFrame(category_data, schema=category_schema)
print(f"Small lookup table size: {df_category_lookup.count()} rows")
df_category_lookup.show()

# COMMAND ----------

# Regular join (SortMergeJoin — requires shuffle)
print("Regular Join (SortMergeJoin — requires shuffle):")
df_regular_join = df_orders.join(df_category_lookup, on="category", how="left")
df_regular_join.explain(extended=False)

# COMMAND ----------

# Broadcast join (no shuffle!)
print("\nBroadcast Join (no shuffle — small table sent to all workers):")
df_broadcast_join = df_orders.join(
    F.broadcast(df_category_lookup),  # Tell Spark: broadcast this small table
    on="category",
    how="left"
)
df_broadcast_join.explain(extended=False)

print("\nLook for 'BroadcastHashJoin' in the second plan — no Exchange/shuffle!")
print("Look for 'SortMergeJoin' + 'Exchange' in the first plan — shuffle present.")

# COMMAND ----------

# Verify the join worked
print("\nSample result with broadcast join enrichment:")
df_broadcast_join.select(
    "order_id", "category", "department", "tax_rate", "total_amount"
).show(5, truncate=False)

# Cache this enriched DataFrame — we'll use it for final aggregations
df_enriched = df_broadcast_join.cache()
_ = df_enriched.count()
print(f"\nEnriched DataFrame cached: {df_enriched.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization 5: Adaptive Query Execution (AQE)
# MAGIC
# MAGIC **AQE (Spark 3.0+) automatically:**
# MAGIC 1. Converts SortMergeJoin → BroadcastHashJoin when possible (detects small tables at runtime)
# MAGIC 2. Coalesces shuffle partitions (reduces the 200-partition default intelligently)
# MAGIC 3. Skews join optimization (handles uneven data distribution automatically)
# MAGIC
# MAGIC It's like having an auto-tuner that fixes common Spark issues at runtime.

# COMMAND ----------

# Check if AQE is enabled (it is by default in Databricks)
aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
coalesce_enabled = spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")

print("=== Adaptive Query Execution (AQE) Status ===\n")
print(f"  AQE enabled:                 {aqe_enabled}")
print(f"  Auto coalesce partitions:    {coalesce_enabled}")
print()
print("AQE Benefits:")
print("  1. Auto-converts SMJ → BHJ when runtime stats show small table")
print("  2. Reduces 200 default shuffle partitions to actual needed count")
print("  3. Handles skewed joins (one partition much larger than others)")
print()
print("Best practice: Always keep AQE enabled in Spark 3+")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Analytics Query Using All Optimizations

# COMMAND ----------

print("=== Final Optimized Analytics Query ===\n")

# This query uses:
# 1. Cached df_enriched (no recomputation)
# 2. Broadcast join already done (no shuffle in plan)
# 3. AQE running in background
# 4. Partition pruning on year/month

df_analytics = df_enriched \
    .filter(F.col("order_status") == "completed") \
    .groupBy("order_year", "order_month", "department", "revenue_band") \
    .agg(
        F.count("*").alias("order_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.sum("total_amount"), 2).alias("gross_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.sum(F.col("is_premium_customer")).alias("premium_orders")
    ) \
    .orderBy("order_year", "order_month", "gross_revenue", ascending=[True, True, False])

print("Analytics aggregation result:")
df_analytics.show(20, truncate=False)

# Save analytics to Parquet
ANALYTICS_PATH = "dbfs:/FileStore/ecommerce_pipeline/curated/analytics_summary/"
df_analytics.coalesce(1).write.mode("overwrite").parquet(ANALYTICS_PATH)
print(f"\nAnalytics saved to: {ANALYTICS_PATH}")

# COMMAND ----------

# Register for Streamlit/reporting use
df_analytics.createOrReplaceTempView("analytics_summary")

print("\n=== Optimization Summary ===")
print(f"{'Technique':<25} {'What it does':<40} {'When to use'}")
print("-" * 90)
print(f"{'repartition(n)':<25} {'Full shuffle, equal distribution':<40} {'Before joins/groupBy on large data'}")
print(f"{'coalesce(n)':<25} {'Merge partitions, no shuffle':<40} {'Before writing to reduce files'}")
print(f"{'cache()':<25} {'Store in memory for reuse':<40} {'Multi-use DataFrame after transforms'}")
print(f"{'persist(level)':<25} {'Custom storage (mem/disk/both)':<40} {'When memory is limited'}")
print(f"{'explain()':<25} {'Show execution plan':<40} {'Debugging slow queries'}")
print(f"{'broadcast(df)':<25} {'Eliminate join shuffle':<40} {'Small lookup tables (< 10MB)'}")
print(f"{'AQE':<25} {'Auto-optimize at runtime':<40} {'Always — leave enabled'}")

dbutils.notebook.exit("optimizations_complete")
