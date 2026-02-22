# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Batch ETL Pipeline
# MAGIC ### Notebook 04: Write to Parquet (Processed Layer)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Takes the cleaned DataFrame
# MAGIC 2. Repartitions it for optimal file sizes
# MAGIC 3. Writes to Parquet format partitioned by year and month
# MAGIC 4. Explains the partitioning strategy
# MAGIC 5. Reads back to verify the write was successful

# COMMAND ----------

from pyspark.sql import functions as F
import os

# Load clean data from temp view
df_clean = spark.table("orders_clean")
row_count = df_clean.count()
print(f"Loaded 'orders_clean': {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Repartition Before Write
# MAGIC
# MAGIC **The Problem:**
# MAGIC After transformations, your DataFrame might have 200 partitions (Spark default = 200).
# MAGIC If you have 450K rows across 200 partitions = ~2,250 rows per partition.
# MAGIC Each partition creates ONE file on disk → 200 tiny files → the "small files problem".
# MAGIC
# MAGIC **Why small files are bad:**
# MAGIC - HDFS/S3/DBFS has overhead per file (metadata, open/close operations)
# MAGIC - Reading 200 files is slower than reading 20 larger files
# MAGIC - Hive/Presto/Athena struggles with too many small files
# MAGIC
# MAGIC **Rule of thumb:**
# MAGIC Aim for ~128MB per file. With 450K rows at ~500 bytes each = ~225MB total.
# MAGIC So 2-4 partitions is ideal here.
# MAGIC
# MAGIC **repartition() vs coalesce():**
# MAGIC - `repartition(n)` = FULL SHUFFLE — redistributes all data evenly across n partitions
# MAGIC - `coalesce(n)` = NO SHUFFLE — only merges partitions, doesn't redistribute
# MAGIC - Use `repartition()` when increasing partitions or when you need even distribution
# MAGIC - Use `coalesce()` only when reducing partitions (faster, no shuffle)

# COMMAND ----------

print("Current partition count (before repartition):")
print(f"  Default partitions: {df_clean.rdd.getNumPartitions()}")

# Repartition to 4 — gives ~100-150MB per file for our 500K dataset
# Also repartition by partition column for better file organization
NUM_PARTITIONS = 4

df_repartitioned = df_clean.repartition(NUM_PARTITIONS, "order_year", "order_month")

print(f"  After repartition({NUM_PARTITIONS}): {df_repartitioned.rdd.getNumPartitions()} partitions")
print(f"  Strategy: Repartitioned by order_year and order_month columns")
print(f"  Result: Data for same year/month lands in same partition → better locality")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition Strategy Explained
# MAGIC
# MAGIC **We partition by: `order_year` / `order_month`**
# MAGIC
# MAGIC **On disk, this creates a folder structure like:**
# MAGIC ```
# MAGIC /processed/orders/
# MAGIC   order_year=2022/
# MAGIC     order_month=1/  → Jan 2022 data
# MAGIC     order_month=2/  → Feb 2022 data
# MAGIC     ...
# MAGIC   order_year=2023/
# MAGIC     order_month=1/  → Jan 2023 data
# MAGIC     ...
# MAGIC ```
# MAGIC
# MAGIC **Why this partition key?**
# MAGIC 1. Most analytics queries filter by date range → Spark skips irrelevant month folders (PARTITION PRUNING)
# MAGIC 2. Year/Month has good cardinality (not too many, not too few partitions)
# MAGIC 3. Aligns with how business teams report (monthly/yearly metrics)
# MAGIC
# MAGIC **Anti-pattern — don't partition by:**
# MAGIC - `order_id` → too many unique values → millions of folders
# MAGIC - `customer_country` alone → if most orders are from one country, uneven partition sizes
# MAGIC - Boolean columns → only 2 partition folders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Parquet

# COMMAND ----------

PROCESSED_PATH = "dbfs:/FileStore/ecommerce_pipeline/processed/orders/"

print(f"Writing to: {PROCESSED_PATH}")
print(f"Format: Parquet")
print(f"Partitioned by: order_year, order_month")
print(f"Mode: overwrite (replaces existing data — idempotent)")
print()

df_repartitioned.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(PROCESSED_PATH)

print("Write complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the Write — Read Back and Check

# COMMAND ----------

print("Reading back written Parquet to verify...")

df_parquet = spark.read.parquet(PROCESSED_PATH)

parquet_count = df_parquet.count()
print(f"\nVerification Results:")
print(f"  Written rows: {row_count:,}")
print(f"  Read-back rows: {parquet_count:,}")
print(f"  Match: {'✅ YES' if parquet_count == row_count else '❌ NO — MISMATCH!'}")
print(f"  Columns in Parquet: {len(df_parquet.columns)}")

# Check partition structure
print("\nPartitions found in Parquet file:")
spark.sql(f"""
    SELECT order_year, order_month, COUNT(*) AS row_count
    FROM parquet.`{PROCESSED_PATH}`
    GROUP BY order_year, order_month
    ORDER BY order_year, order_month
""").show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate Partition Pruning
# MAGIC
# MAGIC **Partition pruning** = Spark skips reading files it doesn't need.
# MAGIC When you filter on a partition column, Spark ONLY reads matching folders.
# MAGIC
# MAGIC Compare these two queries — the pruned one reads far less data:

# COMMAND ----------

print("Demonstrating partition pruning:")
print()

# This reads ALL partitions (no pruning)
print("Query 1: No partition filter (reads all data):")
df_parquet.filter(F.col("status") == "completed").explain(extended=False)

print()
print("Query 2: With partition filter (reads only year=2022, month=6):")
# This triggers partition pruning — only reads /order_year=2022/order_month=6/
df_parquet.filter(
    (F.col("order_year") == 2022) & (F.col("order_month") == 6)
).explain(extended=False)

print("(Look for 'PartitionFilters' in the physical plan above)")
print("(It shows Spark skipping most partition folders — much faster reads!)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Parquet Format?
# MAGIC
# MAGIC | Feature | CSV | JSON | Parquet |
# MAGIC |---------|-----|------|---------|
# MAGIC | Storage size | 100% | 120% | ~20-30% |
# MAGIC | Read speed | Slow | Slow | **Very fast** |
# MAGIC | Schema stored | No | No | **Yes** |
# MAGIC | Columnar | No | No | **Yes** |
# MAGIC | Compression | Manual | Manual | **Built-in** |
# MAGIC | Partition support | Limited | Limited | **Native** |
# MAGIC
# MAGIC **Columnar = Only reads columns you need**
# MAGIC `SELECT category, total_amount FROM orders` → reads only 2 columns, not all 25.
# MAGIC This is huge for analytics — most queries touch <20% of columns.

# COMMAND ----------

# Register parquet table as temp view
df_parquet.createOrReplaceTempView("orders_processed")

print("Registered 'orders_processed' temp view.")
print("Sample aggregation from Parquet data:")

spark.sql("""
    SELECT
        order_year,
        order_month,
        COUNT(*)                         AS total_orders,
        ROUND(SUM(total_amount), 2)      AS total_revenue,
        ROUND(AVG(total_amount), 2)      AS avg_order_value,
        COUNT(DISTINCT customer_id)      AS unique_customers
    FROM orders_processed
    GROUP BY order_year, order_month
    ORDER BY order_year, order_month
""").show(24)

# COMMAND ----------

dbutils.notebook.exit("parquet_write_complete")
