# %% [markdown]
# # E-Commerce Batch ETL Pipeline with PySpark
#
# **Dataset**: Online Retail II — UCI Machine Learning Repository
# Free Download: https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx
#
# **Pipeline Overview**
# ```
# Raw CSV (500K rows)
#       |
#       v
#   [EXTRACT]  -> Explicit Schema (StructType) + FAILFAST
#       |
#       v
#   [TRANSFORM] -> Null handling, Dedup, Cast, Derived Cols, Filter
#       |
#       v
#   [QUALITY]   -> Row count, Null%, Duplicate%, Schema validation
#       |
#       v
#   [LOAD]      -> Parquet (partitioned) + MySQL (JDBC)
# ```

# %% [markdown]
# ## Step 0 — Imports and Setup

# %%
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, BooleanType
)

JOB_START = datetime.now()
print(f"Pipeline started at: {JOB_START.strftime('%Y-%m-%d %H:%M:%S')}")

# %% [markdown]
# ## Step 1 — Build SparkSession
#
# We use `local[*]` which means "use all available CPU cores on the local machine".
# In production this would be `yarn` or `k8s` cluster mode.

# %%
spark = (
    SparkSession.builder
    .appName("ecommerce_etl_pipeline")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
print(f"App name     : {spark.sparkContext.appName}")

# %% [markdown]
# ## Step 2 — Define Explicit Schema (StructType)
#
# Defining the schema explicitly instead of letting Spark infer it has 3 benefits:
# - Faster reads (no schema inference scan needed)
# - Strict type enforcement
# - FAILFAST mode works only when you define the schema

# %%
retail_schema = StructType([
    StructField("Invoice",     StringType(),  nullable=True),
    StructField("StockCode",   StringType(),  nullable=True),
    StructField("Description", StringType(),  nullable=True),
    StructField("Quantity",    IntegerType(), nullable=True),
    StructField("InvoiceDate", StringType(),  nullable=True),
    StructField("Price",       DoubleType(),  nullable=True),
    StructField("Customer ID", StringType(),  nullable=True),
    StructField("Country",     StringType(),  nullable=True),
])

print("Schema defined:")
retail_schema.simpleString()

# %% [markdown]
# ## Step 3 — Read Raw Data (PERMISSIVE mode for production, FAILFAST for dev)
#
# The Online Retail II dataset has ~1M rows.
# We limit to 500K to keep the demo fast.
#
# **How to get the dataset:**
# 1. Go to: https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci
# 2. Download online_retail_II.xlsx
# 3. Convert to CSV: `pd.read_excel('online_retail_II.xlsx').to_csv('online_retail_II.csv', index=False)`
# 4. Place in: `data/raw/online_retail_II.csv`

# %%
DATA_PATH = "../data/raw/online_retail_II.csv"

raw_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("escape", '"')
    .option("multiLine", "true")
    .schema(retail_schema)
    .csv(DATA_PATH)
)

raw_count = raw_df.count()
print(f"Raw records loaded : {raw_count:,}")
raw_df.printSchema()

# %% [markdown]
# ## Step 4 — Scale to 500K Records
#
# If the dataset is smaller than 500K, we union it with itself.
# If larger, we take a 500K sample.
# This simulates a production-scale batch.

# %%
TARGET = 500_000

if raw_count < TARGET:
    expanded = raw_df
    while expanded.count() < TARGET:
        expanded = expanded.union(raw_df)
    df = expanded.limit(TARGET)
    print(f"Expanded from {raw_count:,} to {df.count():,} records")
else:
    df = raw_df.limit(TARGET)
    print(f"Sampled down to {df.count():,} records")

# %% [markdown]
# ## Step 5 — Rename Columns to snake_case

# %%
df = (
    df
    .withColumnRenamed("Invoice",     "invoice_id")
    .withColumnRenamed("StockCode",   "stock_code")
    .withColumnRenamed("Description", "description")
    .withColumnRenamed("Quantity",    "quantity")
    .withColumnRenamed("InvoiceDate", "invoice_date_raw")
    .withColumnRenamed("Price",       "unit_price")
    .withColumnRenamed("Customer ID", "customer_id")
    .withColumnRenamed("Country",     "country")
)

print("Columns after rename:", df.columns)

# %% [markdown]
# ## Step 6 — Cast Columns
#
# InvoiceDate comes in as a string ("12/1/2009 08:26"), we cast it to Timestamp.

# %%
df = (
    df
    .withColumn(
        "invoice_date",
        F.to_timestamp(F.col("invoice_date_raw"), "M/d/yyyy H:mm")
    )
    .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .drop("invoice_date_raw")
)

df.printSchema()

# %% [markdown]
# ## Step 7 — Handle Nulls
#
# - Drop rows where critical columns (invoice_id, stock_code, quantity, unit_price) are null
# - Replace null customer_id with "GUEST" (these are guest/anonymous purchases)

# %%
before_null_drop = df.count()

df = df.dropna(subset=["invoice_id", "stock_code", "quantity", "unit_price"])

df = df.withColumn(
    "customer_id",
    F.when(F.col("customer_id").isNull(), F.lit("GUEST")).otherwise(F.col("customer_id"))
)

after_null_drop = df.count()
print(f"Rows before null handling : {before_null_drop:,}")
print(f"Rows after null handling  : {after_null_drop:,}")
print(f"Rows dropped              : {before_null_drop - after_null_drop:,}")

# %% [markdown]
# ## Step 8 — Deduplicate
#
# Remove rows where (invoice_id, stock_code) pair appears more than once.
# This handles duplicate records caused by system re-sends or data pipeline retries.

# %%
before_dedup = df.count()
df = df.dropDuplicates(["invoice_id", "stock_code"])
after_dedup = df.count()

print(f"Rows before dedup : {before_dedup:,}")
print(f"Rows after dedup  : {after_dedup:,}")
print(f"Duplicates removed: {before_dedup - after_dedup:,}")

# %% [markdown]
# ## Step 9 — Filter Invalid Records
#
# Business rules:
# - quantity must be > 0 (negative = return, we exclude those from the main table)
# - unit_price must be > 0
# - invoice_date must exist (not null after cast)
# - country cannot be null or "Unspecified"

# %%
before_filter = df.count()

df = df.filter(
    (F.col("quantity") > 0) &
    (F.col("unit_price") > 0) &
    (F.col("invoice_date").isNotNull()) &
    (F.col("country").isNotNull()) &
    (F.col("country") != "Unspecified")
)

after_filter = df.count()
print(f"Rows before filter : {before_filter:,}")
print(f"Rows after filter  : {after_filter:,}")
print(f"Invalid rows removed: {before_filter - after_filter:,}")

# %% [markdown]
# ## Step 10 — Add Derived Columns
#
# Derived columns are new columns computed from existing ones.
# These are useful for partitioning, reporting, and aggregations.

# %%
df = (
    df
    .withColumn("invoice_year",  F.year(F.col("invoice_date")))
    .withColumn("invoice_month", F.month(F.col("invoice_date")))
    .withColumn("invoice_day",   F.dayofmonth(F.col("invoice_date")))
    .withColumn("total_amount",  F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("is_return",     F.col("invoice_id").startswith("C"))
    .withColumn(
        "price_bucket",
        F.when(F.col("unit_price") < 1.0,   F.lit("under_1"))
         .when(F.col("unit_price") < 5.0,   F.lit("1_to_5"))
         .when(F.col("unit_price") < 20.0,  F.lit("5_to_20"))
         .when(F.col("unit_price") < 100.0, F.lit("20_to_100"))
         .otherwise(F.lit("above_100"))
    )
)

print("Final columns:")
df.printSchema()
df.show(5, truncate=False)

# %% [markdown]
# ## Step 11 — Broadcast Join Demo
#
# We have a tiny country -> region lookup table.
# Broadcasting it means Spark sends a copy to every executor,
# so the large retail DataFrame never needs to shuffle across the network.
# This is one of the biggest Spark performance wins for star-schema joins.

# %%
region_data = [
    ("United Kingdom", "Europe"),
    ("Germany",        "Europe"),
    ("France",         "Europe"),
    ("Australia",      "Asia-Pacific"),
    ("Japan",          "Asia-Pacific"),
    ("USA",            "North America"),
    ("Canada",         "North America"),
    ("Brazil",         "South America"),
    ("Sweden",         "Europe"),
    ("Netherlands",    "Europe"),
    ("EIRE",           "Europe"),
    ("Spain",          "Europe"),
    ("Switzerland",    "Europe"),
    ("Belgium",        "Europe"),
    ("Norway",         "Europe"),
]

region_df = spark.createDataFrame(region_data, ["country", "region"])

df = df.join(
    F.broadcast(region_df),
    on="country",
    how="left"
).fillna({"region": "Other"})

print("After broadcast join, added 'region' column.")
df.select("invoice_id", "country", "region").show(5)

# %% [markdown]
# ## Step 12 — Show Execution Plan (explain)
#
# explain() shows you how Spark will physically execute this query.
# Look for:
# - BroadcastHashJoin (confirms broadcast join worked)
# - Exchange (means data is being shuffled across nodes — expensive)
# - HashAggregate (aggregation happening on each partition before combine)

# %%
df.explain(mode="simple")

# %% [markdown]
# ## Step 13 — Data Quality Framework
#
# We cache the DataFrame before quality checks because we'll call
# count() and filter() multiple times. Without cache, Spark re-reads
# and re-computes everything from scratch for each action call.

# %%
print("Caching DataFrame for quality checks...")
df.cache()
df.count()  # materialize the cache

# --- Check 1: Row Count ---
total_rows = df.count()
print(f"Total processed rows: {total_rows:,}")

# --- Check 2: Null Percentage Per Column ---
print("\nNull Percentage per Column:")
null_report = []
for col_name in df.columns:
    null_ct = df.filter(F.col(col_name).isNull()).count()
    pct = round((null_ct / total_rows) * 100, 2) if total_rows > 0 else 0
    status = "PASS" if pct <= 10.0 else "FAIL"
    null_report.append((col_name, null_ct, pct, status))
    print(f"  {col_name:<25}: {pct:5.2f}% nulls  [{status}]")

# --- Check 3: Duplicate Count ---
dedup_count = df.dropDuplicates(["invoice_id", "stock_code"]).count()
dup_count = total_rows - dedup_count
dup_pct = round((dup_count / total_rows) * 100, 2) if total_rows > 0 else 0
dup_status = "PASS" if dup_pct <= 5.0 else "FAIL"
print(f"\nDuplicates: {dup_count:,} ({dup_pct}%) [{dup_status}]")

# --- Check 4: Schema Validation ---
expected_cols = {
    "invoice_id", "stock_code", "description", "quantity",
    "unit_price", "total_amount", "invoice_date", "invoice_year",
    "invoice_month", "invoice_day", "customer_id", "country",
    "is_return", "price_bucket", "region"
}
actual_cols = set(df.columns)
missing_cols = expected_cols - actual_cols
schema_status = "PASS" if not missing_cols else "FAIL"
print(f"\nSchema Check: {schema_status}")
if missing_cols:
    print(f"  Missing columns: {missing_cols}")

# --- Build Quality Report DataFrame ---
quality_rows = [
    ("row_count_validation",    float(total_rows),   500000.0, "PASS" if total_rows > 100000 else "FAIL", f"Total rows: {total_rows:,}"),
    ("duplicate_check",         float(dup_count),    0.05,     dup_status,                                f"{dup_pct}% duplicates"),
    ("schema_validation",       float(len(actual_cols)), float(len(expected_cols)), schema_status,        "All columns present" if not missing_cols else str(missing_cols)),
]

quality_schema = StructType([
    StructField("check_name",   StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("threshold",    DoubleType(), True),
    StructField("status",       StringType(), False),
    StructField("detail",       StringType(), True),
])

quality_df = spark.createDataFrame(quality_rows, schema=quality_schema)
print("\nData Quality Report:")
quality_df.show(truncate=False)

df.unpersist()
print("Cache released.")

# %% [markdown]
# ## Step 14 — Select Final Columns for Output

# %%
final_df = df.select(
    "invoice_id", "stock_code", "description", "quantity",
    "unit_price", "total_amount", "invoice_date", "invoice_year",
    "invoice_month", "invoice_day", "customer_id", "country",
    "region", "is_return", "price_bucket"
)

final_count = final_df.count()
print(f"Final record count: {final_count:,}")

# %% [markdown]
# ## Step 15 — Write to Parquet (partitioned by invoice_year)
#
# **Partition Strategy:**
# Partitioning by `invoice_year` means all 2009 records go into one folder,
# all 2010 records go into another, etc. When a downstream query filters by year,
# Spark skips all other partitions (partition pruning).
#
# `repartition(8, "invoice_year")` creates 8 equal files per partition,
# avoiding both the small-file problem and single-file bottlenecks.

# %%
PARQUET_PATH = "../data/processed/parquet/retail_data"

final_df_repartitioned = final_df.repartition(8, "invoice_year")

(
    final_df_repartitioned.write
    .mode("overwrite")
    .partitionBy("invoice_year")
    .parquet(PARQUET_PATH)
)

print(f"Parquet written to: {PARQUET_PATH}")
print("Partition folders created per invoice_year.")

# %% [markdown]
# ## Step 16 — Load to MySQL via JDBC
#
# Prerequisites:
# 1. MySQL running locally (or Docker: `docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root123 mysql:8`)
# 2. Create database: `CREATE DATABASE ecommerce_db;`
# 3. Run sql/create_tables.sql to create the table
# 4. Download mysql-connector-java-8.0.33.jar and place in jars/ folder

# %%
MYSQL_URL = "jdbc:mysql://localhost:3306/ecommerce_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
MYSQL_TABLE = "retail_transactions"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root123"
MYSQL_JAR = "../jars/mysql-connector-java-8.0.33.jar"

mysql_cols = [
    "invoice_id", "stock_code", "description", "quantity",
    "unit_price", "total_amount", "invoice_date", "invoice_year",
    "invoice_month", "invoice_day", "customer_id", "country",
    "is_return", "price_bucket"
]

df_for_mysql = final_df.select(mysql_cols)

jdbc_props = {
    "user":                     MYSQL_USER,
    "password":                 MYSQL_PASSWORD,
    "driver":                   "com.mysql.cj.jdbc.Driver",
    "batchsize":                "5000",
    "rewriteBatchedStatements": "true",
    "truncate":                 "true",
}

(
    df_for_mysql.write
    .mode("append")
    .jdbc(url=MYSQL_URL, table=MYSQL_TABLE, properties=jdbc_props)
)

print(f"Data loaded to MySQL table: {MYSQL_TABLE}")
print(f"Total records written: {final_count:,}")

# %% [markdown]
# ## Step 17 — Pipeline Summary Log

# %%
JOB_END = datetime.now()
duration_secs = (JOB_END - JOB_START).total_seconds()

print("=" * 60)
print("PIPELINE SUMMARY")
print("=" * 60)
print(f"Job Start Time          : {JOB_START.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Job End Time            : {JOB_END.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Duration          : {duration_secs:.2f} seconds")
print(f"Raw Records Loaded      : {before_null_drop:,}")
print(f"Records After Transform : {final_count:,}")
print(f"Duplicates Removed      : {before_dedup - after_dedup:,}")
print(f"Invalid Rows Filtered   : {before_filter - after_filter:,}")
print(f"Parquet Output Path     : {PARQUET_PATH}")
print(f"MySQL Table             : {MYSQL_TABLE}")
print(f"Quality Check Status    : {dup_status} / {schema_status}")
print("=" * 60)

spark.stop()
print("SparkSession stopped.")
