# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Batch ETL Pipeline
# MAGIC ### Notebook 01: Data Ingestion & Schema Definition
# MAGIC
# MAGIC **Author:** [Your Name]
# MAGIC **Last Updated:** 2024-01-15
# MAGIC **Environment:** Databricks Community Edition
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC 1. Generates a synthetic e-commerce dataset with 500K+ records
# MAGIC 2. Defines a strict schema using StructType
# MAGIC 3. Loads raw CSV data with FAILFAST mode
# MAGIC 4. Does a basic preview of what landed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import all libraries we need
# MAGIC Think of this like gathering all your tools before starting a project.

# COMMAND ----------

import uuid
import random
import csv
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
    TimestampType, BooleanType, LongType
)
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Initialize Spark Session
# MAGIC
# MAGIC On Databricks, SparkSession is already available as `spark`.
# MAGIC But we write this so the code also runs locally for testing.

# COMMAND ----------

# On Databricks: spark is already available
# Locally: this block creates a spark session
try:
    spark
    print("Running on Databricks — SparkSession already available.")
except NameError:
    spark = SparkSession.builder \
        .appName("ECommerceETLPipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    print("Running locally — SparkSession created.")

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Generate Synthetic Dataset (500K Records)
# MAGIC
# MAGIC Real-world projects pull from APIs, databases, or S3.
# MAGIC Here we generate realistic-looking e-commerce order data.
# MAGIC
# MAGIC **Why 500K?** Large enough to demonstrate Spark's distributed power,
# MAGIC small enough to run free on Databricks Community Edition.

# COMMAND ----------

def generate_ecommerce_data(num_records: int = 500_000) -> str:
    """
    Generates a synthetic e-commerce orders CSV file.
    Intentionally includes:
      - Some null values (real data is messy)
      - Duplicate order_ids (to test deduplication)
      - Invalid amounts (negative prices — business rule violation)
      - Mixed date formats (edge case handling)

    Returns the DBFS file path where CSV is saved.
    """

    CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden",
                  "Sports", "Toys", "Beauty", "Automotive", "Food", None]

    PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal",
                       "UPI", "Net Banking", "COD", None]

    STATUSES = ["completed", "pending", "cancelled", "refunded", "processing"]

    COUNTRIES = ["India", "USA", "UK", "Germany", "Australia",
                 "Canada", "France", "Japan", "Brazil", None]

    output_path = "/dbfs/FileStore/ecommerce_raw/orders_raw.csv"
    os.makedirs("/dbfs/FileStore/ecommerce_raw", exist_ok=True)

    base_date = datetime(2022, 1, 1)

    # Pre-generate some order_ids that will be duplicated (5% duplicates)
    dup_ids = [str(uuid.uuid4()) for _ in range(int(num_records * 0.05))]

    print(f"Generating {num_records:,} records... This takes ~2-3 minutes.")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id", "customer_id", "product_id", "product_name",
            "category", "quantity", "unit_price", "total_amount",
            "order_date", "delivery_date", "payment_method",
            "order_status", "customer_country", "is_premium_customer",
            "discount_percent", "shipping_cost"
        ])

        for i in range(num_records):
            # 5% chance of using a duplicate order_id
            if random.random() < 0.05 and dup_ids:
                order_id = random.choice(dup_ids)
            else:
                order_id = str(uuid.uuid4())

            customer_id = f"CUST_{random.randint(1000, 99999)}"
            product_id  = f"PROD_{random.randint(100, 9999)}"

            product_name = random.choice([
                "Wireless Headphones", "Running Shoes", "Python Book",
                "Coffee Maker", "Yoga Mat", "Smart Watch", "Laptop Stand",
                "Desk Lamp", "Protein Powder", "Travel Bag", None
            ])

            category         = random.choice(CATEGORIES)
            quantity         = random.choice([None, -1, 0, 1, 2, 3, 5, 10])
            unit_price       = round(random.uniform(-50, 5000), 2)  # some negatives!
            total_amount     = round(quantity * unit_price, 2) if quantity and quantity > 0 else None
            order_dt         = base_date + timedelta(days=random.randint(0, 730))
            delivery_dt      = order_dt + timedelta(days=random.randint(1, 15))
            payment_method   = random.choice(PAYMENT_METHODS)
            order_status     = random.choice(STATUSES)
            country          = random.choice(COUNTRIES)
            is_premium       = random.choice(["true", "false", None])
            discount_pct     = round(random.uniform(0, 50), 2) if random.random() > 0.3 else None
            shipping_cost    = round(random.uniform(0, 200), 2)

            writer.writerow([
                order_id, customer_id, product_id, product_name,
                category, quantity, unit_price, total_amount,
                order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                delivery_dt.strftime("%Y-%m-%d %H:%M:%S"),
                payment_method, order_status, country,
                is_premium, discount_pct, shipping_cost
            ])

            if (i + 1) % 100_000 == 0:
                print(f"  {i+1:,} records written...")

    print(f"\nDone! File saved to: {output_path}")
    return "dbfs:/FileStore/ecommerce_raw/orders_raw.csv"

# Generate the data
RAW_FILE_PATH = generate_ecommerce_data(num_records=500_000)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Define Explicit Schema using StructType
# MAGIC
# MAGIC **Why explicit schema instead of inferSchema=True?**
# MAGIC - `inferSchema` reads the file TWICE — wastes time on large files
# MAGIC - Type inference can get it wrong (e.g., reads "123" as int but should be string)
# MAGIC - Explicit schema = you control exactly what types each column is
# MAGIC - Production pipelines ALWAYS use explicit schemas
# MAGIC
# MAGIC **What is StructType?**
# MAGIC It's like defining a table blueprint — column name + data type + nullable or not

# COMMAND ----------

raw_schema = StructType([
    StructField("order_id",           StringType(),    nullable=True),
    StructField("customer_id",        StringType(),    nullable=True),
    StructField("product_id",         StringType(),    nullable=True),
    StructField("product_name",       StringType(),    nullable=True),
    StructField("category",           StringType(),    nullable=True),
    StructField("quantity",           IntegerType(),   nullable=True),
    StructField("unit_price",         DoubleType(),    nullable=True),
    StructField("total_amount",       DoubleType(),    nullable=True),
    StructField("order_date",         StringType(),    nullable=True),   # Read as string first, cast later
    StructField("delivery_date",      StringType(),    nullable=True),
    StructField("payment_method",     StringType(),    nullable=True),
    StructField("order_status",       StringType(),    nullable=True),
    StructField("customer_country",   StringType(),    nullable=True),
    StructField("is_premium_customer",StringType(),    nullable=True),   # String "true"/"false"
    StructField("discount_percent",   DoubleType(),    nullable=True),
    StructField("shipping_cost",      DoubleType(),    nullable=True),
])

print("Schema defined successfully:")
print(f"  Total columns: {len(raw_schema.fields)}")
for field in raw_schema.fields:
    print(f"  {field.name:<25} {str(field.dataType):<20} nullable={field.nullable}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Load Raw Data with FAILFAST Mode
# MAGIC
# MAGIC **What is FAILFAST mode?**
# MAGIC - When Spark reads CSV, it might encounter rows that don't match the schema
# MAGIC - FAILFAST = "If ANY row is corrupt, STOP and throw an error immediately"
# MAGIC - Alternative modes: PERMISSIVE (keep bad rows, put them in _corrupt_record), DROPMALFORMED
# MAGIC
# MAGIC **Why FAILFAST for raw ingestion?**
# MAGIC - You want to know IMMEDIATELY if the source data is corrupted
# MAGIC - Fail early = fix early = don't propagate bad data downstream
# MAGIC
# MAGIC **Note:** Since we intentionally put some bad values (nulls, negatives),
# MAGIC we handle those in transformations — they're valid CSV rows, just bad business values.

# COMMAND ----------

print("Loading raw data with FAILFAST mode...")

df_raw = spark.read \
    .option("header", "true") \
    .option("mode", "FAILFAST") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("nullValue", "") \
    .option("emptyValue", "") \
    .schema(raw_schema) \
    .csv(RAW_FILE_PATH)

# Force evaluation to check FAILFAST actually works
# .count() triggers a full read of the file
raw_count = df_raw.count()

print(f"\nRaw data loaded successfully!")
print(f"  Total rows: {raw_count:,}")
print(f"  Total columns: {len(df_raw.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Quick Preview of Raw Data

# COMMAND ----------

print("Sample rows from raw data:")
df_raw.show(5, truncate=False)

print("\nData types confirmed:")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Register Raw DataFrame as Temp View
# MAGIC
# MAGIC This lets us query it with SQL in later notebooks.
# MAGIC Think of it like creating a virtual table.

# COMMAND ----------

df_raw.createOrReplaceTempView("orders_raw")

print("Temp view 'orders_raw' registered.")
print("You can now query: SELECT * FROM orders_raw LIMIT 10")

# Quick SQL verification
spark.sql("SELECT order_status, COUNT(*) as cnt FROM orders_raw GROUP BY order_status ORDER BY cnt DESC").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint Summary
# MAGIC
# MAGIC | Step | Status |
# MAGIC |------|--------|
# MAGIC | Dataset generated (500K rows) | ✅ Done |
# MAGIC | Explicit schema defined (16 columns) | ✅ Done |
# MAGIC | Data loaded with FAILFAST mode | ✅ Done |
# MAGIC | Temp view registered | ✅ Done |
# MAGIC
# MAGIC **Next:** Notebook 02 — Transformations & Data Cleaning

# COMMAND ----------

# Save raw count for downstream use
dbutils.notebook.exit(str(raw_count))
