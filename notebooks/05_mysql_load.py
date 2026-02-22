# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Batch ETL Pipeline
# MAGIC ### Notebook 05: Load to MySQL via JDBC
# MAGIC
# MAGIC **What is JDBC?**
# MAGIC Java Database Connectivity — a standard API for connecting Java (and Spark) to relational databases.
# MAGIC Spark uses JDBC to write DataFrames directly into MySQL, PostgreSQL, Oracle, etc.
# MAGIC
# MAGIC **Setup needed:**
# MAGIC - MySQL server running (free: use PlanetScale, Railway, or local MySQL)
# MAGIC - MySQL JDBC driver (already available in Databricks clusters)
# MAGIC - Database credentials stored safely (NOT hardcoded — use Databricks Secrets)
# MAGIC
# MAGIC **On Community Edition:**
# MAGIC Databricks CE doesn't support Secrets. We use widgets (like environment variables for notebooks).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Configure Database Connection
# MAGIC
# MAGIC **IMPORTANT SECURITY NOTE:**
# MAGIC Never hardcode passwords in notebooks. On production Databricks:
# MAGIC `dbutils.secrets.get(scope="prod-secrets", key="mysql-password")`
# MAGIC
# MAGIC For Community Edition demo, we use widgets (user inputs at the top of the notebook).

# COMMAND ----------

# Create widgets for credentials (they appear as text boxes at the top)
dbutils.widgets.text("mysql_host",     "localhost",       "MySQL Host")
dbutils.widgets.text("mysql_port",     "3306",            "MySQL Port")
dbutils.widgets.text("mysql_database", "ecommerce_dw",    "Database Name")
dbutils.widgets.text("mysql_user",     "etl_user",        "MySQL Username")
dbutils.widgets.text("mysql_password", "",                "MySQL Password")
dbutils.widgets.text("mysql_table",    "orders_curated",  "Target Table")

# Read widget values
MYSQL_HOST     = dbutils.widgets.get("mysql_host")
MYSQL_PORT     = dbutils.widgets.get("mysql_port")
MYSQL_DATABASE = dbutils.widgets.get("mysql_database")
MYSQL_USER     = dbutils.widgets.get("mysql_user")
MYSQL_PASSWORD = dbutils.widgets.get("mysql_password")
MYSQL_TABLE    = dbutils.widgets.get("mysql_table")

# Build JDBC URL
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true"

print("JDBC Connection Configuration:")
print(f"  Host:     {MYSQL_HOST}:{MYSQL_PORT}")
print(f"  Database: {MYSQL_DATABASE}")
print(f"  Table:    {MYSQL_TABLE}")
print(f"  User:     {MYSQL_USER}")
print(f"  URL:      {JDBC_URL}")
print(f"  Password: {'*' * len(MYSQL_PASSWORD) if MYSQL_PASSWORD else '(not set — set widget above)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Prepare the Curated DataFrame
# MAGIC
# MAGIC Before writing to MySQL, select only the columns needed for the data warehouse.
# MAGIC MySQL doesn't handle all Spark types — we cast complex types to simple ones.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Load from the processed parquet view
df_processed = spark.table("orders_processed")

# Select and prepare columns for MySQL
# - Keep only business-relevant columns (not intermediate derived ones)
# - Cast Boolean to Integer (MySQL TINYINT) → 0/1
# - Ensure timestamps are in the right format

df_curated = df_processed.select(
    F.col("order_id"),
    F.col("customer_id"),
    F.col("product_id"),
    F.col("product_name"),
    F.col("category"),
    F.col("quantity").cast("int"),
    F.col("unit_price").cast("double"),
    F.col("total_amount").cast("double"),
    F.col("discount_pct").cast("double"),
    F.col("shipping_cost").cast("double"),
    F.col("effective_price").cast("double"),
    F.col("order_date").cast("timestamp"),
    F.col("delivery_date").cast("timestamp"),
    F.col("delivery_days").cast("int"),
    F.col("payment_method"),
    F.col("status").alias("order_status"),
    F.col("country").alias("customer_country"),
    F.col("is_premium").cast("int").alias("is_premium_customer"),  # Boolean→INT
    F.col("order_year").cast("int"),
    F.col("order_month").cast("int"),
    F.col("order_quarter").cast("int"),
    F.col("revenue_band"),
)

curated_count = df_curated.count()
print(f"Curated DataFrame ready:")
print(f"  Rows: {curated_count:,}")
print(f"  Columns: {len(df_curated.columns)}")
df_curated.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: MySQL Table DDL
# MAGIC
# MAGIC Run this SQL in MySQL BEFORE running the pipeline.
# MAGIC This creates the target table with:
# MAGIC - Proper primary key on `order_id`
# MAGIC - Indexes on frequently-queried columns
# MAGIC - VARCHAR lengths tuned for the data
# MAGIC
# MAGIC **Why indexes matter:**
# MAGIC - `PRIMARY KEY (order_id)` → B-tree index → O(log n) lookups
# MAGIC - `INDEX idx_customer` → fast WHERE customer_id = '...' queries
# MAGIC - `INDEX idx_date` → fast date range queries (most analytics!)
# MAGIC - Without indexes → MySQL scans every row = extremely slow at 500K+ rows

# COMMAND ----------

# Display the MySQL DDL (reference — run this in MySQL Workbench)
mysql_ddl = """
-- ============================================================
-- MySQL DDL for orders_curated table
-- Run this in MySQL before executing the Spark pipeline
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce_dw
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ecommerce_dw;

CREATE TABLE IF NOT EXISTS orders_curated (
    order_id              VARCHAR(36)     NOT NULL,
    customer_id           VARCHAR(20)     NOT NULL,
    product_id            VARCHAR(20)     NOT NULL,
    product_name          VARCHAR(200),
    category              VARCHAR(100),
    quantity              INT             NOT NULL,
    unit_price            DECIMAL(10, 2)  NOT NULL,
    total_amount          DECIMAL(12, 2),
    discount_pct          DECIMAL(5, 2)   DEFAULT 0.00,
    shipping_cost         DECIMAL(8, 2)   DEFAULT 0.00,
    effective_price       DECIMAL(10, 2),
    order_date            DATETIME        NOT NULL,
    delivery_date         DATETIME,
    delivery_days         INT,
    payment_method        VARCHAR(50),
    order_status          VARCHAR(50)     NOT NULL,
    customer_country      VARCHAR(100),
    is_premium_customer   TINYINT(1)      DEFAULT 0,
    order_year            SMALLINT        NOT NULL,
    order_month           TINYINT         NOT NULL,
    order_quarter         TINYINT,
    revenue_band          VARCHAR(20),
    created_at            TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    -- Primary key: ensures no duplicate orders
    PRIMARY KEY (order_id),

    -- Index on customer: for customer-specific queries
    -- e.g., SELECT * FROM orders_curated WHERE customer_id = 'CUST_123'
    INDEX idx_customer_id (customer_id),

    -- Index on order date: for time-series queries
    -- e.g., WHERE order_date BETWEEN '2022-01-01' AND '2022-12-31'
    INDEX idx_order_date (order_date),

    -- Composite index: year + month for monthly reports
    -- e.g., WHERE order_year = 2022 AND order_month = 6
    INDEX idx_year_month (order_year, order_month),

    -- Index on status: for operational dashboards
    -- e.g., WHERE order_status = 'pending'
    INDEX idx_status (order_status),

    -- Index on category: for product analytics
    INDEX idx_category (category)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Curated e-commerce orders — loaded by PySpark ETL pipeline';
"""

print(mysql_ddl)

# Also save it as a SQL file
ddl_path = "/dbfs/FileStore/ecommerce_pipeline/sql/create_orders_table.sql"
import os
os.makedirs("/dbfs/FileStore/ecommerce_pipeline/sql", exist_ok=True)
with open(ddl_path, "w") as f:
    f.write(mysql_ddl)
print(f"\nDDL saved to: {ddl_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Write to MySQL via JDBC
# MAGIC
# MAGIC **Key JDBC write options explained:**
# MAGIC
# MAGIC | Option | Value | Why |
# MAGIC |--------|-------|-----|
# MAGIC | `mode` | append | Add rows without deleting existing data |
# MAGIC | `batchsize` | 10000 | Insert 10K rows per SQL batch → reduces round trips |
# MAGIC | `numPartitions` | 4 | 4 parallel JDBC connections → 4x faster write |
# MAGIC | `truncate` | false | Don't truncate table before append |
# MAGIC | `driver` | com.mysql.cj.jdbc.Driver | MySQL connector class |
# MAGIC
# MAGIC **Why batchsize=10000?**
# MAGIC Default is 1000. With 450K rows:
# MAGIC - batchsize=1000 → 450 round trips to MySQL
# MAGIC - batchsize=10000 → 45 round trips → ~10x fewer network calls
# MAGIC
# MAGIC **Caution:** Don't set batchsize too high (>50000).
# MAGIC MySQL has a `max_allowed_packet` limit. Huge batches can fail.

# COMMAND ----------

# Repartition before JDBC write
# This controls how many parallel connections Spark opens to MySQL
# More partitions = more parallel writes = faster (but MySQL needs to handle concurrency)
JDBC_PARTITIONS = 4

df_to_write = df_curated.repartition(JDBC_PARTITIONS)

print(f"Writing {curated_count:,} rows to MySQL...")
print(f"  JDBC partitions (parallel connections): {JDBC_PARTITIONS}")
print(f"  Batch size: 10,000 rows per INSERT batch")
print()

# Only execute if password is provided (prevents errors in demo mode)
if MYSQL_PASSWORD:
    jdbc_properties = {
        "user":     MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "driver":   "com.mysql.cj.jdbc.Driver",
        "batchsize": "10000",
        "numPartitions": str(JDBC_PARTITIONS),
    }

    df_to_write.write \
        .mode("append") \
        .option("truncate", "false") \
        .jdbc(
            url=JDBC_URL,
            table=MYSQL_TABLE,
            properties=jdbc_properties
        )

    print(f"✅ Successfully wrote {curated_count:,} rows to {MYSQL_DATABASE}.{MYSQL_TABLE}")

else:
    print("⚠️  MySQL password not set — skipping actual write.")
    print("   Set the 'mysql_password' widget above with your MySQL password.")
    print()
    print("   To use a free MySQL host, try:")
    print("   - PlanetScale (planetscale.com) — free tier MySQL")
    print("   - Railway (railway.app) — free tier MySQL")
    print("   - Local MySQL + ngrok for tunneling")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Verify the Load (Read Back from MySQL)

# COMMAND ----------

if MYSQL_PASSWORD:
    print("Verifying data in MySQL...")

    jdbc_props_read = {
        "user":   MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    df_mysql = spark.read.jdbc(
        url=JDBC_URL,
        table=f"(SELECT COUNT(*) AS cnt, MAX(order_date) AS latest_order FROM {MYSQL_TABLE}) AS stats",
        properties=jdbc_props_read
    )

    df_mysql.show()

    # Spot-check: read a sample
    df_sample = spark.read.jdbc(
        url=JDBC_URL,
        table=f"(SELECT * FROM {MYSQL_TABLE} LIMIT 5) AS sample",
        properties=jdbc_props_read
    )
    print("Sample rows from MySQL:")
    df_sample.show(truncate=True)
else:
    print("Skipping MySQL read-back verification (no password set).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### JDBC Load Summary
# MAGIC
# MAGIC | Setting | Value | Impact |
# MAGIC |---------|-------|--------|
# MAGIC | Write mode | append | Non-destructive load |
# MAGIC | Batch size | 10,000 | ~10x fewer network calls vs default |
# MAGIC | Parallel connections | 4 | 4x faster writes |
# MAGIC | Primary key | order_id | O(log n) lookups, duplicate prevention |
# MAGIC | Indexes | 5 indexes | Fast filter/join on common columns |
# MAGIC
# MAGIC **Next:** Notebook 06 — Spark Optimizations & Advanced Features

# COMMAND ----------

dbutils.notebook.exit("mysql_load_complete")
