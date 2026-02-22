"""
Pipeline Configuration
======================
Central config file for the E-Commerce ETL Pipeline.
Change values here — notebooks read from this file.

On Databricks: Values can be overridden by dbutils.widgets or Databricks Secrets.
"""

# ------------------------------------------------------------------
# Pipeline Metadata
# ------------------------------------------------------------------
PIPELINE_NAME    = "ECommerce_Batch_ETL"
PIPELINE_VERSION = "1.0.0"
ENVIRONMENT      = "dev"            # dev | staging | prod

# ------------------------------------------------------------------
# Data Paths (DBFS)
# ------------------------------------------------------------------
RAW_DATA_PATH       = "dbfs:/FileStore/ecommerce_raw/"
PROCESSED_DATA_PATH = "dbfs:/FileStore/ecommerce_pipeline/processed/"
CURATED_DATA_PATH   = "dbfs:/FileStore/ecommerce_pipeline/curated/"
LOGS_PATH           = "dbfs:/FileStore/ecommerce_pipeline/logs/"
DQ_REPORT_PATH      = "dbfs:/FileStore/ecommerce_pipeline/dq_report/"

# ------------------------------------------------------------------
# Dataset Configuration
# ------------------------------------------------------------------
NUM_RECORDS          = 500_000      # Number of synthetic records to generate
DUPLICATE_RATE       = 0.05         # 5% duplicate order_ids
NULL_INJECTION_RATE  = 0.10         # 10% null values in optional columns

# ------------------------------------------------------------------
# Spark Tuning
# ------------------------------------------------------------------
WRITE_PARTITIONS    = 4             # Number of output files
JDBC_PARTITIONS     = 4             # Parallel JDBC connections for MySQL write
JDBC_BATCH_SIZE     = 10_000        # Rows per INSERT batch
SHUFFLE_PARTITIONS  = 200           # spark.sql.shuffle.partitions (default 200)
AQE_ENABLED         = True          # Adaptive Query Execution

# ------------------------------------------------------------------
# Data Quality Thresholds
# ------------------------------------------------------------------
DQ_MIN_ROW_COUNT    = 400_000       # Pipeline fails if fewer rows survive cleaning
DQ_MAX_NULL_PCT     = {             # Max allowed null % per critical column
    "order_id":     0.0,
    "customer_id":  0.0,
    "order_date":   0.0,
    "unit_price":   0.0,
    "quantity":     0.0,
    "total_amount": 2.0,
    "category":     5.0,
    "country":      5.0,
}
DQ_MAX_DUPLICATE_PCT = 0.1          # Max allowed duplicate % after dedup step

# ------------------------------------------------------------------
# MySQL / JDBC Configuration
# ------------------------------------------------------------------
# NOTE: Never hardcode passwords here!
# Use Databricks Secrets in production:
#   dbutils.secrets.get(scope="prod-secrets", key="mysql-password")
MYSQL_HOST          = "localhost"
MYSQL_PORT          = 3306
MYSQL_DATABASE      = "ecommerce_dw"
MYSQL_TABLE         = "orders_curated"
MYSQL_USER          = "etl_user"
# MYSQL_PASSWORD: Set via Databricks widget or Secret

# ------------------------------------------------------------------
# Partition Strategy
# ------------------------------------------------------------------
PARTITION_COLUMNS   = ["order_year", "order_month"]   # Parquet partition keys
DEDUP_KEY_COLUMNS   = ["order_id"]                    # Deduplication key

# ------------------------------------------------------------------
# Business Rules for Filtering
# ------------------------------------------------------------------
MIN_QUANTITY        = 1             # Minimum valid quantity
MIN_UNIT_PRICE      = 0.01          # Minimum valid price (not 0 or negative)
MIN_ORDER_YEAR      = 2020          # Earliest valid order year
VALID_ORDER_STATUSES = [
    "completed", "pending", "cancelled", "refunded", "processing"
]

# ------------------------------------------------------------------
# Helper function
# ------------------------------------------------------------------
def get_jdbc_url(host: str = None, port: int = None, database: str = None) -> str:
    """Build JDBC URL from config values."""
    h = host     or MYSQL_HOST
    p = port     or MYSQL_PORT
    d = database or MYSQL_DATABASE
    return f"jdbc:mysql://{h}:{p}/{d}?useSSL=false&allowPublicKeyRetrieval=true"


if __name__ == "__main__":
    print("Pipeline Configuration:")
    print(f"  Name:         {PIPELINE_NAME}")
    print(f"  Version:      {PIPELINE_VERSION}")
    print(f"  Environment:  {ENVIRONMENT}")
    print(f"  Records:      {NUM_RECORDS:,}")
    print(f"  JDBC URL:     {get_jdbc_url()}")
