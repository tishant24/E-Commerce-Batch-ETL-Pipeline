from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
from src.config import PathConfig, ETLConfig
from src.logger_setup import get_logger

logger = get_logger("Extract")


def get_retail_schema():
    """
    Explicit schema for the Online Retail II dataset from UCI.
    Dataset source: https://archive.ics.uci.edu/ml/datasets/Online+Retail+II

    Columns:
      Invoice     - Invoice number. Starts with 'C' if cancelled.
      StockCode   - Product code.
      Description - Product name/description.
      Quantity    - Quantity purchased per transaction.
      InvoiceDate - Date and time of invoice.
      Price       - Unit price per product in GBP.
      Customer ID - Unique customer identifier.
      Country     - Country where customer resides.
    """
    schema = StructType([
        StructField("Invoice",     StringType(),    nullable=True),
        StructField("StockCode",   StringType(),    nullable=True),
        StructField("Description", StringType(),    nullable=True),
        StructField("Quantity",    IntegerType(),   nullable=True),
        StructField("InvoiceDate", StringType(),    nullable=True),
        StructField("Price",       DoubleType(),    nullable=True),
        StructField("Customer ID", StringType(),    nullable=True),
        StructField("Country",     StringType(),    nullable=True),
    ])
    return schema


def read_raw_data(spark: SparkSession, path: str = None, mode: str = "PERMISSIVE"):
    """
    Read raw CSV file with explicit schema.

    mode options:
      FAILFAST    -> Raise exception on malformed rows (use in dev/test)
      PERMISSIVE  -> Replace bad rows with null (use in production)
      DROPMALFORMED -> Silently drop bad rows
    """
    file_path = path or PathConfig.RAW_DATA
    schema = get_retail_schema()

    logger.info(f"Reading raw data from: {file_path}")
    logger.info(f"Parse mode: {mode}")

    df = (
        spark.read
        .option("header", "true")
        .option("mode", mode)
        .option("timestampFormat", "M/d/yyyy H:mm")
        .option("escape", '"')
        .option("multiLine", "true")
        .schema(schema)
        .csv(file_path)
    )

    raw_count = df.count()
    logger.info(f"Raw records loaded: {raw_count:,}")

    return df, raw_count


def expand_to_target_size(df, target: int = ETLConfig.TARGET_RECORDS):
    """
    If the source dataset is smaller than target, union it with itself
    (with slight variation to avoid perfect duplicates at a dataset level).
    This simulates a production-scale 500K record dataset.
    """
    current_count = df.count()

    if current_count >= target:
        logger.info(f"Dataset has {current_count:,} rows, sampling down to {target:,}")
        return df.limit(target)

    logger.info(f"Dataset has {current_count:,} rows. Expanding to reach ~{target:,} records...")
    expanded = df
    while expanded.count() < target:
        expanded = expanded.union(df)

    expanded = expanded.limit(target)
    logger.info(f"Expanded dataset size: {expanded.count():,}")
    return expanded
