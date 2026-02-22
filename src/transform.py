from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType, IntegerType
from src.config import ETLConfig
from src.logger_setup import get_logger

logger = get_logger("Transform")


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename columns to snake_case. 'Customer ID' has a space so it needs
    backtick quoting in Spark SQL, so we rename it here early.
    """
    logger.info("Renaming columns to snake_case...")
    renamed = (
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
    return renamed


def cast_columns(df: DataFrame) -> DataFrame:
    """
    Cast invoice_date_raw from string to proper Timestamp.
    Quantity and unit_price already have types from schema,
    but we double-cast here to be safe.
    """
    logger.info("Casting column types...")
    casted = (
        df
        .withColumn(
            "invoice_date",
            F.to_timestamp(F.col("invoice_date_raw"), "M/d/yyyy H:mm")
        )
        .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .drop("invoice_date_raw")
    )
    return casted


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Drop rows where critical business columns are null.
    Critical columns: invoice_id, stock_code, quantity, unit_price.
    customer_id has ~25% nulls in this dataset (guest checkouts), which is
    normal. We keep those rows but flag them.
    """
    logger.info("Handling null values...")
    before_count = df.count()

    cleaned = df.dropna(subset=["invoice_id", "stock_code", "quantity", "unit_price"])

    cleaned = cleaned.withColumn(
        "customer_id",
        F.when(F.col("customer_id").isNull(), F.lit("GUEST")).otherwise(F.col("customer_id"))
    )

    after_count = cleaned.count()
    dropped = before_count - after_count
    logger.info(f"Null handling: dropped {dropped:,} rows with critical nulls")
    return cleaned


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate rows based on (invoice_id, stock_code).
    In retail data, the same item should appear only once per invoice.
    We keep the first occurrence ordered by invoice_date.
    """
    logger.info("Deduplicating records...")
    before_count = df.count()

    deduped = df.dropDuplicates(ETLConfig.DEDUP_COLUMNS)

    after_count = deduped.count()
    removed = before_count - after_count
    logger.info(f"Deduplication: removed {removed:,} duplicate rows")
    return deduped


def filter_invalid_records(df: DataFrame) -> DataFrame:
    """
    Business rules for valid transactions:
      - quantity must be > 0  (negative = return/cancellation, handled separately)
      - unit_price must be > 0
      - invoice_date must not be null after casting
      - country must not be empty or 'Unspecified'
    """
    logger.info("Filtering invalid records...")
    before_count = df.count()

    filtered = df.filter(
        (F.col("quantity") > 0) &
        (F.col("unit_price") > 0) &
        (F.col("invoice_date").isNotNull()) &
        (F.col("country").isNotNull()) &
        (F.col("country") != "Unspecified")
    )

    after_count = filtered.count()
    removed = before_count - after_count
    logger.info(f"Invalid record filter: removed {removed:,} rows")
    return filtered


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Derived columns added for analytical purposes:
      invoice_year    -> year extracted from invoice_date
      invoice_month   -> month extracted from invoice_date
      invoice_day     -> day of month
      total_amount    -> quantity * unit_price (revenue per line item)
      is_return       -> True if invoice starts with 'C' (cancellation)
      price_bucket    -> categorize unit_price into buckets
    """
    logger.info("Adding derived columns...")
    enriched = (
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
    return enriched


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Final column selection and ordering for the curated output table.
    This defines the exact schema of the processed dataset.
    """
    return df.select(
        "invoice_id",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "total_amount",
        "invoice_date",
        "invoice_year",
        "invoice_month",
        "invoice_day",
        "customer_id",
        "country",
        "is_return",
        "price_bucket"
    )


def run_all_transforms(df: DataFrame) -> DataFrame:
    """
    Master transformation function that chains all steps in order.
    Each step is logged so the pipeline is easy to debug.
    """
    logger.info("Starting transformation pipeline...")

    df = rename_columns(df)
    df = cast_columns(df)
    df = handle_nulls(df)
    df = deduplicate(df)
    df = filter_invalid_records(df)
    df = add_derived_columns(df)
    df = select_final_columns(df)

    logger.info("All transformations complete.")
    return df
