from pyspark.sql import DataFrame
from src.config import PathConfig, MySQLConfig, ETLConfig
from src.logger_setup import get_logger

logger = get_logger("Load")


def write_parquet(df: DataFrame, output_path: str = None, partition_col: str = None):
    """
    Write the processed DataFrame to Parquet format.

    Partition strategy:
      - We partition by 'invoice_year' so that when analysts query
        a specific year, Spark only scans the relevant partition folder
        instead of reading all files. This is called partition pruning.
      - repartition(8) before write creates 8 roughly equal sized files
        inside each partition. Without this, you could end up with
        thousands of tiny files (the small file problem) which kills
        performance on reads.

    Why Parquet?
      - Columnar storage: only reads columns you actually need
      - Built-in compression: ~3-5x smaller than CSV
      - Schema is stored inside the file (self-describing)
      - Native support in Spark, Hive, Presto, Athena, Databricks
    """
    path = output_path or PathConfig.PARQUET_OUTPUT
    col = partition_col or ETLConfig.PARTITION_COLUMN

    logger.info(f"Writing Parquet to: {path}")
    logger.info(f"Partition column  : {col}")
    logger.info(f"Repartition count : {ETLConfig.REPARTITION_COUNT}")

    df_repartitioned = df.repartition(ETLConfig.REPARTITION_COUNT, col)

    (
        df_repartitioned.write
        .mode(ETLConfig.WRITE_MODE)
        .partitionBy(col)
        .parquet(path)
    )

    logger.info("Parquet write complete.")


def write_mysql(df: DataFrame):
    """
    Load the curated data into MySQL using Spark's JDBC connector.

    Why these settings?
      - batchsize: Instead of inserting one row at a time, Spark sends
        batches of 5000 rows per JDBC call. Drastically reduces round-trips.
      - rewriteBatchedStatements=true: MySQL driver rewrites individual
        inserts into multi-row INSERT statements. Even faster.
      - truncate=true with append mode: Truncates the table on each run
        instead of dropping/re-creating it (preserves indexes).

    We select only columns that exist in the MySQL table to avoid schema mismatch.
    """
    mysql_cols = [
        "invoice_id", "stock_code", "description", "quantity",
        "unit_price", "total_amount", "invoice_date", "invoice_year",
        "invoice_month", "invoice_day", "customer_id", "country",
        "is_return", "price_bucket"
    ]

    df_mysql = df.select(mysql_cols)

    logger.info(f"Writing to MySQL table: {MySQLConfig.TABLE}")
    logger.info(f"JDBC URL : {MySQLConfig.JDBC_URL}")
    logger.info(f"Batch size: {MySQLConfig.BATCH_SIZE:,}")

    jdbc_props = {
        "user":                       MySQLConfig.USER,
        "password":                   MySQLConfig.PASSWORD,
        "driver":                     MySQLConfig.DRIVER,
        "batchsize":                  str(MySQLConfig.BATCH_SIZE),
        "rewriteBatchedStatements":   "true",
        "truncate":                   "true",
    }

    (
        df_mysql.write
        .mode(ETLConfig.JDBC_WRITE_MODE)
        .jdbc(
            url=MySQLConfig.JDBC_URL,
            table=MySQLConfig.TABLE,
            properties=jdbc_props
        )
    )

    logger.info("MySQL write complete.")


def write_quality_report(report_df: DataFrame):
    """
    Persist the quality report DataFrame as JSON files for record-keeping
    AND write it to the MySQL dq_report table via JDBC.
    """
    import os

    # Save to JSON (Parquet-style) in logs folder
    report_path = os.path.join(PathConfig.LOG_DIR, "quality_report")
    logger.info(f"Saving quality report to: {report_path}")
    report_df.coalesce(1).write.mode("overwrite").json(report_path)
    logger.info("Quality report saved.")

    # Write to MySQL dq_report table
    # MySQL auto-generates: id (PK) and run_timestamp (DEFAULT CURRENT_TIMESTAMP)
    # We only send the 5 data columns
    dq_cols = ["check_name", "metric_value", "threshold", "status", "detail"]
    df_dq = report_df.select(dq_cols)

    jdbc_props = {
        "user":     MySQLConfig.USER,
        "password": MySQLConfig.PASSWORD,
        "driver":   MySQLConfig.DRIVER,
    }

    logger.info("Writing quality report to MySQL table: dq_report")
    (
        df_dq.write
        .mode("append")          # append so each run adds new rows
        .jdbc(
            url=MySQLConfig.JDBC_URL,
            table="dq_report",
            properties=jdbc_props
        )
    )
    logger.info("Quality report written to MySQL.")
