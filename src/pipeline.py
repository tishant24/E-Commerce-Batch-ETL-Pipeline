"""
E-Commerce Batch ETL Pipeline
Dataset : Online Retail II (UCI ML Repository)
Author  : Your Name
Version : 1.0.0

Pipeline Flow:
  Raw CSV -> Extract -> Transform -> Quality Checks -> Parquet + MySQL
"""

import sys
import os

# Fix for Windows: set HADOOP_HOME before PySpark starts
# winutils.exe must exist at C:\hadoop\bin\winutils.exe
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

# Fix for Windows: point Spark to the venv Python directly
# Without this, Windows redirects 'python' to Microsoft Store
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import SparkConfig, MySQLConfig, ETLConfig
from src.logger_setup import get_logger, PipelineTracker
from src.extract import read_raw_data, expand_to_target_size
from src.transform import run_all_transforms
from src.quality_checks import run_all_checks
from src.load import write_parquet, write_mysql, write_quality_report

logger = get_logger("Pipeline")


def build_spark_session() -> SparkSession:
    """
    Build and return a configured SparkSession.
    We set specific configs to avoid memory issues on local mode.
    """
    spark = (
        SparkSession.builder
        .appName(SparkConfig.APP_NAME)
        .master(SparkConfig.MASTER)
        .config("spark.executor.memory",            SparkConfig.EXECUTOR_MEMORY)
        .config("spark.driver.memory",              SparkConfig.DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions",     SparkConfig.SHUFFLE_PARTITIONS)
        .config("spark.serializer",                 SparkConfig.SERIALIZER)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.jars",                       MySQLConfig.JAR_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SparkConfig.LOG_LEVEL)
    return spark


def demo_broadcast_join(spark: SparkSession, df):
    """
    Broadcast Join Example:
    We have a small country-to-region lookup table (~10 rows).
    Instead of shuffling the large retail DataFrame, Spark broadcasts
    the small table to every executor node. This avoids the shuffle
    and is significantly faster for large-table + small-table joins.
    """
    logger.info("Running broadcast join demo (country -> region mapping)...")

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

    # F.broadcast() tells Spark: "this table is small, send a full copy to each node"
    joined = df.join(
        F.broadcast(region_df),
        on="country",
        how="left"
    ).fillna({"region": "Other"})

    logger.info("Broadcast join complete. 'region' column added.")
    return joined


def show_explain_plan(df):
    """
    Show the physical execution plan that Spark generated.
    This helps you understand how Spark will actually run the query.
    Look for: BroadcastHashJoin, HashAggregate, Exchange (shuffle).
    """
    logger.info("Showing Spark physical execution plan...")
    print("\n" + "=" * 60)
    print("SPARK PHYSICAL EXECUTION PLAN (explain())")
    print("=" * 60)
    df.explain(mode="simple")
    print("=" * 60 + "\n")


def cache_for_quality_check(df):
    """
    We cache the DataFrame here because we're about to run multiple
    count() and filter() operations on it for quality checks.
    Without cache(), each operation would re-read and re-compute the
    entire DataFrame from scratch.

    Rule of thumb: cache only if you will reuse the DataFrame 2+ times.
    Call unpersist() when done to free memory.
    """
    logger.info("Caching DataFrame for quality checks...")
    df.cache()
    df.count()  # trigger the cache to materialize
    return df


def run():
    tracker = PipelineTracker(logger)
    tracker.start()

    spark = build_spark_session()
    validation_status = "PASSED"

    try:
        # STAGE 1: EXTRACT
        logger.info("--- STAGE 1: EXTRACT ---")
        raw_df, raw_count = read_raw_data(spark, mode="PERMISSIVE")
        raw_df = expand_to_target_size(raw_df, target=ETLConfig.TARGET_RECORDS)
        actual_raw_count = raw_df.count()
        tracker.log_stage("Extract", record_count=actual_raw_count)
        tracker.log_metric("raw_record_count", f"{actual_raw_count:,}")

        # STAGE 2: TRANSFORM
        logger.info("--- STAGE 2: TRANSFORM ---")
        transformed_df = run_all_transforms(raw_df)
        processed_count = transformed_df.count()
        tracker.log_stage("Transform", record_count=processed_count)
        tracker.log_metric("processed_record_count", f"{processed_count:,}")

        # STAGE 3: BROADCAST JOIN (Optimization Demo)
        logger.info("--- STAGE 3: BROADCAST JOIN ---")
        enriched_df = demo_broadcast_join(spark, transformed_df)

        # STAGE 4: SHOW EXECUTION PLAN
        show_explain_plan(enriched_df)

        # STAGE 5: CACHE + QUALITY CHECKS
        logger.info("--- STAGE 4: DATA QUALITY ---")
        cached_df = cache_for_quality_check(enriched_df)
        quality_report_df, validation_status = run_all_checks(spark, cached_df, actual_raw_count)
        quality_report_df.show(truncate=False)
        cached_df.unpersist()
        tracker.log_metric("quality_check_status", validation_status)

        # STAGE 6: WRITE PARQUET
        logger.info("--- STAGE 5: WRITE PARQUET ---")
        write_parquet(enriched_df)
        tracker.log_stage("Parquet Write", record_count=processed_count)

        # STAGE 7: WRITE MYSQL
        logger.info("--- STAGE 6: WRITE MYSQL ---")
        write_mysql(enriched_df)
        tracker.log_stage("MySQL Write", record_count=processed_count)

        # STAGE 8: SAVE QUALITY REPORT
        write_quality_report(quality_report_df)

        tracker.log_metric("total_records_loaded_to_mysql", f"{processed_count:,}")

    except Exception as e:
        validation_status = "FAILED"
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

    finally:
        tracker.finish(validation_status=validation_status)
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    run()
