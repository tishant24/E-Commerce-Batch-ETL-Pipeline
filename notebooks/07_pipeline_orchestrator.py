# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Batch ETL Pipeline
# MAGIC ### Notebook 07: Pipeline Orchestrator + Logging
# MAGIC
# MAGIC **This is the MASTER notebook.**
# MAGIC It runs all other notebooks in sequence and tracks:
# MAGIC - Job start/end time
# MAGIC - Records processed at each stage
# MAGIC - DQ validation status
# MAGIC - Any failures (with error details)
# MAGIC
# MAGIC **In production:** This would be triggered by:
# MAGIC - Databricks Jobs (scheduled runs)
# MAGIC - Apache Airflow DAG
# MAGIC - Azure Data Factory pipeline
# MAGIC - AWS Step Functions
# MAGIC
# MAGIC On Community Edition: Run this notebook manually to execute the full pipeline.

# COMMAND ----------

import json
import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, IntegerType, LongType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Logger
# MAGIC
# MAGIC A simple structured logger that tracks each stage of the pipeline.
# MAGIC We write logs to a DataFrame so they're queryable — much better than plain text logs.

# COMMAND ----------

class PipelineLogger:
    """
    Tracks pipeline execution: stage-by-stage timing, record counts, status.
    Writes a structured log DataFrame at the end — useful for SLA monitoring.
    """

    def __init__(self, pipeline_name: str, run_id: str = None):
        self.pipeline_name  = pipeline_name
        self.run_id         = run_id or datetime.now().strftime("RUN_%Y%m%d_%H%M%S")
        self.pipeline_start = datetime.now()
        self.stages         = []
        self.current_stage  = None
        self.stage_start    = None

        print("=" * 70)
        print(f"PIPELINE STARTED: {self.pipeline_name}")
        print(f"Run ID:           {self.run_id}")
        print(f"Start time:       {self.pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

    def start_stage(self, stage_name: str, description: str = ""):
        """Mark a pipeline stage as started."""
        self.current_stage = stage_name
        self.stage_start   = time.time()
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{ts}] ▶  STARTED: {stage_name}")
        if description:
            print(f"          {description}")

    def end_stage(self, stage_name: str, records_in: int = 0,
                  records_out: int = 0, status: str = "SUCCESS",
                  notes: str = ""):
        """Mark a pipeline stage as completed."""
        elapsed  = time.time() - (self.stage_start or time.time())
        end_time = datetime.now()
        ts       = end_time.strftime("%H:%M:%S")

        icon = "✅" if status == "SUCCESS" else "❌" if status == "FAILED" else "⚠️ "
        print(f"[{ts}] {icon} COMPLETED: {stage_name}")
        print(f"          Duration: {elapsed:.1f}s  |  In: {records_in:,}  |  Out: {records_out:,}  |  Status: {status}")
        if notes:
            print(f"          Notes: {notes}")

        self.stages.append({
            "run_id":        self.run_id,
            "pipeline_name": self.pipeline_name,
            "stage_name":    stage_name,
            "start_time":    datetime.fromtimestamp(self.stage_start).strftime("%Y-%m-%d %H:%M:%S"),
            "end_time":      end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_secs": round(elapsed, 2),
            "records_in":    records_in,
            "records_out":   records_out,
            "status":        status,
            "notes":         notes or "",
        })

    def log_failure(self, stage_name: str, error_msg: str):
        """Log a stage failure."""
        self.end_stage(stage_name, status="FAILED", notes=error_msg[:500])
        print(f"\n🚨 PIPELINE FAILURE at stage: {stage_name}")
        print(f"   Error: {error_msg}")

    def finalize(self) -> dict:
        """Calculate final pipeline summary."""
        end_time       = datetime.now()
        total_duration = (end_time - self.pipeline_start).total_seconds()

        statuses       = [s["status"] for s in self.stages]
        overall_status = "FAILED" if "FAILED" in statuses else \
                         "PARTIAL" if "WARNING" in statuses else "SUCCESS"

        summary = {
            "run_id":            self.run_id,
            "pipeline_name":     self.pipeline_name,
            "overall_status":    overall_status,
            "start_time":        self.pipeline_start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time":          end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_duration_s":  round(total_duration, 2),
            "total_stages":      len(self.stages),
            "successful_stages": statuses.count("SUCCESS"),
            "failed_stages":     statuses.count("FAILED"),
        }

        print("\n" + "=" * 70)
        print(f"PIPELINE COMPLETED: {self.pipeline_name}")
        print(f"Overall Status: {overall_status}")
        print(f"Duration: {total_duration:.1f}s ({total_duration/60:.1f} mins)")
        print(f"Stages: {len(self.stages)} total, {statuses.count('SUCCESS')} succeeded, {statuses.count('FAILED')} failed")
        print("=" * 70)

        return summary

    def to_dataframe(self):
        """Convert log entries to a Spark DataFrame for storage."""
        if not self.stages:
            return None

        schema = StructType([
            StructField("run_id",        StringType(),  False),
            StructField("pipeline_name", StringType(),  False),
            StructField("stage_name",    StringType(),  False),
            StructField("start_time",    StringType(),  True),
            StructField("end_time",      StringType(),  True),
            StructField("duration_secs", DoubleType(),  True),
            StructField("records_in",    LongType(),    True),
            StructField("records_out",   LongType(),    True),
            StructField("status",        StringType(),  False),
            StructField("notes",         StringType(),  True),
        ])

        from pyspark.sql.types import DoubleType

        rows = [
            (s["run_id"], s["pipeline_name"], s["stage_name"],
             s["start_time"], s["end_time"], float(s["duration_secs"]),
             int(s["records_in"]), int(s["records_out"]),
             s["status"], s["notes"])
            for s in self.stages
        ]

        return spark.createDataFrame(rows, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Full Pipeline

# COMMAND ----------

# Initialize logger
logger = PipelineLogger(
    pipeline_name="ECommerce_Batch_ETL",
    run_id=f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

pipeline_summary = {}
raw_count        = 0
clean_count      = 0
dq_summary       = {}

# COMMAND ----------

# STAGE 1: Data Ingestion
logger.start_stage(
    "01_data_ingestion",
    "Generate synthetic dataset + load with explicit schema + FAILFAST mode"
)
try:
    result = dbutils.notebook.run("./01_data_ingestion", timeout_seconds=600, arguments={})
    raw_count = int(result)
    logger.end_stage(
        "01_data_ingestion",
        records_in=0,
        records_out=raw_count,
        status="SUCCESS",
        notes=f"Loaded {raw_count:,} raw records from CSV"
    )
except Exception as e:
    logger.log_failure("01_data_ingestion", str(e))
    raise

# COMMAND ----------

# STAGE 2: Transformations
logger.start_stage(
    "02_transformations",
    "Null handling, dedup, casting, derived columns, filtering"
)
try:
    result = dbutils.notebook.run("./02_transformations", timeout_seconds=600, arguments={})
    clean_count = spark.table("orders_clean").count()
    dropped     = raw_count - clean_count
    logger.end_stage(
        "02_transformations",
        records_in=raw_count,
        records_out=clean_count,
        status="SUCCESS",
        notes=f"Dropped {dropped:,} invalid records ({dropped/raw_count*100:.1f}%)"
    )
except Exception as e:
    logger.log_failure("02_transformations", str(e))
    raise

# COMMAND ----------

# STAGE 3: Data Quality
logger.start_stage(
    "03_data_quality",
    "Row count, null %, duplicate, schema, business rule checks"
)
try:
    result_json = dbutils.notebook.run("./03_data_quality", timeout_seconds=600, arguments={})
    dq_summary  = json.loads(result_json)

    dq_status = "SUCCESS" if dq_summary["overall"] in ["PASS", "WARN"] else "FAILED"
    if dq_summary["overall"] == "WARN":
        dq_status = "WARNING"

    logger.end_stage(
        "03_data_quality",
        records_in=clean_count,
        records_out=clean_count,
        status=dq_status,
        notes=(f"Checks: {dq_summary['total_checks']} total, "
               f"{dq_summary['passed']} passed, {dq_summary['warned']} warned, "
               f"{dq_summary['failed']} failed")
    )

    if dq_summary["overall"] == "FAIL":
        print("🚨 DQ FAILURES detected — pipeline will stop before write stage")
        raise Exception("Data Quality validation failed — stopping pipeline to prevent bad data in storage")

except json.JSONDecodeError:
    # DQ ran but returned non-JSON exit (shouldn't happen, handle gracefully)
    logger.end_stage("03_data_quality", status="WARNING", notes="Could not parse DQ summary")
except Exception as e:
    logger.log_failure("03_data_quality", str(e))
    raise

# COMMAND ----------

# STAGE 4: Write Parquet
logger.start_stage(
    "04_write_parquet",
    "Repartition + write to Parquet partitioned by year/month"
)
try:
    result = dbutils.notebook.run("./04_write_parquet", timeout_seconds=900, arguments={})
    logger.end_stage(
        "04_write_parquet",
        records_in=clean_count,
        records_out=clean_count,
        status="SUCCESS",
        notes="Written to dbfs:/FileStore/ecommerce_pipeline/processed/ partitioned by year/month"
    )
except Exception as e:
    logger.log_failure("04_write_parquet", str(e))
    raise

# COMMAND ----------

# STAGE 5: MySQL Load (optional — requires credentials)
logger.start_stage(
    "05_mysql_load",
    "JDBC write to MySQL with batchsize optimization"
)
try:
    result = dbutils.notebook.run(
        "./05_mysql_load",
        timeout_seconds=900,
        arguments={}  # Credentials come from widgets inside notebook 05
    )
    logger.end_stage(
        "05_mysql_load",
        records_in=clean_count,
        records_out=clean_count,
        status="SUCCESS",
        notes="Loaded to MySQL orders_curated table"
    )
except Exception as e:
    # MySQL is optional — treat as warning, not failure
    logger.end_stage(
        "05_mysql_load",
        status="WARNING",
        notes=f"MySQL load skipped or failed: {str(e)[:200]}"
    )
    print("⚠️  MySQL load failed/skipped — continuing pipeline (non-critical)")

# COMMAND ----------

# STAGE 6: Spark Optimizations Demo
logger.start_stage(
    "06_optimizations",
    "Broadcast join, caching, explain plans, AQE demo"
)
try:
    result = dbutils.notebook.run("./06_optimizations", timeout_seconds=600, arguments={})
    logger.end_stage(
        "06_optimizations",
        records_in=clean_count,
        records_out=clean_count,
        status="SUCCESS",
        notes="All optimization demos completed"
    )
except Exception as e:
    logger.log_failure("06_optimizations", str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Summary & Log Report

# COMMAND ----------

# Finalize and generate summary
pipeline_summary = logger.finalize()

# Create log DataFrame
df_pipeline_log = logger.to_dataframe()

print("\nPipeline Execution Log:")
df_pipeline_log.show(20, truncate=False)

# Register log as temp view
df_pipeline_log.createOrReplaceTempView("pipeline_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overall Pipeline Statistics

# COMMAND ----------

print("=" * 70)
print("FINAL PIPELINE STATISTICS")
print("=" * 70)
print(f"  Pipeline Name    : {pipeline_summary['pipeline_name']}")
print(f"  Run ID           : {pipeline_summary['run_id']}")
print(f"  Overall Status   : {pipeline_summary['overall_status']}")
print(f"  Start Time       : {pipeline_summary['start_time']}")
print(f"  End Time         : {pipeline_summary['end_time']}")
print(f"  Total Duration   : {pipeline_summary['total_duration_s']:.1f}s ({pipeline_summary['total_duration_s']/60:.1f} mins)")
print()
print(f"  Raw Records      : {raw_count:,}")
print(f"  Clean Records    : {clean_count:,}")
print(f"  Records Dropped  : {raw_count - clean_count:,} ({(raw_count-clean_count)/raw_count*100:.1f}%)")
print()
if dq_summary:
    print(f"  DQ Checks Run    : {dq_summary.get('total_checks', 0)}")
    print(f"  DQ Passed        : {dq_summary.get('passed', 0)}")
    print(f"  DQ Warned        : {dq_summary.get('warned', 0)}")
    print(f"  DQ Failed        : {dq_summary.get('failed', 0)}")
    print(f"  DQ Overall       : {dq_summary.get('overall', 'N/A')}")
print()
print(f"  Stages Completed : {pipeline_summary['successful_stages']}/{pipeline_summary['total_stages']}")

# COMMAND ----------

# Save pipeline log to DBFS for historical tracking
LOG_PATH = "dbfs:/FileStore/ecommerce_pipeline/logs/"
import os
os.makedirs("/dbfs/FileStore/ecommerce_pipeline/logs/", exist_ok=True)

df_pipeline_log.coalesce(1).write.mode("append").json(LOG_PATH)
print(f"\nPipeline log appended to: {LOG_PATH}")
print("Historical log is accumulative — each run adds a new record.")
print("\nPipeline completed successfully! ✅")
