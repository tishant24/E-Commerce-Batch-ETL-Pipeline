import os
from dotenv import load_dotenv

load_dotenv()


class SparkConfig:
    APP_NAME = "ecommerce_etl_pipeline"
    MASTER = "local[*]"
    EXECUTOR_MEMORY = "2g"
    DRIVER_MEMORY = "2g"
    SHUFFLE_PARTITIONS = "8"
    LOG_LEVEL = "WARN"
    SERIALIZER = "org.apache.spark.serializer.KryoSerializer"


class PathConfig:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DATA = os.path.join(BASE_DIR, "data", "raw", "online_retail_II.csv")
    PARQUET_OUTPUT = os.path.join(BASE_DIR, "data", "processed", "parquet", "retail_data")
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")
    SAMPLE_DATA = os.path.join(BASE_DIR, "data", "sample", "sample_retail_data.csv")


class MySQLConfig:
    HOST = os.getenv("MYSQL_HOST", "localhost")
    PORT = os.getenv("MYSQL_PORT", "3306")
    DATABASE = os.getenv("MYSQL_DATABASE", "ecommerce_db")
    TABLE = os.getenv("MYSQL_TABLE", "retail_transactions")
    USER = os.getenv("MYSQL_USER", "root")
    PASSWORD = os.getenv("MYSQL_PASSWORD", "root123")
    JDBC_URL = (
        f"jdbc:mysql://{HOST}:{PORT}/{DATABASE}"
        f"?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
    )
    DRIVER = "com.mysql.cj.jdbc.Driver"
    BATCH_SIZE = 5000
    JAR_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "jars",
        "mysql-connector-j-9.6.0.jar"
    )


class ETLConfig:
    TARGET_RECORDS = 500000
    REPARTITION_COUNT = 8
    PARTITION_COLUMN = "invoice_year"
    MAX_NULL_THRESHOLD = 0.10
    MAX_DUPLICATE_THRESHOLD = 0.05
    WRITE_MODE = "overwrite"
    JDBC_WRITE_MODE = "overwrite"
    CRITICAL_COLUMNS = ["invoice_id", "stock_code", "quantity", "unit_price"]
    DEDUP_COLUMNS = ["invoice_id", "stock_code"]
