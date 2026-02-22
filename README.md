# E-Commerce Batch ETL Pipeline

A production-style data engineering project built with PySpark that processes
500,000 records of real UK e-commerce transaction data through a complete
Extract, Transform, Load pipeline with data quality validation.

---

## Dataset

**Online Retail II** — UCI Machine Learning Repository

Real transactional data from a UK-based online retailer covering Dec 2009 to Dec 2011.
The company primarily sells unique all-occasion gift-ware to individual customers and wholesalers.

Free download (no account needed):

```
https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx
```

Convert to CSV before running the pipeline:

```bash
python -c "import pandas as pd; pd.read_excel('online_retail_II.xlsx').to_csv('data/raw/online_retail_II.csv', index=False)"
```

Or generate 500K synthetic rows locally:

```bash
python data/sample/generate_sample.py
```

---

## Project Structure

```
ecommerce-etl-pipeline/
    data/
        raw/                     <- put online_retail_II.csv here
        processed/parquet/       <- pipeline writes Parquet here
        sample/
            generate_sample.py   <- synthetic data generator
    src/
        config.py                <- all configuration (paths, MySQL, Spark)
        logger_setup.py          <- logging setup and pipeline tracker
        extract.py               <- CSV reader with explicit StructType schema
        transform.py             <- 8-step transformation chain
        quality_checks.py        <- 18-check data quality framework
        load.py                  <- Parquet writer and MySQL JDBC writer
        pipeline.py              <- main orchestrator (run this)
    notebooks/
        etl_pipeline.py          <- full step-by-step notebook (open in Jupyter)
    streamlit_app/
        app.py                   <- interactive demo app for recruiters
    sql/
        create_tables.sql        <- MySQL DDL with index explanations
    logs/                        <- pipeline.log and quality report written here
    jars/                        <- place mysql-connector-java-8.0.33.jar here
    requirements.txt
    README.md
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set up Java (required for PySpark)

```bash
# Windows: download JDK 11 from https://adoptium.net
# Then set JAVA_HOME:
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-11.x.x.x-hotspot"
```

### 3. Download the MySQL JDBC driver

Go to: https://dev.mysql.com/downloads/connector/j/

Download `mysql-connector-java-8.0.33.jar` and place it in the `jars/` folder.

### 4. Set up MySQL (free with Docker)

```bash
docker run -d \
    --name mysql-etl \
    -e MYSQL_ROOT_PASSWORD=root123 \
    -e MYSQL_DATABASE=ecommerce_db \
    -p 3306:3306 \
    mysql:8.0

mysql -u root -proot123 < sql/create_tables.sql
```

### 5. Configure environment variables

Create a `.env` file in the project root:

```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=ecommerce_db
MYSQL_TABLE=retail_transactions
MYSQL_USER=root
MYSQL_PASSWORD=root123
```

### 6. Run the pipeline

```bash
python src/pipeline.py
```

### 7. Run the Streamlit demo

```bash
streamlit run streamlit_app/app.py
```

---

## Pipeline Stages

| Stage | Description | Key Technique |
|-------|-------------|---------------|
| Extract | Read CSV with explicit schema | StructType, FAILFAST mode |
| Transform | Clean, cast, deduplicate, enrich | 8-step chain |
| Quality | Validate 18 data quality rules | Structured DQ DataFrame |
| Parquet Write | Write partitioned columnar format | partitionBy, repartition(8) |
| MySQL Load | Load to relational database | JDBC, batchsize=5000 |
| Broadcast Join | Enrich with country-region lookup | F.broadcast() |

---

## Transformations Applied

1. Column rename to snake_case
2. InvoiceDate string to Timestamp cast
3. Null drop for critical columns (invoice_id, stock_code, quantity, unit_price)
4. Null replace for customer_id (null -> GUEST)
5. Deduplication on (invoice_id, stock_code)
6. Filter: quantity > 0, unit_price > 0, valid date, valid country
7. Derived columns: invoice_year, invoice_month, invoice_day, total_amount, is_return, price_bucket
8. Column selection to define output schema

---

## Data Quality Framework

18 checks across 5 categories:
- Row count validation
- Null percentage per column (threshold: 10%)
- Duplicate count check (threshold: 5%)
- Schema validation (expected vs actual columns)
- Business rule validation (total_amount > 0, year in valid range)

All results stored as a structured Spark DataFrame and saved as JSON.

---

## Spark Optimizations

- `repartition(8, "invoice_year")` before Parquet write to control file count
- `cache()` before quality checks, `unpersist()` when done
- `F.broadcast()` for the country-region join (avoids 500K row shuffle)
- `explain()` used to verify execution plans
- `spark.sql.shuffle.partitions=8` for local mode efficiency
- `KryoSerializer` for faster broadcast and shuffle serialization

---

## MySQL Table Design

The `retail_transactions` table uses:
- `BIGINT AUTO_INCREMENT` surrogate primary key for clustered index
- `UNIQUE INDEX` on (invoice_id, stock_code) for business key integrity
- Composite index on (country, invoice_year) for regional analytics
- Index on invoice_date for date range scans
- `ROW_FORMAT=COMPRESSED` for storage efficiency

---

## Running the Notebook

The notebook is in `notebooks/etl_pipeline.py` with `# %%` cell markers.

Open it in VS Code with the Jupyter extension, or convert to .ipynb:

```bash
pip install jupytext
jupytext --to notebook notebooks/etl_pipeline.py
jupyter notebook notebooks/etl_pipeline.ipynb
```

---

## Tech Stack

- Apache PySpark 3.5
- Python 3.10
- MySQL 8.0
- Apache Parquet
- Streamlit (demo app)
- Plotly (visualizations)
