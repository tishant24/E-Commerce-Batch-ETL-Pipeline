# Databricks Community Edition Setup Guide

Step-by-step instructions to run this pipeline on the free Databricks tier.

---

## Step 1: Create Free Account

1. Go to: **community.cloud.databricks.com**
2. Click "Get started for free"
3. Sign up with email — no credit card required
4. Verify your email

---

## Step 2: Create a Cluster

In Databricks Community Edition, you get one cluster at a time.

1. Click **Compute** in the left sidebar
2. Click **Create Cluster**
3. Use these settings:

```
Cluster Name:    etl-pipeline-cluster
Cluster Mode:    Single Node (CE only supports this)
Databricks Runtime: 13.3 LTS (includes Spark 3.4.1, Python 3.10)
Node Type:       (default — 15.3 GB Memory, 2 Cores)
```

4. Click **Create Cluster**
5. Wait ~3 minutes for the cluster to start (green circle = running)

> **Note:** Community Edition clusters auto-terminate after 2 hours of inactivity.
> Your notebooks and data are saved, but you'll need to restart the cluster.

---

## Step 3: Import Notebooks

### Option A: Import .py files directly

1. Click **Workspace** in the left sidebar
2. Right-click your username folder → **Create → Folder** → name it "ecommerce-etl"
3. Click into that folder
4. Click the folder icon → **Import**
5. Select **File** → drag each `.py` file from `notebooks/` folder
6. Repeat for all 7 notebooks

### Option B: Clone from GitHub (Recommended)

1. Click **Repos** in the left sidebar
2. Click **Add Repo**
3. Paste your GitHub URL
4. Click **Create Repo**

All notebooks will be available in your Databricks workspace.

---

## Step 4: Set Up DBFS Folders

Run this in any notebook cell:

```python
import os

folders = [
    "/dbfs/FileStore/ecommerce_raw/",
    "/dbfs/FileStore/ecommerce_pipeline/processed/",
    "/dbfs/FileStore/ecommerce_pipeline/curated/",
    "/dbfs/FileStore/ecommerce_pipeline/logs/",
    "/dbfs/FileStore/ecommerce_pipeline/dq_report/",
    "/dbfs/FileStore/ecommerce_pipeline/sql/",
]

for folder in folders:
    os.makedirs(folder, exist_ok=True)
    print(f"Created: {folder}")

print("All DBFS folders ready!")
```

---

## Step 5: Install MySQL Connector (if using MySQL)

In your cluster's **Libraries** tab:
1. Click **Install New**
2. Source: **Maven**
3. Coordinates: `mysql:mysql-connector-java:8.0.33`
4. Click **Install**
5. Wait for installation to complete

---

## Step 6: Run the Pipeline

### Option A: Run full pipeline (recommended)
1. Open `07_pipeline_orchestrator.py`
2. Make sure your cluster is attached (top right dropdown)
3. Click **Run All** (Ctrl+Shift+Enter)

### Option B: Run notebooks individually (for learning)
Run in this exact order:
1. `01_data_ingestion.py` — ~2 minutes
2. `02_transformations.py` — ~1 minute
3. `03_data_quality.py` — ~1 minute
4. `04_write_parquet.py` — ~1 minute
5. `05_mysql_load.py` — optional, requires MySQL
6. `06_optimizations.py` — ~1 minute

---

## Step 7: Set Up MySQL (Optional)

### Free MySQL Options

**Option A: PlanetScale (Recommended)**
1. Go to planetscale.com → Sign up (free tier)
2. Create a database named `ecommerce_dw`
3. Get connection string
4. Run `sql/create_tables.sql` in their web console

**Option B: Railway**
1. Go to railway.app → Sign up (free tier)
2. Create a MySQL service
3. Copy connection details

**Option C: Local MySQL + ngrok**
```bash
# Start local MySQL
mysql -u root -p < sql/create_tables.sql

# Install ngrok and expose port 3306
ngrok tcp 3306
# Use the ngrok URL in Databricks widgets
```

### Configure in Databricks

In notebook `05_mysql_load.py`, fill in the widgets at the top:
- **MySQL Host:** your-host.planetscale.com
- **MySQL Port:** 3306
- **Database Name:** ecommerce_dw
- **MySQL Username:** your-username
- **MySQL Password:** your-password

---

## Common Issues

**Issue:** `AnalysisException: Path does not exist`
**Fix:** Run Step 4 (create DBFS folders) first

**Issue:** Cluster not attached
**Fix:** Click the cluster dropdown (top of notebook) → select your cluster

**Issue:** Java JDBC error for MySQL
**Fix:** Install the Maven library in Step 5

**Issue:** `dbutils not defined`
**Fix:** You're running locally. `dbutils` is Databricks-only. Use local SparkSession instead.

**Issue:** Out of memory on Community Edition
**Fix:** Reduce dataset size in `config/pipeline_config.py`:
```python
NUM_RECORDS = 100_000  # Reduce from 500K for testing
```

---

## What Files Are Created

After a successful run, DBFS will contain:

```
/FileStore/ecommerce_raw/
  orders_raw.csv                    ← 500K row raw file

/FileStore/ecommerce_pipeline/
  processed/orders/
    order_year=2022/
      order_month=1/part-00000.parquet
      order_month=2/part-00000.parquet
      ...
    order_year=2023/
      ...
  dq_report/
    part-00000.json                 ← DQ report as JSON
  logs/
    part-00000.json                 ← Pipeline execution log
```

View files with:
```python
display(dbutils.fs.ls("dbfs:/FileStore/ecommerce_pipeline/"))
```
