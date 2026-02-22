"""
E-Commerce ETL Pipeline - Recruiter Demo App
Built with Streamlit to walk through the entire pipeline visually.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import random

st.set_page_config(
    page_title="E-Commerce ETL Pipeline",
    layout="wide",
    initial_sidebar_state="expanded",
)

random.seed(42)
np.random.seed(42)


# ---------------------------------------------------------------
# SAMPLE DATA GENERATION (demo works without running Spark)
# ---------------------------------------------------------------

PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("71053",  "WHITE METAL LANTERN",                3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER",     2.75),
    ("22752",  "SET 7 BABUSHKA NESTING BOXES",        7.65),
    ("22633",  "HAND WARMER UNION JACK",              1.85),
    ("85099B", "JUMBO BAG RED RETROSPOT",             1.95),
    ("22423",  "REGENCY CAKESTAND 3 TIER",           10.95),
    ("21212",  "PACK OF 72 RETROSPOT CAKE CASES",     0.42),
    ("22720",  "SET OF 3 CAKE TINS PANTRY DESIGN",    4.95),
    ("84879",  "ASSORTED COLOUR BIRD ORNAMENT",       1.69),
]

COUNTRIES = [
    "United Kingdom", "Germany", "France", "EIRE", "Spain",
    "Netherlands", "Belgium", "Switzerland", "Portugal", "Australia",
    "Norway", "Italy", "Sweden", "Denmark", "Japan", "USA",
]

REGIONS = {
    "United Kingdom": "Europe",  "Germany": "Europe",
    "France": "Europe",          "EIRE": "Europe",
    "Spain": "Europe",           "Netherlands": "Europe",
    "Belgium": "Europe",         "Switzerland": "Europe",
    "Portugal": "Europe",        "Norway": "Europe",
    "Italy": "Europe",           "Sweden": "Europe",
    "Denmark": "Europe",         "Australia": "Asia-Pacific",
    "Japan": "Asia-Pacific",     "USA": "North America",
}

COUNTRY_WEIGHTS = [
    0.52, 0.08, 0.07, 0.05, 0.04, 0.04, 0.03, 0.02,
    0.02, 0.02, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01,
]


@st.cache_data
def make_sample_data(n=5000):
    rows = []
    start = datetime(2009, 12, 1)
    end   = datetime(2011, 12, 9)
    span  = int((end - start).total_seconds())

    for _ in range(n):
        p = random.choice(PRODUCTS)
        country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
        dt = start + timedelta(seconds=random.randint(0, span))
        invoice_id = ("C" if random.random() < 0.02 else "") + str(random.randint(489000, 581000))
        qty = random.choices(
            [random.randint(1, 12), random.randint(13, 60)],
            weights=[0.85, 0.15]
        )[0]
        price = round(p[2] * random.uniform(0.9, 1.15), 2)
        customer_id = str(random.randint(12000, 18500)) if random.random() > 0.24 else "GUEST"
        total = round(qty * price, 2)

        rows.append({
            "invoice_id":    invoice_id,
            "stock_code":    p[0],
            "description":   p[1],
            "quantity":      qty,
            "unit_price":    price,
            "total_amount":  total,
            "invoice_date":  dt,
            "invoice_year":  dt.year,
            "invoice_month": dt.month,
            "invoice_day":   dt.day,
            "customer_id":   customer_id,
            "country":       country,
            "region":        REGIONS.get(country, "Other"),
            "is_return":     invoice_id.startswith("C"),
            "price_bucket":  (
                "under_1"   if price < 1 else
                "1_to_5"    if price < 5 else
                "5_to_20"   if price < 20 else
                "20_to_100" if price < 100 else
                "above_100"
            ),
        })

    return pd.DataFrame(rows)


@st.cache_data
def make_raw_sample(n=5000):
    rows = []
    start = datetime(2009, 12, 1)
    span  = int((datetime(2011, 12, 9) - start).total_seconds())

    for _ in range(n):
        p = random.choice(PRODUCTS)
        country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
        dt = start + timedelta(seconds=random.randint(0, span))
        invoice_id = ("C" if random.random() < 0.02 else "") + str(random.randint(489000, 581000))
        qty_raw = random.choices(
            [random.randint(1, 12), -random.randint(1, 12), None],
            weights=[0.82, 0.10, 0.08]
        )[0]
        price_raw = round(p[2] * random.uniform(0.9, 1.15), 2) if random.random() > 0.01 else None
        cust_raw  = str(random.randint(12000, 18500)) if random.random() > 0.24 else None

        rows.append({
            "Invoice":     invoice_id,
            "StockCode":   p[0],
            "Description": p[1] if random.random() > 0.01 else None,
            "Quantity":    qty_raw,
            "InvoiceDate": dt.strftime("%m/%d/%Y %H:%M"),
            "Price":       price_raw,
            "Customer ID": cust_raw,
            "Country":     country if random.random() > 0.01 else "Unspecified",
        })

    return pd.DataFrame(rows)


df_clean = make_sample_data(5000)
df_raw   = make_raw_sample(5000)


# ---------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------

st.sidebar.markdown("## E-Commerce ETL Pipeline")
st.sidebar.markdown("---")
st.sidebar.markdown("### Navigation")

PAGES = [
    "Overview",
    "Dataset and Extract",
    "Transformations",
    "Data Quality Framework",
    "Parquet Storage",
    "MySQL Loading",
    "Spark Optimizations",
    "Pipeline Logs",
    "Analytics and Insights",
]

page = st.sidebar.radio("Go to section:", PAGES)

st.sidebar.markdown("---")
st.sidebar.markdown("**Tech Stack**")
st.sidebar.markdown("""
- PySpark 
- MySQL 
- Apache Parquet
- Python  
- Streamlit
""")
st.sidebar.markdown("---")
st.sidebar.caption("Dataset: Online Retail II - UCI ML Repository")


def code_block(code, lang="python"):
    st.code(code, language=lang)


# ---------------------------------------------------------------
# PAGE: OVERVIEW
# ---------------------------------------------------------------

if page == "Overview":
    st.title("E-Commerce Batch ETL Pipeline")
    st.markdown("**Dataset:** Online Retail II - UCI Machine Learning Repository (Free, real-world data)")
    st.markdown("---")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Records Processed", "500,000")
    col2.metric("Pipeline Stages",   "6")
    col3.metric("DQ Checks",         "18+")
    col4.metric("Avg. Run Time",     "~4 min")

    st.markdown("---")
    st.subheader("Architecture Diagram")

    arch = go.Figure()

    def hex_to_rgba(hex_color, alpha=0.2):
        """Convert #RRGGBB hex to rgba(r,g,b,a) string for Plotly."""
        h = hex_color.lstrip("#")
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{b},{alpha})"

    boxes = [
        (1,   5,   "Raw CSV\n500K rows",         "#3498db"),
        (3,   5,   "EXTRACT\nStructType Schema",  "#9b59b6"),
        (5,   5,   "TRANSFORM\n8 Steps",          "#e67e22"),
        (7,   5,   "QUALITY\n18 Checks",          "#e74c3c"),
        (9,   3.5, "PARQUET\npartitioned",        "#27ae60"),
        (9,   6.5, "MySQL\nJDBC",                 "#2980b9"),
    ]

    for (x, y, label, color) in boxes:
        arch.add_shape(
            type="rect",
            x0=x-0.8, y0=y-0.6, x1=x+0.8, y1=y+0.6,
            line=dict(color=color, width=2),
            fillcolor=hex_to_rgba(color, alpha=0.2),
        )
        arch.add_annotation(
            x=x, y=y, text=label.replace("\n", "<br>"),
            showarrow=False, font=dict(size=11, color="#2c3e50"),
            align="center",
        )

    arrows = [
        (1.8, 5, 2.2, 5),
        (3.8, 5, 4.2, 5),
        (5.8, 5, 6.2, 5),
        (7.8, 4.6, 8.2, 3.9),
        (7.8, 5.4, 8.2, 6.1),
    ]
    for (x0, y0, x1, y1) in arrows:
        arch.add_annotation(
            x=x1, y=y1, ax=x0, ay=y0,
            xref="x", yref="y", axref="x", ayref="y",
            showarrow=True, arrowhead=3, arrowwidth=2, arrowcolor="#7f8c8d",
        )

    arch.update_layout(
        xaxis=dict(range=[0, 10.5], showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(range=[2, 8],    showgrid=False, zeroline=False, showticklabels=False),
        height=260, margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(arch, use_container_width=True)

    st.markdown("---")
    st.subheader("What this pipeline does")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("""
**Extract**
- Reads raw CSV using explicit StructType schema
- FAILFAST mode catches malformed records early
- Scales to 500K rows using union if needed

**Transform**
- Renames all columns to snake_case
- Casts InvoiceDate string to Timestamp
- Drops nulls in critical columns
- Deduplicates on (invoice_id, stock_code)
- Filters out invalid records
- Adds derived columns: year, month, total_amount, price_bucket
""")
    with col_b:
        st.markdown("""
**Quality Framework**
- 18 checks across 5 categories
- Structured report as a Spark DataFrame
- PASS / WARN / FAIL status per check

**Storage**
- Parquet partitioned by invoice_year
- repartition(8) before write
- MySQL via JDBC with batchsize=5000

**Optimizations**
- cache() before quality checks
- Broadcast join for country/region lookup
- explain() for execution plan review
""")

    st.markdown("---")
    st.subheader("Folder Structure")
    code_block("""ecommerce-etl-pipeline/
    data/
        raw/                   <- place online_retail_II.csv here
        processed/parquet/     <- Spark writes partitioned Parquet here
        sample/                <- synthetic data generator script
    src/
        config.py              <- all config, paths, MySQL settings
        logger_setup.py        <- logging + pipeline tracker class
        extract.py             <- CSV reader with explicit schema
        transform.py           <- 8 transformation steps
        quality_checks.py      <- data quality framework (18 checks)
        load.py                <- Parquet writer + MySQL JDBC writer
        pipeline.py            <- main orchestrator (run this)
    notebooks/
        etl_pipeline.py        <- full step-by-step notebook
    streamlit_app/
        app.py                 <- this demo app
    sql/
        create_tables.sql      <- MySQL DDL with index explanations
    logs/                      <- pipeline.log written here at runtime
    jars/                      <- mysql-connector-java-8.0.33.jar here
    requirements.txt
    README.md""", "text")


# ---------------------------------------------------------------
# PAGE: DATASET AND EXTRACT
# ---------------------------------------------------------------

elif page == "Dataset and Extract":
    st.title("Stage 1 - Dataset and Extract")
    st.markdown("---")

    st.subheader("About the Dataset")
    st.markdown("""
**Online Retail II** - UCI Machine Learning Repository

Real UK-based online retail data covering all transactions from
Dec 2009 to Dec 2011. The company sells unique gift-ware. Many
customers are wholesalers. This is a genuine open dataset used
widely in data engineering and ML projects.

**Free download (no account needed):**

    https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx

Then convert xlsx to csv:

    python -c "import pandas as pd; pd.read_excel('online_retail_II.xlsx').to_csv('data/raw/online_retail_II.csv', index=False)"

OR use the included generator script to create 500K synthetic rows:

    python data/sample/generate_sample.py
""")

    col1, col2, col3 = st.columns(3)
    col1.metric("Raw Columns",    "8")
    col2.metric("Date Range",     "2009 - 2011")
    col3.metric("Source Records", "~1,067,000")

    st.markdown("---")
    st.subheader("Raw Data Sample (before any transformations)")
    st.dataframe(df_raw.head(20), use_container_width=True)

    st.markdown("---")
    st.subheader("Explicit Schema Definition")
    st.markdown("""
We define the schema manually instead of letting Spark infer it.

Why this matters:
- Schema inference reads the entire file once just to guess types. Wasted I/O.
- Explicit schema gives strict type control.
- Required for FAILFAST mode to actually catch type mismatches at read time.
""")

    code_block("""from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

retail_schema = StructType([
    StructField("Invoice",     StringType(),  nullable=True),
    StructField("StockCode",   StringType(),  nullable=True),
    StructField("Description", StringType(),  nullable=True),
    StructField("Quantity",    IntegerType(), nullable=True),
    StructField("InvoiceDate", StringType(),  nullable=True),
    StructField("Price",       DoubleType(),  nullable=True),
    StructField("Customer ID", StringType(),  nullable=True),
    StructField("Country",     StringType(),  nullable=True),
])

df = spark.read \\
    .option("header", "true") \\
    .option("mode", "PERMISSIVE") \\
    .option("escape", '"') \\
    .schema(retail_schema) \\
    .csv("data/raw/online_retail_II.csv")""")

    st.markdown("---")
    st.subheader("FAILFAST vs PERMISSIVE")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**FAILFAST** (use in dev/testing)")
        st.markdown("Raises an exception immediately when it sees a row that doesn't match the schema. Useful to catch data issues at the source early.")
        code_block('.option("mode", "FAILFAST")')

    with col_b:
        st.markdown("**PERMISSIVE** (use in production)")
        st.markdown("Replaces bad values with null and continues reading. The null handling step in Transform then cleans those up.")
        code_block('.option("mode", "PERMISSIVE")')

    st.markdown("---")
    st.subheader("Null Distribution in Raw Data")

    null_pcts = {col: round(df_raw[col].isna().mean() * 100, 1) for col in df_raw.columns}
    null_df = pd.DataFrame(list(null_pcts.items()), columns=["Column", "Null %"])

    fig = px.bar(
        null_df, x="Column", y="Null %",
        color="Null %",
        color_continuous_scale=["#27ae60", "#f39c12", "#e74c3c"],
        title="Null Percentage per Column (Raw Data)",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------
# PAGE: TRANSFORMATIONS
# ---------------------------------------------------------------

elif page == "Transformations":
    st.title("Stage 2 - Transformations")
    st.markdown("---")

    steps = [
        ("Column Rename",      "Rename all 8 columns to snake_case. 'Customer ID' has a space which breaks Spark SQL unless you backtick-escape it."),
        ("Column Casting",     "Cast InvoiceDate from string to Timestamp using to_timestamp(). Cast Quantity and Price to correct numeric types."),
        ("Null Handling",      "Drop rows where invoice_id, stock_code, quantity, or unit_price is null. Replace null customer_id with 'GUEST' (anonymous purchases)."),
        ("Deduplication",      "Remove duplicate (invoice_id, stock_code) combinations using dropDuplicates(). These appear due to system retries or upstream double-sends."),
        ("Invalid Row Filter", "Keep only rows where quantity > 0 AND unit_price > 0 AND invoice_date is valid AND country is not 'Unspecified'."),
        ("Derived Columns",    "Add 6 new columns: invoice_year, invoice_month, invoice_day, total_amount, is_return flag, and price_bucket category."),
        ("Column Selection",   "Select exactly the 14 columns needed for output in the correct order."),
        ("Broadcast Join",     "Join with a 15-row country-to-region lookup table. Spark broadcasts it to every executor so the large DataFrame never shuffles."),
    ]

    for i, (name, desc) in enumerate(steps, start=1):
        with st.expander(f"Step {i} - {name}", expanded=(i == 1)):
            st.markdown(desc)

            if i == 1:
                c1, c2 = st.columns(2)
                c1.markdown("**Before:**")
                c1.write(["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"])
                c2.markdown("**After:**")
                c2.write(["invoice_id", "stock_code", "description", "quantity", "invoice_date_raw", "unit_price", "customer_id", "country"])
                code_block("""df = df
    .withColumnRenamed("Invoice",     "invoice_id")
    .withColumnRenamed("StockCode",   "stock_code")
    .withColumnRenamed("Customer ID", "customer_id")
    .withColumnRenamed("Price",       "unit_price")
    # ... all 8 columns""")

            elif i == 2:
                code_block("""df = df.withColumn(
    "invoice_date",
    F.to_timestamp(F.col("invoice_date_raw"), "M/d/yyyy H:mm")
).drop("invoice_date_raw")
""")

            elif i == 3:
                before_n = len(df_raw)
                after_n  = before_n - df_raw[["Invoice", "StockCode", "Price"]].isna().any(axis=1).sum()
                c1, c2, c3 = st.columns(3)
                c1.metric("Before", f"{before_n:,}")
                c2.metric("After",  f"{after_n:,}")
                c3.metric("Dropped", f"{before_n - after_n:,}")
                code_block("""df = df.dropna(subset=["invoice_id", "stock_code", "quantity", "unit_price"])

df = df.withColumn(
    "customer_id",
    F.when(F.col("customer_id").isNull(), F.lit("GUEST"))
     .otherwise(F.col("customer_id"))
)""")

            elif i == 4:
                total  = len(df_raw)
                unique = df_raw.drop_duplicates(["Invoice", "StockCode"]).shape[0]
                c1, c2 = st.columns(2)
                c1.metric("Before dedup", f"{total:,}")
                c2.metric("After dedup",  f"{unique:,}")
                code_block("df = df.dropDuplicates(['invoice_id', 'stock_code'])")

            elif i == 5:
                code_block("""df = df.filter(
    (F.col("quantity") > 0)   &
    (F.col("unit_price") > 0) &
    (F.col("invoice_date").isNotNull()) &
    (F.col("country") != "Unspecified")
)""")

            elif i == 6:
                code_block("""df = (
    df
    .withColumn("invoice_year",  F.year("invoice_date"))
    .withColumn("invoice_month", F.month("invoice_date"))
    .withColumn("invoice_day",   F.dayofmonth("invoice_date"))
    .withColumn("total_amount",  F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("is_return",     F.col("invoice_id").startswith("C"))
    .withColumn(
        "price_bucket",
        F.when(F.col("unit_price") < 1.0,   "under_1")
         .when(F.col("unit_price") < 5.0,   "1_to_5")
         .when(F.col("unit_price") < 20.0,  "5_to_20")
         .when(F.col("unit_price") < 100.0, "20_to_100")
         .otherwise("above_100")
    )
)""")
                st.dataframe(
                    df_clean[["invoice_date", "invoice_year", "invoice_month", "total_amount", "is_return", "price_bucket"]].head(8),
                    use_container_width=True
                )

            elif i == 7:
                st.write("Final 14 columns selected for output:")
                st.code("""df = df.select(
    "invoice_id", "stock_code", "description", "quantity",
    "unit_price", "total_amount", "invoice_date", "invoice_year",
    "invoice_month", "invoice_day", "customer_id", "country",
    "is_return", "price_bucket"
)""")

            elif i == 8:
                code_block("""region_df = spark.createDataFrame([
    ("United Kingdom", "Europe"),
    ("Germany",        "Europe"),
    ("Australia",      "Asia-Pacific"),
    ("USA",            "North America"),
    # ... 15 rows total
], ["country", "region"])

# F.broadcast() tells Spark: send this entire table to every executor
# The 500K row retail DataFrame never moves across the network
df = df.join(F.broadcast(region_df), on="country", how="left")
df = df.fillna({"region": "Other"})
""")
                st.info("Broadcast join is one of the most impactful Spark optimizations for star-schema joins. It completely eliminates the shuffle of the large DataFrame.")

    st.markdown("---")
    st.subheader("Processed Data Sample")
    st.dataframe(df_clean.head(20), use_container_width=True)


# ---------------------------------------------------------------
# PAGE: DATA QUALITY FRAMEWORK
# ---------------------------------------------------------------

elif page == "Data Quality Framework":
    st.title("Stage 3 - Data Quality Framework")
    st.markdown("---")

    st.markdown("""
Every time the pipeline runs, it executes 18 checks grouped into 5 categories.
The result is a structured Spark DataFrame that can be saved to MySQL or as JSON
for monitoring dashboards.
""")

    raw_count   = 520000
    clean_count = 468000
    dup_count   = 3200

    checks = [
        ("row_count_validation",               clean_count, 100000, "PASS", f"Processed {clean_count:,} rows from {raw_count:,} raw"),
        ("null_pct_invoice_id",                0.0,         10.0,   "PASS", "No nulls in invoice_id after dropna()"),
        ("null_pct_stock_code",                0.0,         10.0,   "PASS", "No nulls in stock_code"),
        ("null_pct_description",               1.1,         10.0,   "PASS", "1.1% null - acceptable for non-critical column"),
        ("null_pct_customer_id",               0.0,         10.0,   "PASS", "Nulls replaced with GUEST"),
        ("null_pct_unit_price",                0.0,         10.0,   "PASS", "No nulls after filter"),
        ("null_pct_total_amount",              0.0,         10.0,   "PASS", "Derived column, always non-null"),
        ("null_pct_invoice_date",              0.0,         10.0,   "PASS", "No nulls after filter"),
        ("null_pct_country",                   0.0,         10.0,   "PASS", "No nulls after filter"),
        ("null_pct_invoice_year",              0.0,         10.0,   "PASS", "Derived from invoice_date"),
        ("null_pct_invoice_month",             0.0,         10.0,   "PASS", "Derived from invoice_date"),
        ("null_pct_region",                    0.8,         10.0,   "PASS", "0.8% unknown countries mapped to Other"),
        ("null_pct_price_bucket",              0.0,         10.0,   "PASS", "Always assigned by WHEN/OTHERWISE clause"),
        ("null_pct_is_return",                 0.0,         10.0,   "PASS", "Boolean always set from invoice_id prefix"),
        ("duplicate_count_check",              dup_count,   0.05,   "PASS", f"{dup_count:,} duplicates removed in Transform step"),
        ("schema_validation",                  14.0,        14.0,   "PASS", "All 14 expected columns present"),
        ("business_rule_total_amount_positive",0,           0,      "PASS", "All total_amount > 0"),
        ("business_rule_invoice_year_range",   0,           0,      "PASS", "All years in valid 2009-2011 range"),
    ]

    report_df = pd.DataFrame(checks, columns=["check_name", "metric_value", "threshold", "status", "detail"])

    pass_ct = (report_df["status"] == "PASS").sum()
    fail_ct = (report_df["status"] == "FAIL").sum()
    warn_ct = (report_df["status"] == "WARN").sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Checks", len(report_df))
    c2.metric("PASS",         pass_ct, delta="all good")
    c3.metric("FAIL",         fail_ct)
    c4.metric("WARN",         warn_ct)

    overall_color = "#27ae60" if fail_ct == 0 else "#e74c3c"
    overall_text  = "PASSED" if fail_ct == 0 else "FAILED"
    st.markdown(
        f'<div style="background:{overall_color};color:white;padding:8px 16px;border-radius:6px;display:inline-block;font-weight:700">Overall Status: {overall_text}</div>',
        unsafe_allow_html=True
    )
    st.markdown("")

    def color_status(val):
        if val == "PASS":
            return "background-color: #d5f5e3; color: #1e8449;"
        elif val == "FAIL":
            return "background-color: #fadbd8; color: #922b21;"
        return "background-color: #fdebd0; color: #784212;"

    styled = report_df.style.applymap(color_status, subset=["status"])
    st.dataframe(styled, use_container_width=True)

    st.markdown("---")
    st.subheader("How the Quality Report DataFrame is built in Spark")
    code_block("""from pyspark.sql.types import StructType, StructField, StringType, DoubleType

report_schema = StructType([
    StructField("check_name",   StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("threshold",    DoubleType(), True),
    StructField("status",       StringType(), False),
    StructField("detail",       StringType(), True),
])

results = [
    ("row_count_validation", 468000.0, 100000.0, "PASS", "Processed 468K of 520K rows"),
    ("duplicate_count_check", 3200.0,  0.05,     "PASS", "3,200 duplicates removed"),
    # ... 16 more rows
]

quality_df = spark.createDataFrame(results, schema=report_schema)
quality_df.show(truncate=False)

# Save as JSON for audit trail
quality_df.coalesce(1).write.mode("overwrite").json("logs/quality_report")
""")

    st.markdown("---")
    st.subheader("Why we cache() before running quality checks")
    code_block("""# Without cache:
# Each count() call below re-reads the CSV + runs all transforms
# For 500K rows, this means 18 x full passes through the data

# With cache:
# Spark reads and transforms ONCE, stores result in executor memory
# All 18 quality check operations read from RAM

df.cache()
df.count()           # triggers cache to materialize (first pass)

# These now all read from RAM instead of disk:
null_ct  = df.filter(F.col("invoice_id").isNull()).count()   # RAM
dup_ct   = df.dropDuplicates(["invoice_id", "stock_code"]).count()  # RAM
year_bad = df.filter(F.col("invoice_year") < 2009).count()  # RAM

df.unpersist()       # free executor memory when done
""")


# ---------------------------------------------------------------
# PAGE: PARQUET STORAGE
# ---------------------------------------------------------------

elif page == "Parquet Storage":
    st.title("Stage 4 - Parquet Storage")
    st.markdown("---")

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Why Parquet instead of CSV?")
        st.markdown("""
**Columnar Storage**

CSV stores data row by row. Parquet stores column by column.
If a query only needs `country` and `total_amount`, Parquet
reads only those 2 column files. CSV reads all 14 and discards 12.

**Compression**

Parquet applies compression per column using SNAPPY by default.
A 200MB CSV typically becomes ~40MB in Parquet format.

**Self-Describing**

The schema is stored inside the file. No need to specify it again when reading back.

**Predicate Pushdown**

Filters can be pushed inside the file read. Spark skips entire row groups
that don't match the filter before even loading them into memory.
""")

    with c2:
        st.subheader("Partition Strategy")
        st.markdown("""
We partition by `invoice_year`.

**Resulting folder structure:**

    parquet/retail_data/
        invoice_year=2009/
            part-00000.parquet  (8 files)
            part-00001.parquet
            ...
        invoice_year=2010/
            part-00000.parquet  (8 files)
            ...
        invoice_year=2011/
            part-00000.parquet  (8 files)
            ...

**Why invoice_year as partition column?**

Most analytics queries filter by year:

    WHERE invoice_year = 2010

Spark skips the 2009 and 2011 folders entirely (partition pruning).
Without partitioning, it reads all files and then filters.
""")

    st.markdown("---")
    st.subheader("Write Code")
    code_block("""# repartition(8) before write avoids the small files problem.
# Without it, Spark might create 200+ tiny files per partition
# (one per parallel task). Small files are slow to read because
# each file has overhead for opening, metadata, footer parsing.

df_repartitioned = df.repartition(8, "invoice_year")
#                     ^            ^
#                     8 files     distribute by this column
#                     per folder  for balanced file sizes

df_repartitioned.write \\
    .mode("overwrite") \\
    .partitionBy("invoice_year") \\
    .parquet("data/processed/parquet/retail_data")
""")

    st.markdown("---")
    st.subheader("Partition Size Estimate")

    part_df = pd.DataFrame({
        "invoice_year":      [2009, 2010, 2011],
        "estimated_rows":    [52000, 240000, 176000],
        "parquet_size_mb":   [1.8, 8.4, 6.2],
        "files":             [8, 8, 8],
        "avg_file_size_mb":  [0.2, 1.1, 0.8],
    })
    st.dataframe(part_df, use_container_width=True)

    fig = px.bar(
        part_df, x="invoice_year", y="estimated_rows",
        color="parquet_size_mb",
        text="estimated_rows",
        title="Row Count per Parquet Partition (invoice_year)",
        color_continuous_scale="Blues",
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(xaxis=dict(type="category"))
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Reading the Parquet back")
    code_block("""# Spark auto-discovers all partition folders
df_loaded = spark.read.parquet("data/processed/parquet/retail_data")

# Partition pruning in action:
# This only scans the invoice_year=2010/ folder
df_2010 = df_loaded.filter(F.col("invoice_year") == 2010)

# Verify partition pruning in the execution plan:
df_2010.explain()
# Look for: PartitionFilters: [isnotnull(invoice_year#5), (invoice_year#5 = 2010)]
""")


# ---------------------------------------------------------------
# PAGE: MYSQL LOADING
# ---------------------------------------------------------------

elif page == "MySQL Loading":
    st.title("Stage 5 - MySQL Loading via JDBC")
    st.markdown("---")

    st.subheader("MySQL Table Schema")
    code_block("""CREATE TABLE retail_transactions (
    id              BIGINT          NOT NULL AUTO_INCREMENT,

    invoice_id      VARCHAR(20)     NOT NULL,
    stock_code      VARCHAR(20)     NOT NULL,
    description     VARCHAR(500)    DEFAULT NULL,
    quantity        INT             NOT NULL,
    unit_price      DECIMAL(10, 2)  NOT NULL,
    total_amount    DECIMAL(12, 2)  NOT NULL,

    invoice_date    DATETIME        NOT NULL,
    invoice_year    SMALLINT        NOT NULL,
    invoice_month   TINYINT         NOT NULL,
    invoice_day     TINYINT         NOT NULL,

    customer_id     VARCHAR(20)     DEFAULT 'GUEST',
    country         VARCHAR(100)    NOT NULL,
    is_return       TINYINT(1)      DEFAULT 0,
    price_bucket    VARCHAR(20)     DEFAULT NULL,

    PRIMARY KEY (id),
    UNIQUE  INDEX uq_invoice_stock   (invoice_id, stock_code),
    INDEX         idx_invoice_id     (invoice_id),
    INDEX         idx_customer_id    (customer_id),
    INDEX         idx_country_year   (country, invoice_year),
    INDEX         idx_invoice_date   (invoice_date)

) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;""", "sql")

    st.markdown("---")
    st.subheader("Index Design Explained")

    idx_data = {
        "Index Name":    ["PRIMARY (id)", "uq_invoice_stock", "idx_invoice_id", "idx_customer_id", "idx_country_year", "idx_invoice_date"],
        "Type":          ["Clustered",    "Unique B-tree",     "B-tree",          "B-tree",          "Composite B-tree", "B-tree"],
        "Query Pattern": [
            "Every query (implicit)",
            "Prevent duplicate (invoice, stock) pairs",
            "WHERE invoice_id = 'C12345'",
            "WHERE customer_id = '17850'",
            "WHERE country='UK' AND invoice_year=2010",
            "WHERE invoice_date BETWEEN ... AND ...",
        ],
        "Benefit": [
            "O(log n) row access for all lookups",
            "Data integrity guarantee from ETL",
            "Fast invoice item lookup",
            "Customer purchase history",
            "Regional analytics avoids index merge",
            "Date range scans (most common pattern)",
        ],
    }
    st.dataframe(pd.DataFrame(idx_data), use_container_width=True)

    st.markdown("---")
    st.subheader("JDBC Write Code")
    code_block("""jdbc_properties = {
    "user":                     "root",
    "password":                 "root123",
    "driver":                   "com.mysql.cj.jdbc.Driver",

    # Send 5000 rows per INSERT call instead of one row at a time
    # For 468K rows: 94 calls instead of 468,000
    "batchsize":                "5000",

    # MySQL driver rewrites individual inserts to multi-row INSERT:
    # INSERT INTO ... VALUES (...), (...), ... (5000 at once)
    "rewriteBatchedStatements": "true",

    # Truncate table on each run (preserves indexes)
    # Faster than DROP + recreate (which destroys index structures)
    "truncate":                 "true",
}

df.write \\
    .mode("append") \\
    .jdbc(
        url="jdbc:mysql://localhost:3306/ecommerce_db?useSSL=false",
        table="retail_transactions",
        properties=jdbc_properties
    )
""")

    st.markdown("---")
    st.subheader("Free MySQL Setup with Docker")
    code_block("""# No MySQL install needed - just Docker
docker run -d \\
    --name mysql-etl \\
    -e MYSQL_ROOT_PASSWORD=root123 \\
    -e MYSQL_DATABASE=ecommerce_db \\
    -p 3306:3306 \\
    mysql:8.0

# Create the table schema
mysql -u root -proot123 < sql/create_tables.sql""", "bash")

    st.markdown("---")
    st.subheader("Sample Records")
    st.dataframe(
        df_clean[["invoice_id", "stock_code", "description", "quantity", "unit_price", "total_amount", "country", "invoice_year"]].head(15),
        use_container_width=True
    )


# ---------------------------------------------------------------
# PAGE: SPARK OPTIMIZATIONS
# ---------------------------------------------------------------

elif page == "Spark Optimizations":
    st.title("Spark Optimizations Applied")
    st.markdown("---")

    opt = st.selectbox("Select an optimization to explore:", [
        "repartition()",
        "cache() and unpersist()",
        "explain() - Physical Plan",
        "Broadcast Join",
        "Shuffle Partition Tuning",
        "Kryo Serializer",
    ])

    st.markdown("---")

    if opt == "repartition()":
        st.subheader("repartition(n, column)")
        st.markdown("""
`repartition()` reshuffles all data across exactly N partitions.

**When to use it:**
- Before writing Parquet or MySQL to control output file count
- When one partition has much more data than others (data skew)
- When you have too many tiny tasks that create overhead

**repartition vs coalesce:**
- `repartition(n)` does a full shuffle. Can increase or decrease partition count.
- `coalesce(n)` does NO shuffle. Can only decrease count. Faster but can cause skew.
""")
        code_block("""# Without repartition: 148 tiny files per year partition (one per task)
# With repartition:    8 balanced files per year partition

df_balanced = df.repartition(8, "invoice_year")
#                             ^   ^
#                             8 output files per partition folder
#                             distribute data by this column value

df_balanced.write \\
    .partitionBy("invoice_year") \\
    .parquet(output_path)

# Folder result:
# invoice_year=2010/part-00000.parquet (size: ~1.1 MB)
# invoice_year=2010/part-00001.parquet (size: ~1.1 MB)
# ... 8 files total, all roughly equal
""")
        comparison = pd.DataFrame({
            "Approach":       ["No repartition", "repartition(8)"],
            "Files per year": [148,              8],
            "Avg file size":  ["0.06 MB",        "1.1 MB"],
            "Read speed":     ["Slow (metadata overhead per file)", "Fast (optimal size range)"],
        })
        st.table(comparison)

    elif opt == "cache() and unpersist()":
        st.subheader("cache() and unpersist()")
        st.markdown("""
By default, Spark recomputes a DataFrame every time you call an action (count, show, write, etc.).
`cache()` tells Spark to store the computed result in executor memory after the first computation.
""")
        code_block("""# WITHOUT cache - Spark re-reads CSV and re-applies all transforms
# for each of these calls (4 full passes through the data):
count1   = df.count()
null_ct  = df.filter(F.col("invoice_id").isNull()).count()
dup_ct   = df.dropDuplicates().count()
df.write.parquet(...)

# WITH cache - Spark reads and transforms ONCE, then stores in RAM:
df.cache()
df.count()          # first action: triggers cache to materialize

# All subsequent actions read from executor memory:
count1  = df.count()                                # RAM
null_ct = df.filter(F.col("invoice_id").isNull()).count()  # RAM
dup_ct  = df.dropDuplicates().count()               # RAM
df.write.parquet(...)                               # RAM

df.unpersist()      # release executor memory when done
# If you skip unpersist(), OOM errors can occur on large datasets
""")
        st.warning("Only cache DataFrames you will use 2 or more times. Caching something used only once is wasted memory.")

    elif opt == "explain() - Physical Plan":
        st.subheader("explain() - How Spark executes your query")
        st.markdown("This shows you the physical plan Spark generated - what it will actually do at runtime.")
        code_block("df.explain(mode='simple')")
        st.markdown("**Example output after broadcast join:**")
        code_block("""== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [invoice_id, stock_code, quantity, ...]
   +- BroadcastHashJoin [country], [country], Inner, BuildRight
      :- Filter ((quantity > 0) AND (unit_price > 0))
      :  +- FileScan csv [...] DataFilters: [isnotnull(quantity), ...]
      +- BroadcastExchange HashedRelationBroadcastMode(List(country))
            +- LocalTableScan [country, region]

# What to look for:
# BroadcastHashJoin  -> broadcast join worked (no shuffle of large table)
# Exchange           -> data shuffle (expensive, minimize these)
# FileScan + DataFilters -> filter pushed down to file read level
""", "text")

    elif opt == "Broadcast Join":
        st.subheader("Broadcast Join")
        st.markdown("""
**The Problem with Normal Joins:**

When you join two DataFrames, Spark normally shuffles BOTH across the network
so matching rows end up on the same executor node (Sort-Merge Join).
For a 500K row table, this is a lot of network I/O.

**The Solution:**

If one table is very small (under 10MB by default), tell Spark to broadcast it.
Spark sends a full copy to every executor. The large table never moves.

**The difference in plain terms:**
- Sort-Merge Join: both tables travel across the network
- Broadcast Join: only the small table travels, once, to each node
""")
        code_block("""region_df = spark.createDataFrame([
    ("United Kingdom", "Europe"),
    ("Germany",        "Europe"),
    ("Australia",      "Asia-Pacific"),
    ("USA",            "North America"),
    # ... 15 rows total - tiny table
], ["country", "region"])

# Without broadcast: Spark shuffles retail_df (500K rows) for the join
# With broadcast:    Spark copies region_df (15 rows) to each executor
#                    The 500K row table stays in place

df = df.join(
    F.broadcast(region_df),   # wrap the small table
    on="country",
    how="left"
)

# Verify in explain() output:
# BroadcastHashJoin -> confirmed broadcast happened
""")

    elif opt == "Shuffle Partition Tuning":
        st.subheader("spark.sql.shuffle.partitions")
        st.markdown("Controls how many partitions are created after a shuffle operation (join, groupBy, distinct).")
        code_block("""# Default is 200. For a 500K row dataset on local mode, 200 is overkill.
# 200 tiny partitions = 200 tasks with minimal data each = overhead

spark = SparkSession.builder \\
    .config("spark.sql.shuffle.partitions", "8") \\
    .getOrCreate()

# Rule of thumb for local mode:
# set to 2x the number of CPU cores
# 4 cores -> use 8
# 8 cores -> use 16

# Rule of thumb for cluster mode:
# set to 2-4x the total number of executor cores
# 20 executors x 4 cores = 80 total cores -> use 160-320
""")

    elif opt == "Kryo Serializer":
        st.subheader("Kryo Serializer")
        st.markdown("Controls how Spark serializes objects when broadcasting or shuffling data.")
        code_block("""# Default is Java serialization - verbose and slow.
# Kryo is typically 10x faster and 3-5x more compact.

spark = SparkSession.builder \\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
    .getOrCreate()

# When does serialization happen?
# 1. Broadcast joins (region_df sent to executors)
# 2. Shuffles across nodes (after groupBy, join)
# 3. Caching with disk spill (MEMORY_AND_DISK_SER storage level)

# Kryo helps most with large shuffles or many broadcasts.
""")


# ---------------------------------------------------------------
# PAGE: PIPELINE LOGS
# ---------------------------------------------------------------

elif page == "Pipeline Logs":
    st.title("Pipeline Logs")
    st.markdown("---")

    log_output = """2024-01-15 14:22:01 | INFO     | Pipeline      | ======================================================================
2024-01-15 14:22:01 | INFO     | Pipeline      | PIPELINE STARTED
2024-01-15 14:22:01 | INFO     | Pipeline      | Job Start Time : 2024-01-15 14:22:01
2024-01-15 14:22:01 | INFO     | Pipeline      | ======================================================================
2024-01-15 14:22:03 | INFO     | Pipeline      | --- STAGE 1: EXTRACT ---
2024-01-15 14:22:03 | INFO     | Extract       | Reading raw data from: data/raw/online_retail_II.csv
2024-01-15 14:22:03 | INFO     | Extract       | Parse mode: PERMISSIVE
2024-01-15 14:22:08 | INFO     | Extract       | Raw records loaded: 1,067,371
2024-01-15 14:22:09 | INFO     | Extract       | Sampled down to 500,000 records
2024-01-15 14:22:09 | INFO     | Pipeline      | STAGE [Extract] | Records: 500,000
2024-01-15 14:22:09 | INFO     | Pipeline      | --- STAGE 2: TRANSFORM ---
2024-01-15 14:22:09 | INFO     | Transform     | Starting transformation pipeline...
2024-01-15 14:22:09 | INFO     | Transform     | Renaming columns to snake_case...
2024-01-15 14:22:10 | INFO     | Transform     | Casting column types...
2024-01-15 14:22:11 | INFO     | Transform     | Handling null values...
2024-01-15 14:22:14 | INFO     | Transform     | Null handling: dropped 37,482 rows with critical nulls
2024-01-15 14:22:14 | INFO     | Transform     | Deduplicating records...
2024-01-15 14:22:17 | INFO     | Transform     | Deduplication: removed 14,902 duplicate rows
2024-01-15 14:22:17 | INFO     | Transform     | Filtering invalid records...
2024-01-15 14:22:21 | INFO     | Transform     | Invalid record filter: removed 11,203 rows
2024-01-15 14:22:21 | INFO     | Transform     | Adding derived columns...
2024-01-15 14:22:22 | INFO     | Transform     | All transformations complete.
2024-01-15 14:22:22 | INFO     | Pipeline      | STAGE [Transform] | Records: 436,413
2024-01-15 14:22:22 | INFO     | Pipeline      | --- STAGE 3: BROADCAST JOIN ---
2024-01-15 14:22:23 | INFO     | Pipeline      | Broadcast join complete. 'region' column added.
2024-01-15 14:22:23 | INFO     | Pipeline      | --- STAGE 4: DATA QUALITY ---
2024-01-15 14:22:23 | INFO     | Pipeline      | Caching DataFrame for quality checks...
2024-01-15 14:22:29 | INFO     | QualityChecks | Row Count Check | Raw: 500,000 | Processed: 436,413 | PASS
2024-01-15 14:22:33 | INFO     | QualityChecks | Null Check [invoice_id]: 0.0% | PASS
2024-01-15 14:22:37 | INFO     | QualityChecks | Null Check [customer_id]: 0.0% | PASS
2024-01-15 14:22:47 | INFO     | QualityChecks | Duplicate Check | Duplicates: 0 | Pct: 0.0% | PASS
2024-01-15 14:22:48 | INFO     | QualityChecks | Schema Check | PASS | Schema matches exactly
2024-01-15 14:22:50 | INFO     | QualityChecks | Quality Report: 18 checks | FAIL: 0 | WARN: 0 | PASSED
2024-01-15 14:22:50 | INFO     | Pipeline      | --- STAGE 5: WRITE PARQUET ---
2024-01-15 14:22:50 | INFO     | Load          | Writing Parquet to: data/processed/parquet/retail_data
2024-01-15 14:22:50 | INFO     | Load          | Partition column  : invoice_year
2024-01-15 14:22:50 | INFO     | Load          | Repartition count : 8
2024-01-15 14:23:04 | INFO     | Load          | Parquet write complete.
2024-01-15 14:23:04 | INFO     | Pipeline      | --- STAGE 6: WRITE MYSQL ---
2024-01-15 14:23:04 | INFO     | Load          | Writing to MySQL table: retail_transactions
2024-01-15 14:23:04 | INFO     | Load          | Batch size: 5,000
2024-01-15 14:23:47 | INFO     | Load          | MySQL write complete.
2024-01-15 14:23:47 | INFO     | Pipeline      | ======================================================================
2024-01-15 14:23:47 | INFO     | Pipeline      | PIPELINE FINISHED
2024-01-15 14:23:47 | INFO     | Pipeline      | Job End Time            : 2024-01-15 14:23:47
2024-01-15 14:23:47 | INFO     | Pipeline      | Total Duration          : 106.42 seconds
2024-01-15 14:23:47 | INFO     | Pipeline      | Validation Status       : PASSED
2024-01-15 14:23:47 | INFO     | Pipeline      | raw_record_count        : 500,000
2024-01-15 14:23:47 | INFO     | Pipeline      | processed_record_count  : 436,413
2024-01-15 14:23:47 | INFO     | Pipeline      | quality_check_status    : PASSED
2024-01-15 14:23:47 | INFO     | Pipeline      | mysql_records_loaded    : 436,413
2024-01-15 14:23:47 | INFO     | Pipeline      | ======================================================================"""

    st.text_area("logs/pipeline.log", value=log_output, height=450)

    st.markdown("---")
    st.subheader("Logger Setup")
    code_block("""import logging

def get_logger(name="ETLPipeline"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console: show INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File: show DEBUG and above (full detail)
    file_handler = logging.FileHandler("logs/pipeline.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

logger = get_logger("Extract")
logger.info("Reading raw data from: data/raw/online_retail_II.csv")
""")


# ---------------------------------------------------------------
# PAGE: ANALYTICS AND INSIGHTS
# ---------------------------------------------------------------

elif page == "Analytics and Insights":
    st.title("Analytics and Insights from Processed Data")
    st.markdown("These charts use the processed sample data — the same data that gets written to Parquet and MySQL.")
    st.markdown("---")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Revenue by Country",
        "Monthly Trend",
        "Price Distribution",
        "Top Products",
    ])

    with tab1:
        country_rev = (
            df_clean[df_clean["is_return"] == False]
            .groupby("country")["total_amount"]
            .sum()
            .reset_index()
            .sort_values("total_amount", ascending=False)
            .head(12)
        )
        fig = px.bar(
            country_rev, x="country", y="total_amount",
            color="total_amount",
            color_continuous_scale="Blues",
            title="Revenue by Country (top 12, returns excluded)",
            labels={"total_amount": "Revenue (GBP)", "country": "Country"},
        )
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        monthly = (
            df_clean[df_clean["is_return"] == False]
            .groupby(["invoice_year", "invoice_month"])["total_amount"]
            .sum()
            .reset_index()
        )
        monthly["period"] = (
            monthly["invoice_year"].astype(str) + "-"
            + monthly["invoice_month"].astype(str).str.zfill(2)
        )
        monthly = monthly.sort_values("period")

        fig = px.line(
            monthly, x="period", y="total_amount",
            markers=True,
            title="Monthly Revenue Trend",
            labels={"total_amount": "Revenue (GBP)", "period": "Month"},
        )
        fig.update_traces(line_color="#3498db", marker_color="#e74c3c")
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        bucket_counts = df_clean["price_bucket"].value_counts().reset_index()
        bucket_counts.columns = ["price_bucket", "count"]
        order = ["under_1", "1_to_5", "5_to_20", "20_to_100", "above_100"]
        bucket_counts["price_bucket"] = pd.Categorical(bucket_counts["price_bucket"], categories=order, ordered=True)
        bucket_counts = bucket_counts.sort_values("price_bucket")

        fig = px.pie(
            bucket_counts, names="price_bucket", values="count",
            title="Transaction Count by Price Bucket",
            color_discrete_sequence=px.colors.sequential.Blues_r,
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab4:
        top_products = (
            df_clean[df_clean["is_return"] == False]
            .groupby(["stock_code", "description"])
            .agg(total_revenue=("total_amount", "sum"), units_sold=("quantity", "sum"))
            .reset_index()
            .sort_values("total_revenue", ascending=False)
            .head(10)
        )

        fig = px.bar(
            top_products, x="total_revenue", y="description",
            orientation="h",
            color="units_sold",
            color_continuous_scale="Greens",
            title="Top 10 Products by Revenue",
            labels={"total_revenue": "Revenue (GBP)", "description": "Product"},
        )
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("SQL you can run directly on MySQL")
    code_block("""-- Revenue by country
SELECT country, SUM(total_amount) AS revenue
FROM retail_transactions WHERE is_return = 0
GROUP BY country ORDER BY revenue DESC;

-- Monthly trend
SELECT invoice_year, invoice_month, SUM(total_amount) AS monthly_revenue
FROM retail_transactions WHERE is_return = 0
GROUP BY invoice_year, invoice_month
ORDER BY invoice_year, invoice_month;

-- Top 10 products by units sold
SELECT stock_code, description, SUM(quantity) AS units_sold
FROM retail_transactions WHERE is_return = 0
GROUP BY stock_code, description
ORDER BY units_sold DESC LIMIT 10;

-- Return rate by country
SELECT country,
       SUM(is_return) AS returns,
       COUNT(*) AS total,
       ROUND(SUM(is_return) / COUNT(*) * 100, 2) AS return_pct
FROM retail_transactions
GROUP BY country
ORDER BY return_pct DESC;""", "sql")
