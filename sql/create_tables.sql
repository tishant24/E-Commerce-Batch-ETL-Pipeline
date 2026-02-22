-- ============================================================
-- E-Commerce ETL Pipeline - MySQL Table DDL
-- Dataset : Online Retail II (UCI ML Repository)
-- MySQL   : 8.0+
-- ============================================================
-- Run this before executing the pipeline:
--   mysql -u root -p < sql/create_tables.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ecommerce_db;

-- ============================================================
-- Main Fact Table: retail_transactions
-- ============================================================
-- Primary Key (id): auto-increment surrogate key
--   We use a surrogate PK instead of (invoice_id + stock_code) because:
--   1. MySQL clusters rows by PK — a single BIGINT is faster than composite
--   2. JDBC writes from Spark work better with a simple PK
--   3. The business key (invoice_id + stock_code) is enforced via UNIQUE index
--
-- Index Design Explained:
--   idx_invoice_id   -> "show all items in invoice C12345"
--   idx_customer_id  -> "show all purchases by customer 17850"
--   idx_country_year -> "revenue by country for 2010" (composite = avoids index merge)
--   idx_invoice_date -> date range scans (most common analytics pattern)
--   idx_price_bucket -> "group by price tier" queries
-- ============================================================

DROP TABLE IF EXISTS retail_transactions;

CREATE TABLE retail_transactions (
    id              BIGINT          NOT NULL AUTO_INCREMENT,

    -- Business identifiers
    invoice_id      VARCHAR(20)     NOT NULL,
    stock_code      VARCHAR(20)     NOT NULL,
    description     VARCHAR(500)    DEFAULT NULL,

    -- Quantities and pricing
    quantity        INT             NOT NULL,
    unit_price      DECIMAL(10, 2)  NOT NULL,
    total_amount    DECIMAL(12, 2)  NOT NULL,

    -- Time dimensions
    invoice_date    DATETIME        NOT NULL,
    invoice_year    SMALLINT        NOT NULL,
    invoice_month   TINYINT         NOT NULL,
    invoice_day     TINYINT         NOT NULL,

    -- Customer and geography
    customer_id     VARCHAR(20)     DEFAULT 'GUEST',
    country         VARCHAR(100)    NOT NULL,

    -- Derived flags
    is_return       TINYINT(1)      DEFAULT 0,
    price_bucket    VARCHAR(20)     DEFAULT NULL,

    -- Audit
    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),

    UNIQUE  INDEX uq_invoice_stock   (invoice_id, stock_code),
    INDEX         idx_invoice_id     (invoice_id),
    INDEX         idx_customer_id    (customer_id),
    INDEX         idx_country_year   (country, invoice_year),
    INDEX         idx_invoice_date   (invoice_date),
    INDEX         idx_invoice_year   (invoice_year),
    INDEX         idx_price_bucket   (price_bucket)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  ROW_FORMAT=COMPRESSED
  COMMENT='Curated retail transactions — loaded by PySpark ETL v1.0';


-- ============================================================
-- Data Quality Report Table
-- One row per check per pipeline run
-- ============================================================

DROP TABLE IF EXISTS dq_report;

CREATE TABLE dq_report (
    id              INT             NOT NULL AUTO_INCREMENT,
    run_timestamp   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    check_name      VARCHAR(100)    NOT NULL,
    metric_value    DOUBLE          DEFAULT NULL,
    threshold       DOUBLE          DEFAULT NULL,
    status          VARCHAR(10)     NOT NULL COMMENT 'PASS | WARN | FAIL',
    detail          VARCHAR(500)    DEFAULT NULL,

    PRIMARY KEY (id),
    INDEX idx_run_ts (run_timestamp),
    INDEX idx_status (status)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Data quality check results per pipeline run';


-- ============================================================
-- Quick verification queries after loading
-- ============================================================

-- Total record count
-- SELECT COUNT(*) AS total_rows FROM retail_transactions;

-- Revenue by country (top 10)
-- SELECT country, SUM(total_amount) AS revenue
-- FROM retail_transactions
-- GROUP BY country ORDER BY revenue DESC LIMIT 10;

-- Monthly revenue trend
-- SELECT invoice_year, invoice_month, SUM(total_amount) AS monthly_revenue
-- FROM retail_transactions
-- WHERE is_return = 0
-- GROUP BY invoice_year, invoice_month
-- ORDER BY invoice_year, invoice_month;

-- Top 10 products by quantity sold
-- SELECT stock_code, description, SUM(quantity) AS total_sold
-- FROM retail_transactions
-- GROUP BY stock_code, description
-- ORDER BY total_sold DESC LIMIT 10;
