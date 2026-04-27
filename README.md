# PySpark + Apache Iceberg Medallion Architecture (Retail Analytics)

This project demonstrates a **local end-to-end data engineering pipeline** using **PySpark + Apache Iceberg + SQL Server** following the **Medallion Architecture pattern (Bronze → Silver → Gold)**.

The goal of this project is to simulate a modern data platform locally while implementing real-world ingestion patterns such as:

* Full snapshot ingestion
* Incremental ingestion using watermark columns
* SQL Server CDC ingestion using LSN tracking
* SCD Type 2 logic in Silver layer
* Dimensional modeling in Gold layer
* Iceberg table management for analytical workloads

This project uses the **AdventureWorks SalesLT database** as the source system.

---

## Architecture Overview

```text
SQL Server (AdventureWorks)
        |
        v
Bronze Layer (Raw ingestion)
- Full Snapshot
- Incremental Watermark
- CDC ingestion
        |
        v
Silver Layer (Business transformations)
- Deduplication
- Merge logic
- SCD Type 2 historical tracking
        |
        v
Gold Layer (Analytics-ready)
- Dimension tables
- Fact tables
- Star schema modeling
```

### Technologies Used

* Python 3.13
* PySpark
* Apache Iceberg
* SQL Server
* JDBC
* UV (Python package manager)
* Local object storage / warehouse

---

# Project Structure

```bash
pyspark-iceberg-medallion-retail/
│
├── src/
│   ├── config/
│   │   ├── settings.py
│   │   ├── spark_config.py
│   │   └── tables_config.py
│   │
│   ├── jobs/
│   │   ├── run_bronze.py
│   │   ├── run_silver.py
│   │   ├── run_gold.py
│   │   ├── init_cdc_state.py
│   │   └── test_sqlserver_connection.py
│   │
│   ├── pipelines/
│   │   ├── bronze/
│   │   │   └── generic_bronze.py
│   │   │
│   │   ├── silver/
│   │   │   └── generic_silver.py
│   │   │
│   │   └── gold/
│   │       ├── generic_gold.py
│   │       └── sql/
│   │           ├── dim_customer.sql
│   │           ├── dim_product.sql
│   │           └── fact_sales.sql
│   │
│   └── utils/
│       └── watermark.py
│
├── state/
│   ├── saleslt_customer_bronze_watermark.json
│   ├── saleslt_product_bronze_watermark.json
│   ├── saleslt_salesorderheader_bronze_watermark.json
│   └── saleslt_salesorderdetail_cdc_bronze_lsn.json
│
├── pyproject.toml
└── README.md
```

---

# Bronze Layer

The Bronze layer is responsible for raw ingestion from SQL Server.

## Supported ingestion strategies

### 1. Full Snapshot

Used when source tables do not have incremental fields.

Example:

* Customer Address
* Reference tables

---

### 2. Incremental Watermark

Used when source tables contain a `ModifiedDate` column.

Example:

* Product
* Customer
* SalesOrderHeader

The pipeline stores the latest processed watermark inside:

```bash
/state/
```

Example:

```json
{
  "last_watermark": "2026-04-20 10:00:00"
}
```

---

### 3. SQL Server CDC

Used for transactional tables with frequent updates.

Example:

* SalesOrderDetail

The project tracks the latest processed:

* LSN
* CDC state

This simulates enterprise-grade incremental ingestion.

---

# Silver Layer

The Silver layer applies business transformations.

Responsibilities:

* Deduplication
* Merge logic
* Data cleaning
* Schema standardization
* Historical tracking

---

## SCD Type 2 Implementation

The Silver layer maintains historical versions using:

* `is_current`
* `valid_from`
* `valid_to`

This allows full historical tracking of dimension changes.

Example:

| ProductID | Name   | Price | is_current |
| --------- | ------ | ----- | ---------- |
| 1         | Bike A | 100   | 0          |
| 1         | Bike A | 120   | 1          |

---

# Gold Layer

The Gold layer creates analytics-ready datasets.

Current outputs:

### Dimension Tables

* `dim_customer`
* `dim_product`

### Fact Tables

* `fact_sales`

These tables follow a **star schema design** for BI/reporting consumption.

---

# How to Run

## 1. Install dependencies

```bash
uv sync
```

---

## 2. Configure environment variables

Create a `.env` file:

```bash
SQL_SERVER_HOST=
SQL_SERVER_PORT=
SQL_SERVER_DATABASE=
SQL_SERVER_USER=
SQL_SERVER_PASSWORD=
```

---

## 3. Run Bronze ingestion

```bash
python src/jobs/run_bronze.py saleslt_product
```

Example CDC ingestion:

```bash
python src/jobs/run_bronze.py saleslt_salesorderdetail_cdc
```

---

## 4. Run Silver layer

```bash
python src/jobs/run_silver.py saleslt_product
```

---

## 5. Run Gold layer

```bash
python src/jobs/run_gold.py
```

---

# Example Business Scenario

This project simulates a retail company that needs:

* Product history tracking
* Customer analytics
* Sales fact reporting
* Incremental ingestion optimization
* CDC for transactional tables

This mirrors real-world modern data platform challenges.

---

# Future Improvements (Version 2)

Potential enhancements:

* Apache Airflow orchestration
* dbt integration
* Data quality checks (Great Expectations)
* CI/CD pipeline
* Cloud deployment (AWS/Azure/GCP)
* Streaming ingestion with Kafka
* Unit tests

---

# Why This Project Matters

This project demonstrates practical experience with:

✅ PySpark
✅ Apache Iceberg
✅ Incremental pipelines
✅ CDC ingestion
✅ SCD Type 2
✅ Dimensional modeling
✅ Data lakehouse architecture

These are highly relevant skills for:

* Data Engineer roles
* Analytics Engineer roles
* Modern Lakehouse platforms
* Databricks environments

---

# Author

Built as a portfolio project to demonstrate modern data engineering practices using local infrastructure.
