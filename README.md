# PySpark + Apache Iceberg Medallion Architecture (Retail Analytics)

This project demonstrates a **local end-to-end data engineering platform** using **PySpark, Apache Iceberg, SQL Server CDC, MinIO, Docker, and Apache Airflow** following the **Medallion Architecture pattern (Bronze → Silver → Gold)**.

The goal is to simulate how modern companies build data platforms for analytical workloads while handling:

* Full snapshot ingestion
* Incremental ingestion using watermarks
* SQL Server CDC ingestion using LSN tracking
* SCD Type 2 historical tracking
* Dimensional modeling
* Workflow orchestration with Apache Airflow
* Local lakehouse infrastructure using Docker

This project uses the **AdventureWorks SalesLT database** as the transactional source system.

---

# Architecture Overview

```text
                         ┌──────────────────────────┐
                         │ SQL Server Source System │
                         │ AdventureWorks SalesLT   │
                         └────────────┬─────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          │                           │                           │
          │                           │                           │
 Full Snapshot Load         Incremental Watermark Load       CDC Load
          │                           │                           │
          └───────────────┬───────────┴───────────┬──────────────┘
                          │                       │
                          v                       v

                Bronze Layer (Apache Iceberg)
        - Raw ingestion
        - Snapshot ingestion
        - Incremental ingestion
        - CDC ingestion via SQL Server LSN

                          |
                          v

                Silver Layer
        - Merge logic
        - Deduplication
        - Schema standardization
        - SCD Type 2 historical tracking

                          |
                          v

                Gold Layer
        - Fact tables
        - Dimension tables
        - Star schema modeling

                          |
                          v

              Apache Airflow Orchestration
        - Initial Load DAG
        - Incremental DAG
```

---

# Tech Stack

* Python 3.13
* PySpark
* Apache Iceberg
* SQL Server
* SQL Server CDC
* Apache Airflow
* Docker
* MinIO
* Iceberg REST Catalog
* JDBC
* UV Package Manager

---

# Project Structure

```bash
pyspark-iceberg-medallion-retail/
│
├── airflow/
│   ├── dags/
│   │   ├── medallion_retail_pipeline.py
│   │   └── init_salesorderdetail_snapshot.py
│   │
│   ├── logs/
│   ├── plugins/
│   └── Dockerfile
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
│   │   ├── silver/
│   │   └── gold/
│   │
│   └── utils/
│       └── watermark.py
│
├── state/
│   ├── bronze watermarks
│   └── cdc lsn states
│
├── docker-compose.yaml
├── pyproject.toml
└── README.md
```

---

# Bronze Layer

The Bronze layer is responsible for raw ingestion from SQL Server.

## 1. Full Snapshot

Used when source tables do not contain incremental tracking fields.

Example:

* Reference tables
* Initial SalesOrderDetail historical load

---

## 2. Incremental Watermark

Used when source tables contain `ModifiedDate`.

Examples:

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

## 3. SQL Server CDC

Used for highly transactional tables.

Example:

* SalesOrderDetail

The pipeline tracks:

* LSN
* CDC state

This simulates real enterprise incremental ingestion.

---

# Silver Layer

The Silver layer applies business transformations.

Responsibilities:

* Merge logic
* Deduplication
* Schema standardization
* Data cleaning
* Historical tracking

---

## SCD Type 2 Implementation

The Silver layer maintains historical versions using:

* `is_current`
* `valid_from`
* `valid_to`

Example:

| ProductID | Name   | Price | is_current |
| --------- | ------ | ----- | ---------- |
| 1         | Bike A | 100   | 0          |
| 1         | Bike A | 120   | 1          |

---

# Gold Layer

The Gold layer creates analytics-ready datasets.

## Dimension Tables

* `dim_customer`
* `dim_product`

## Fact Tables

* `fact_sales`

These tables follow a **star schema design** for BI/reporting consumption.

---

# Apache Airflow Orchestration

This project uses Apache Airflow to orchestrate the platform.

## Initial Load DAG

DAG: `init_salesorderdetail_snapshot`

This DAG runs only once to bootstrap historical data:

1. Bronze full snapshot ingestion
2. Silver transformation
3. `init_cdc_state.py`

This creates the baseline before incremental CDC starts.

---

## Incremental DAG

DAG: `medallion_retail_pipeline`

This DAG handles recurring executions:

* Bronze incremental ingestion
* Bronze CDC ingestion
* Silver transformations
* Gold refresh

For `SalesOrderDetail`, only incremental CDC changes are processed after the initial load.

---

# Infrastructure

The platform runs locally using Docker.

Services:

- Apache Spark
- Apache Airflow
- MinIO
- Iceberg REST Catalog
- Microsoft SQL Server

Run infrastructure:

```bash
docker compose up -d

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

## 3. Run initial load DAG

Access Airflow:

```text
http://localhost:8081
```

Run:

* `init_salesorderdetail_snapshot`

---

## 4. Run incremental DAG

Run:

* `medallion_retail_pipeline`

---

# Business Scenario

This project simulates a retail company that needs:

* Product history tracking
* Customer analytics
* Sales reporting
* Incremental ingestion optimization
* CDC ingestion for transactional tables

This mirrors real-world modern data platform challenges.

---

# Future Improvements

* dbt integration
* Data quality validation (Great Expectations)
* CI/CD pipeline
* Cloud deployment (AWS/Azure/GCP)
* Streaming ingestion with Kafka
* Unit testing
* Monitoring/alerting

---

# Why This Project Matters

This project demonstrates practical experience with:

✅ PySpark
✅ Apache Iceberg
✅ Apache Airflow
✅ SQL Server CDC
✅ Incremental pipelines
✅ SCD Type 2
✅ Dimensional modeling
✅ Docker infrastructure
✅ Lakehouse architecture

These are highly relevant skills for:

* Data Engineer roles
* Analytics Engineer roles
* Modern lakehouse environments


---

# Author

Built as a portfolio project to demonstrate real-world data engineering architecture patterns using local infrastructure.
