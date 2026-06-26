# 🚀 Metadata-Driven ETL Pipeline using Apache Airflow & PostgreSQL

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![License](https://img.shields.io/badge/License-MIT-green)

A complete **production-grade ETL pipeline** using **Apache Airflow**, **PostgreSQL**, and **SCD Type 2** implementation.

This project demonstrates advanced Data Engineering skills in building scalable, maintainable, and reliable data pipelines.

---

## 🎯 Project Overview

This system automates the daily ingestion and historization of banking transaction data.


## 🏛️ Data Architecture (Medallion Architecture)

This project follows the **Medallion Architecture** (Bronze → Silver → Gold):

- **Bronze Layer**: Raw source data (Parquet files)
- **Silver Layer**: Cleaned, validated, and historized data (SCD Type 2 dimensions)
- **Gold Layer**: Aggregated, business-ready, and consumption-ready tables optimized for analytics, reporting, and alerting

This layered approach ensures **data quality**, **traceability**, and **high performance** for downstream consumers (BI tools, ML models, investigators).

### Key Features

- Daily source data generation with realistic patterns
- Parameterized and reusable Airflow DAGs
- Robust SCD Type 2 implementation using Stored Procedures
- Hash-based change detection (`hash_pk` + `hash_diff`)
- Dynamic multi-table processing
- Proper error handling and logging
- Production-ready folder structure

---

## 🛠️ Tech Stack

- **Orchestration**: Apache Airflow (TaskFlow API)
- **Database**: PostgreSQL 15+
- **Language**: Python 3.11+
- **Data Format**: Parquet
- **Connection**: psycopg2 + PostgresHook

---

## 📁 Project Structure

```bash
Airflow_ETL_Project/
├── dags/
│   ├── dag1.py
│   ├── dag2.py
│   ├── dag3.py
│   └── dag4.py
├── source_queries/
│   ├── schema_creation.txt
│   ├── sp_creation_txt
│   └── table_creation.txt
├── data/
│   └── tm_source_data/
│       └── <date>/
├── config/
├── logs/
├── docker-compose.yml
├── .env
└── README.md


Core Components

1. Daily Source Data Generation
- Generates realistic customer, account, and transaction data
- Supports date-specific runs

2. SCD Type 2 Implementation
Stored Procedure: scd2_upsert()
Features:
- hash_pk → Unique record identification
- hash_diff → Change detection on attributes
- Automatic expiration of previous versions (rec_end, is_active)
- Idempotent design (safe to rerun)

3. Airflow DAGs



Key Airflow Features Used:
- Dynamic Task Mapping
- Parameter passing between DAGs
- TriggerDagRunOperator
- Runtime configuration via params
- Proper task dependencies and branching

How to Run

1. Setup Airflow

```bash
# Using official docker-compose
docker-compose up -d
```

2. Create Postgres Connection
Go to Airflow UI → Admin → Connections → Create new connection:
- Conn ID: postgres_default
- Conn Type: Postgres
- Host: postgres (or your host)
- Schema: public / staging

3. Deploy DAGs
Copy DAG files to /opt/airflow/dags/ and scripts to appropriate folders. 



SCD2 Stored Procedure

Signature

```sql
CALL scd2_upsert(
    p_run_date       := DATE,
    p_staging_table  := TEXT,
    p_target_table   := TEXT,
    p_pk_columns     := TEXT[],
    p_non_pk_columns := TEXT[]
);
```

Example Call
```sql
CALL scd2_upsert(
    CURRENT_DATE,
    'staging.stg_customer',
    'dim_customer',
    ARRAY['customer_id'],
    ARRAY['full_name', 'date_of_birth', 'nationality', 'risk_score']
);
```

Key Learnings & Skills Demonstrated

Airflow Expertise
- TaskFlow API (@dag, @task)
- Dynamic Task Mapping (.partial().expand())
- Cross-DAG triggering with parameters
- Context handling and XCom usage
- Runtime parameterization
- Proper error handling and retries

Data Engineering Best Practices
- SCD Type 2 with hash-based change detection
- Idempotent pipelines
- Separation of concerns (scripts vs DAGs)
- Comprehensive logging using RAISE NOTICE
- Schema management and dynamic SQL
- Production-ready folder structure

PostgreSQL Advanced Concepts
- Stored Procedures with dynamic SQL
- Proper escaping and format()
- Array parameters
- Performance optimization using hashes

Future Enhancements (Roadmap)
Data Quality checks using Great Expectations
Incremental loading with watermark
DBT integration for transformations
Backfill capability

Skills Showcase
This project demonstrates ability to:
- Design and implement enterprise-grade ETL pipelines
- Master Apache Airflow orchestration patterns
- Implement SCD Type 2 patterns correctly
- Write clean, maintainable, and production-ready code
- Integrate Python and PostgreSQL effectively
- Build self-documenting and reusable frameworks


License
MIT License

