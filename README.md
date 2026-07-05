# Production-Grade ETL Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/SQL-ffA500?logo=sql&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white" />
  <img src="https://img.shields.io/badge/Prefect-2C3E50?logo=prefect&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/BigQuery-4285F4?logo=googlecloud&logoColor=white" />
  <img src="https://img.shields.io/badge/GitHub_Actions-2088FF?logo=github-actions&logoColor=white" />
</p>

<p align="center">
  <strong>End-to-End • Production Ready • Data Quality First</strong>
</p>


<br>


A modern, scalable batch ETL pipeline that processes 500K+ orders with 97.5% data quality, automated quarantine logic, dbt transformations, and dual cloud warehousing.

Built with best practices: observability, CI/CD with manual approval, Docker, and production-ready deployment.


## Architecture

```mermaid
flowchart LR
    A[CSV Files] --> B[Spark Processing]
    B --> C[Data Quality Engine]
    C --> D{Quality > 80%?}
    D -->|Yes| E[PostgreSQL Staging]
    D -->|No| F[Quarantine Zone]
    E --> G[dbt Transformations]
    G --> H[BigQuery Analytics]
```
## Pipeline In Action

# Data Quality Engine
<img width="685" height="629" alt="image" src="https://github.com/user-attachments/assets/4cf42804-bd06-46c4-9bb9-2159e55ced48" alt= "Data Quality Report"/>

# BiQuery Load
<img width="1482" height="368" alt="Screenshot 2026-07-03 051832" src="https://github.com/user-attachments/assets/c249d01d-5ae7-43b7-a408-be56d64f09c6" alt="BigQuery Table Info" />

# BigQuery In Console
<img width="1645" height="286" alt="Screenshot 2026-04-22 211924" src="https://github.com/user-attachments/assets/ec26c181-8091-459c-9e4b-c9123bfde370" alt="BigQuery In Console Logs" />




---
## Key Features

- High-Performance Processing using PySpark that efficiently handles 500K+ rows with duplicate removal and schema inference
- Advanced Data Quality Engine with automated scoring across 11 columns and intelligent quarantine system
- Modern Transformations using dbt with incremental models, SCD Type 2 snapshots, custom macros, and 20+ tests
- Dual Warehousing setup with PostgreSQL for staging and BigQuery for analytics (with proper partitioning and clustering)
- Full Containerization using Docker with multi-stage builds and health checks
- Production-grade CI/CD using GitHub Actions with security scanning, manual approval gates, and deployment tagging
- Robust Orchestration using Prefect Cloud (migrated from Airflow for better developer experience)

## Data Quality Highlights

- Average Quality Score: 97.5%
- Total Rows Processed: 500,000+
- Quarantine Logic: Automatically isolates rows below 80% quality threshold
- Invalid Data Detected: ~2.5% of records (auto-quarantined)
- Columns Validated: 11 critical columns

## Tech Stack

- Processing: PySpark
- Orchestration: Prefect Cloud
- Transformations: dbt (models, snapshots, macros, tests)
- Warehousing: BigQuery + PostgreSQL
- Containerization: Docker + Docker Compose
- CI/CD: GitHub Actions
- Data Quality: Custom scoring engine + quarantine logic
- Monitoring: Logging + Prefect Cloud

## What I Learned

- Data quality must be enforced before loading, not after — bad data can poison downstream analytics
- Modern orchestration tools like Prefect offer significantly better developer experience than traditional tools for mid-size pipelines
- Proper CI/CD with manual approval gates and deployment tagging dramatically increases confidence in production deployments
- Cloud warehousing best practices (partitioning + clustering) have a massive impact on both cost and query performance

## Quick Start

git clone https://github.com/AM7013/production-ETL-pipeline.git

cd production-ETL-pipeline

cp .env.example .env

docker-compose up --build

## Structure
``` text
production-ETL-pipeline/
├── .github/
│   └── workflows/
│       ├── prefect-ci.yml
│       └── prefect-cd.yml
├── Screenshot/                  # Screenshots for README
├── dbt/                         # dbt project
│   ├── macros/
│   ├── models/
│   ├── snapshots/
│   ├── tests/
│   └── ... (other dbt folders)
├── etl-pipeline/                # Core ETL logic
├── orchestration/               # Prefect flows
│   └── Prefect/
│       └── etl_flow.py
├── samples/                     # Sample data
│   └── cleaned_data_test.csv
├── sandbox/                     # Experimental code
├── .dockerignore
├── .env.example
├── .gitignore
├── .prefectignore
├── .trivyignore
├── Dockerfile
├── LICENSE
├── README.md
├── requirements.txt
├── run_cd.py
└── (other root files)
```
## License

MIT License - see LICENSE file for details.
