# Production-Grade ETL Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-ffA500?style=for-the-badge&logo=SQL&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-2C3E50?style=for-the-badge&logo=prefect&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

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

etl-pipeline/ — Core ETL logic (PySpark processing, data quality checks, and loading)

orchestration/ — Prefect flows and orchestration

dbt/ — dbt models, snapshots, macros, and tests

.github/workflows/ — CI/CD pipelines (GitHub Actions)

samples/ — Sample input data

Screenshot/ — Pipeline execution examples and outputs

## License

MIT License - see LICENSE file for details.
