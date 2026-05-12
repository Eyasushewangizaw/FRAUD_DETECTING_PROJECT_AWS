<!--
  Professional README for an AWS Fraud Detection Pipeline.
  Sections are intentionally structured to be compelling for data engineering job applications.
-->

# Fraud Detection Pipeline (AWS)

![Python 3.10](https://img.shields.io/badge/Python-3.10-3776AB?style=flat-square&logo=python)
![Terraform](https://img.shields.io/badge/Terraform-IaC-844FBA?style=flat-square&logo=terraform)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-FE542B?style=flat-square&logo=apache-airflow)
![AWS](https://img.shields.io/badge/AWS-Cloud-FF9900?style=flat-square&logo=amazon-aws)

## One-line description
End-to-end fraud analytics pipeline that streams events through Kinesis into an S3 lake, curates with Glue, loads into Redshift, and visualizes with QuickSight — orchestrated by Airflow (MWAA) and governed by automated data-quality gates and monitoring.

## Architecture

![alt text](aws-fraud-pipeline-architecture.png)

## Problem statement
Fraud patterns evolve quickly, and the ability to detect suspicious activity in near real time can materially reduce financial loss and improve customer trust. However, real-time analytics requires more than model inference: it depends on reliable event ingestion, repeatable ETL, governed storage, and operational monitoring to prevent data drift and pipeline failures from silently impacting decisions.

This project demonstrates a production-style AWS architecture for building that end-to-end capability using streaming ingestion, lakehouse principles, a governed warehouse layer, and dashboard-ready curated outputs.

## Tech stack

| Service | Purpose |
|---|---|
| Amazon S3 | Data lake (raw landing + curated Parquet) |
| Amazon Kinesis Data Streams | Low-latency event ingestion buffer |
| Amazon Kinesis Data Firehose | Deliver stream data into S3 with buffering + error output |
| AWS Lambda | S3 event trigger to start Glue ETL runs |
| AWS Glue | PySpark ETL (features + heuristic fraud scoring) + Data Catalog updates |
| AWS Glue Crawlers | Register/refresh curated table partitions |
| Amazon Redshift | Curated warehouse for analytics + governed mart tables |
| Amazon Athena | Ad-hoc query layer over curated data (optional) |
| Amazon QuickSight | BI dashboards for fraud KPIs |
| Amazon MWAA / Apache Airflow | Hourly orchestration and quality gates |
| Amazon CloudWatch | Metrics + alarms + operational observability |
| AWS SNS | Alert fan-out to email subscribers |
| AWS IAM | Least-privilege access control for all services |
| AWS STS (via boto3) | Credential identity breadcrumb for validation tooling |

## Pipeline flow

1. **Producer** emits synthetic transaction events.
2. **Kinesis** buffers events durably for consumers.
3. **S3** receives raw objects (via Firehose delivery) into `raw/transactions/` with partition-friendly prefixes.
4. **Lambda** listens to new raw objects and starts the **Glue** ETL job.
5. **Glue** reads raw JSON, performs data validation, feature engineering, and applies rule-based fraud scoring. It writes curated Parquet partitioned by `year/month/day`.
6. **Redshift** loads curated Parquet into `fraud.fact_transactions` using `COPY ... FORMAT AS PARQUET`.
7. **QuickSight** builds a fraud dashboard on top of Redshift datasets and scheduled refresh.

## Key features

- Streaming ingestion with Kinesis and durable S3 landing for replayability.
- Curated lake outputs using partitioned Parquet (`year/month/day`) for efficient analytics.
- Heuristic fraud scoring rules (starter baseline) engineered alongside derived features.
- Redshift loading via `S3ToRedshiftOperator` and COPY optimized for Parquet.
- Automated **Redshift data quality checks** (row count, null checks, ranges, duplicate transaction IDs) designed to fail Airflow on bad data.
- Operational monitoring via CloudWatch alarms (Glue failures, Kinesis lag, Lambda errors) and SNS alerts.
- Analytics-ready SQL and dashboard guidance for QuickSight.

## Setup instructions

### Prerequisites

- Python (project expects Python **3.10** or compatible)
- AWS account + permissions to create Kinesis, S3, Glue, Redshift, Lambda, MWAA, CloudWatch, and SNS resources
- AWS CLI configured (`aws configure`)
- Terraform **1.5+**

### Clone the repository

```bash
git clone <your-repo-url>
cd FRAUD_DETECTING_PROJECT_AWS
```

### Configure AWS CLI

```bash
aws configure
```

Ensure credentials can create and manage resources in the selected region.

### Terraform: provision infrastructure

> The Terraform backend for state is configured in `infrastructure/main.tf` to an S3 bucket named `fraud-detection-tf-state`. Create that bucket (and versioning) before the first `terraform init`.

```bash
cd infrastructure
terraform init
terraform plan
terraform apply
```

### Run the producer (generate events)

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

2. Create/activate a virtual environment (optional if you already use one):

```bash
./setup.sh
```

3. Publish events to Kinesis (default stream: `fraud-detection-transactions`):

```bash
python src/producer/producer.py
```

## Sample queries

Daily fraud summary (fraud trend by day):

```sql
SELECT
    CAST(transaction_timestamp AS DATE) AS txn_date,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(fraud_score) * 100, 2) AS avg_fraud_score_pct,
    SUM(CASE WHEN is_fraud_flagged THEN amount ELSE 0 END) AS total_fraud_usd
FROM fraud.fact_transactions
GROUP BY CAST(transaction_timestamp AS DATE)
ORDER BY txn_date DESC;
```

Example output:

| txn_date   | total_transactions | fraud_count | avg_fraud_score_pct | total_fraud_usd |
|---|---:|---:|---:|---:|
| 2026-05-07 | 12,450 | 612  | 34.87 | 48,392.55 |
| 2026-05-06 | 11,980 | 584  | 33.22 | 45,701.10 |
| 2026-05-05 | 12,210 | 601  | 35.03 | 47,120.43 |

## Dashboard

![Dashboard](docs/dashboard.png)

## Future improvements

- **ML model integration (SageMaker)**: replace heuristic rules with a trained model and persist calibrated risk probabilities.
- **dbt transformations**: standardize SQL transformations and enforce tests/lineage for the curated Redshift layer.
- **CDC with AWS DMS**: support change data capture from operational systems to supplement streaming signals.
- **Data lineage & governance**: integrate AWS Glue job metadata, catalog conventions, and lineage tooling to track dataset provenance end-to-end.
