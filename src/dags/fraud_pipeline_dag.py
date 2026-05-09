"""
Hourly fraud analytics batch: Glue ETL for curated Parquet, Glue crawler refresh,
Redshift COPY of curated data, mandatory Redshift data-quality gates, success audit log.

Pipeline SLA (operational target): finish within ~50 minutes of each hourly dag run
boundary so downstream dashboards stay fresh; Glue/Redshift capacity should be tuned
to keep p95 runtime under this window.

Execution order note: ``data_quality_check`` runs **after** ``load_to_redshift`` because
``src/utils/data_quality.py`` asserts row-level integrity on ``fraud.fact_transactions``
in Amazon Redshift (DQ would fail before any rows exist).

Configure Airflow Variables (or overrides in deployment UI):
  - ``fraud_curated_bucket``: S3 bucket for curated Parquet
  - ``fraud_curated_s3_prefix``: key prefix passed to COPY (no leading ``s3://``)
  - ``redshift_copy_iam_role_arn``: IAM role Redshift assumes for COPY
Connections: ``aws_default``, ``redshift_default``.
"""

from __future__ import annotations

import logging
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

LOGGER = logging.getLogger(__name__)

DAG_ID = "fraud_detection_pipeline"
GLUE_JOB_NAME = "fraud-detection-etl"
GLUE_CRAWLER_NAME = "fraud-curated-crawler"
REDSHIFT_SCHEMA = "fraud"
REDSHIFT_TABLE = "fact_transactions"

_UTILS_PATH = Path(__file__).resolve().parent.parent / "utils"


def _run_data_quality_checks() -> None:
    """Fails the task unless every Redshift DQ rule in ``data_quality.py`` passes."""

    utils_dir = str(_UTILS_PATH)
    if utils_dir not in sys.path:
        sys.path.insert(0, utils_dir)

    from data_quality import run_checks

    if not run_checks():
        raise AirflowException("Redshift data quality checks reported FAIL — see upstream logs")


def _log_pipeline_success() -> None:
    """Log pipeline completion with current Redshift mart rowcount."""

    try:
        from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
    except ImportError:
        from airflow.providers.postgres.hooks.postgres import PostgresHook as RedshiftSQLHook

    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")  # type: ignore[assignment,misc]
    row = hook.get_first(f"SELECT COUNT(*) FROM {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE};")
    count = row[0] if row else None
    LOGGER.info(
        "fraud_detection_pipeline completed successfully — %s.%s row_count=%s",
        REDSHIFT_SCHEMA,
        REDSHIFT_TABLE,
        count,
    )


default_args = {
    "owner": "fraud-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["your@email.com"],
    "email_on_failure": True,
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Orchestrate Glue curated fraud mart, crawler sync, COPY to Redshift, DQ, audit log.",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["fraud", "fintech", "aws"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    run_fraud_etl = GlueJobOperator(
        task_id="run_fraud_etl",
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True,
        verbose=True,
        aws_conn_id="aws_default",
    )

    update_glue_catalog = GlueCrawlerOperator(
        task_id="update_glue_catalog",
        crawler_name=GLUE_CRAWLER_NAME,
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    load_to_redshift = S3ToRedshiftOperator(
        task_id="load_to_redshift",
        schema=REDSHIFT_SCHEMA,
        table=REDSHIFT_TABLE,
        s3_bucket="{{ var.value.get('fraud_curated_bucket') }}",
        s3_key="{{ var.value.get('fraud_curated_s3_prefix', 'curated/transactions') }}",
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
        iam_role_arn="{{ var.value.get('redshift_copy_iam_role_arn') }}",
        copy_options=["FORMAT AS PARQUET"],
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=_run_data_quality_checks,
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_log_pipeline_success,
    )

    run_fraud_etl >> update_glue_catalog >> load_to_redshift >> data_quality_check >> notify_success
