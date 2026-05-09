"""
AWS Glue ETL script: validates raw fraud-transaction JSON, engineers features,
applies heuristic fraud scoring, and writes curated partitioned Parquet to S3.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, hour, lit, month, weekday, year
from pyspark.sql.types import BooleanType, DoubleType

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    force=True,
)

REQUIRED_AMOUNT_COL = "amount"
REQUIRED_TS_COL = "transaction_timestamp"


def parse_args(argv: list[str]) -> dict[str, str]:
    return getResolvedOptions(argv, ["JOB_NAME", "S3_INPUT", "S3_OUTPUT"])


def init_glue(job_name: str, glue_args: dict[str, Any]) -> tuple[GlueContext, Job]:
    LOGGER.info("Bootstrapping Spark / Glue contexts for JOB_NAME=%s", job_name)
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    job = Job(glue_ctx)
    job.init(job_name, glue_args)

    parallelism = getattr(sc, "defaultParallelism", None)
    if parallelism:
        LOGGER.info("Spark parallelism now set to ~%s task slots.", parallelism)
    return glue_ctx, job


def read_transactions_json(spark_session: Any, s3_uri: str) -> DataFrame:
    LOGGER.info("Reading raw JSON inputs from %s", s3_uri)
    df = spark_session.read.option("inferSchema", "true").option("multiLine", "false").json(
        s3_uri,
    )
    LOGGER.info("Inferred schema after read:\n%s", df.schema.simpleString())
    LOGGER.info("Row count post-read (before QC): %d", df.count())
    return df


def drop_incomplete_records(df: DataFrame) -> DataFrame:
    LOGGER.info(
        "Dropping rows missing any required field: transaction_id / amount / transaction_timestamp",
    )
    before = df.count()
    tid = col("transaction_id").cast("string")
    cleaned = df.filter(
        tid.isNotNull()
        & (F.length(F.trim(tid)) > lit(0))
        & col(REQUIRED_AMOUNT_COL).isNotNull()
        & col(REQUIRED_TS_COL).isNotNull(),
    )
    after = cleaned.count()
    LOGGER.info(
        "Row-level QC retained %d of %d rows (removed %d).",
        after,
        before,
        before - after,
    )
    return cleaned


def cast_core_types(df: DataFrame) -> DataFrame:
    LOGGER.info(
        "Casting amount→double and parsing timestamps; aligning boolean sentinel columns.",
    )
    booleans = ["is_international", "card_present"]

    typed = df
    for bn in booleans:
        if bn not in typed.columns:
            typed = typed.withColumn(bn, lit(None).cast(BooleanType()))
            LOGGER.warning(
                "Column %s absent from payload—initialising null booleans downstream.",
                bn,
            )
        else:
            typed = typed.withColumn(bn, col(bn).cast(BooleanType()))

    typed = (
        typed.withColumn(REQUIRED_AMOUNT_COL, col(REQUIRED_AMOUNT_COL).cast(DoubleType()))
        .withColumn(
            REQUIRED_TS_COL,
            F.to_timestamp(col(REQUIRED_TS_COL)),
        )
    ).filter(col(REQUIRED_TS_COL).isNotNull())

    after_ts = typed.count()
    LOGGER.info("%d rows remain after coercion + valid timestamp predicate.", after_ts)

    typed = typed.fillna({bn: False for bn in booleans})
    LOGGER.info(
        "Null-safe defaults applied for booleans so rule evaluation stays deterministic.",
    )
    return typed


def add_time_features(df: DataFrame) -> DataFrame:
    LOGGER.info(
        "Deriving transaction_hour, weekday-based is_weekend, and partition buckets year/month/day.",
    )
    # Spark weekday(): Monday == 1 … Sunday == 7 → weekend spans Saturday (6) + Sunday (7).
    wd = weekday(col(REQUIRED_TS_COL))
    enriched = (
        df.withColumn("transaction_hour", hour(REQUIRED_TS_COL))
        .withColumn("is_weekend", wd.isin(6, 7))
        .withColumn("year", year(REQUIRED_TS_COL))
        .withColumn("month", month(REQUIRED_TS_COL))
        .withColumn("day", F.dayofmonth(REQUIRED_TS_COL))
    )
    LOGGER.info(
        "Sample aggregates—weekend_share=%.4f hourly_min=%s hourly_max=%s",
        enriched.filter(col("is_weekend")).count() / max(enriched.count(), 1),
        enriched.select(F.min("transaction_hour")).first()[0],
        enriched.select(F.max("transaction_hour")).first()[0],
    )
    return enriched


def apply_fraud_score(df: DataFrame) -> DataFrame:
    LOGGER.info("Applying prioritized rule ladder for heuristic fraud scoring.")
    hour_col = col("transaction_hour")
    amt = col("amount")
    intl = col("is_international")
    present = col("card_present")

    scored = df.withColumn(
        "fraud_score",
        F.when(
            (amt > 1000)
            & intl
            & hour_col.between(0, 5),
            lit(0.92),
        )
        .when(amt > 2000, lit(0.75))
        .when(intl & ~present, lit(0.60))
        .otherwise(lit(0.10))
        .cast(DoubleType()),
    ).withColumn(
        "is_fraud_flagged",
        col("fraud_score") > lit(0.5),
    )
    totals = scored.count()
    flagged = scored.filter(col("is_fraud_flagged")).count()
    LOGGER.info(
        "Completed scoring—flagged %d/%d rows (>0.5 threshold).",
        flagged,
        totals,
    )
    return scored


def finalize_timestamps(df: DataFrame) -> DataFrame:
    LOGGER.info("Stamping audit column processing_timestamp in UTC-aligned Spark timestamps.")
    return df.withColumn("processing_timestamp", F.current_timestamp())


def write_partitioned(df: DataFrame, s3_out: str, job: Job) -> None:
    LOGGER.info(
        "Writing appended Parquet to %s partitioned by year/month/day …",
        s3_out,
    )
    record_count = df.count()
    (
        df.write.mode("append")
        .partitionBy("year", "month", "day")
        .option("compression", "snappy")
        .parquet(s3_out)
    )
    LOGGER.info("Persisted %d curated records into %s", record_count, s3_out)
    LOGGER.info("Committing Glue bookkeeping metadata.")
    job.commit()


def run(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv

    args = parse_args(argv)
    job_name = args["JOB_NAME"]
    s3_input = args["S3_INPUT"]
    s3_output = args["S3_OUTPUT"]

    LOGGER.info(
        "Starting fraud ETL Glue job [%s]; input→%s output→%s",
        job_name,
        s3_input,
        s3_output,
    )

    glue_ctx, job_obj = init_glue(job_name, args)
    spark_session = glue_ctx.spark_session

    raw_df = read_transactions_json(spark_session, s3_input)
    qc_df = drop_incomplete_records(raw_df)
    typed_df = cast_core_types(qc_df)
    feat_df = add_time_features(typed_df)
    scored_df = apply_fraud_score(feat_df)
    final_df = finalize_timestamps(scored_df)

    write_partitioned(final_df, s3_output, job_obj)
    LOGGER.info("Glue fraud ETL workflow completed successfully.")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
