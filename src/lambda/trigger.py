"""
Lambda handler for S3ObjectCreated notifications under raw/transactions/.

Each event record can start a Glue ETL run whose script receives --S3_INPUT and --S3_OUTPUT URIs.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import BotoCoreError, ClientError

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

glue = boto3.client("glue")

_RAW_PREFIX = "raw/transactions/"
_JOB_ENV = "GLUE_JOB_NAME"
_CURATED_PREFIX_ENV = "CURATED_OUTPUT_PREFIX"
_OUTPUT_BUCKET_ENV = "OUTPUT_BUCKET"


def _build_io_uris(bucket: str, raw_key: str) -> tuple[str, str]:
    decoded_key = unquote_plus(raw_key).lstrip("/")
    curated_prefix = os.environ.get(_CURATED_PREFIX_ENV, "curated/transactions").strip("/")
    out_bucket = os.environ.get(_OUTPUT_BUCKET_ENV, bucket).strip()

    input_uri = f"s3://{bucket.strip()}/{decoded_key}"
    output_uri = f"s3://{out_bucket}/{curated_prefix}"
    return input_uri, output_uri


def _start_glue_etl(job_name: str, bucket: str, object_key: str) -> dict[str, str]:
    s3_input, s3_output = _build_io_uris(bucket, object_key)

    LOGGER.info(
        "Starting Glue job %s (--S3_INPUT=%s --S3_OUTPUT=%s)",
        job_name,
        s3_input,
        s3_output,
    )

    response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--JOB_NAME": job_name,
            "--S3_INPUT": s3_input,
            "--S3_OUTPUT": s3_output,
        },
    )
    job_run_id = response.get("JobRunId", "")
    LOGGER.info(
        "Glue JobRunId=%s HTTP=%s trigger_object=s3://%s/%s",
        job_run_id,
        response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
        bucket,
        unquote_plus(object_key),
    )
    return {
        "JobRunId": job_run_id,
        "bucket": bucket,
        "key": object_key,
        "S3_INPUT": s3_input,
        "S3_OUTPUT": s3_output,
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Processes batched Records from S3 event notifications.

    Returns Gateway-style payloads (statusCode + JSON body). For pure async S3→Lambda invokes
    the return value chiefly aids logging and optional downstream wrappers.

    Does not raise outward; problems return HTTP 500 payloads for operators to reconcile.
    """
    try:
        job_name = os.environ[_JOB_ENV]
    except KeyError:
        LOGGER.error("Missing required env var GLUE_JOB_NAME – cannot enqueue Glue executions.")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Server misconfiguration – GLUE_JOB_NAME unset."}),
        }

    job_runs: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []

    try:
        records = event.get("Records") or []

        if not isinstance(records, list):
            msg = "'Records' missing or malformed for S3 payloads"
            LOGGER.error(msg)
            return {"statusCode": 500, "body": json.dumps({"message": msg})}

        LOGGER.info(
            "Processing %d S3 record(s); request=%s",
            len(records),
            getattr(context, "aws_request_id", "unknown"),
        )

        for idx, record in enumerate(records):
            try:
                bucket = (
                    record.get("s3", {}).get("bucket", {}).get("name")
                )
                raw_key = record.get("s3", {}).get("object", {}).get("key")

                if not bucket or not raw_key:
                    errors.append({"record_index": str(idx), "detail": "Missing bucket/name or object/key"})
                    LOGGER.error("Record %s lacked bucket/name or object/key.", idx)
                    continue

                normalized_key = unquote_plus(raw_key)
                if not normalized_key.startswith(_RAW_PREFIX):
                    skipped.append({"bucket": bucket, "key": raw_key})
                    LOGGER.info(
                        "Skipping record %s (key='%s') – outside monitored prefix '%s'.",
                        idx,
                        normalized_key,
                        _RAW_PREFIX,
                    )
                    continue

                outcome = _start_glue_etl(job_name, bucket, raw_key)
                job_runs.append(outcome)

            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "GlueClientError")
                errors.append({"record_index": str(idx), "detail": code, "message": str(exc)})
                LOGGER.error("Glue ClientError record %s: [%s] %s", idx, code, exc, exc_info=True)
            except BotoCoreError as exc:
                errors.append({"record_index": str(idx), "detail": type(exc).__name__, "message": str(exc)})
                LOGGER.error("Glue BotoCoreError record %s: %s", idx, exc, exc_info=True)
            except Exception as exc:
                LOGGER.exception("Unexpected handler failure on record %s", idx)
                errors.append({"record_index": str(idx), "detail": type(exc).__name__, "message": str(exc)})

        if errors:
            LOGGER.error(
                "Completed batch with Glue failures – successes=%s errors=%s skips=%s",
                len(job_runs),
                len(errors),
                len(skipped),
            )
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {
                        "message": "One or more records failed Glue job submission.",
                        "jobRuns": job_runs,
                        "skipped": skipped,
                        "errors": errors,
                    },
                    default=str,
                ),
            }

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Glue job runs queued for actionable S3 notifications.",
                    "jobRuns": job_runs,
                    "skipped": skipped,
                    "requestId": getattr(context, "aws_request_id", None),
                },
                default=str,
            ),
        }

    except Exception:
        LOGGER.exception("Fatal Lambda error before completing batch.")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "Fatal handler failure – inspect CloudWatch Logs.",
                    "jobRuns": job_runs,
                    "skipped": skipped,
                    "errors": errors,
                },
                default=str,
            ),
        }
