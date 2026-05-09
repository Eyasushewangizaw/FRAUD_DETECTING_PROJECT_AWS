"""
Redshift data-quality gate for ``fraud.fact_transactions``.

Uses psycopg2 for SQL checks and boto3 (STS) for a lightweight AWS identity breadcrumb
so Airflow / MWAA logs show which principal ran the validation when credentials allow.
"""

from __future__ import annotations

import os
import sys
from contextlib import closing

import boto3
import psycopg2
from botocore.exceptions import BotoCoreError, ClientError
from psycopg2.extensions import connection as PgConnection

TABLE = "fraud.fact_transactions"


def _env_required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"[FAIL] Missing required environment variable: {name}")
        sys.exit(1)
    return value


def _connect() -> PgConnection:
    return psycopg2.connect(
        host=_env_required("REDSHIFT_HOST"),
        dbname=_env_required("REDSHIFT_DB"),
        user=_env_required("REDSHIFT_USER"),
        password=_env_required("REDSHIFT_PASSWORD"),
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        connect_timeout=int(os.environ.get("REDSHIFT_CONNECT_TIMEOUT", "30")),
        sslmode=os.environ.get("REDSHIFT_SSLMODE", "prefer"),
    )


def _log_boto_audit() -> None:
    """Proof that boto3 credentials work alongside DB auth (helps MWAA troubleshooting)."""

    try:
        arn = boto3.client("sts").get_caller_identity().get("Arn", "?")
        print(f"STS caller ARN (boto3): {arn}")
    except (ClientError, BotoCoreError) as exc:
        print(f"WARN: boto3 STS call skipped ({type(exc).__name__}: {exc})")


def _print_result(name: str, passed: bool, detail: str) -> bool:
    label = "PASS" if passed else "FAIL"
    print(f"[{label}] {name} — {detail}")
    return passed


def run_checks() -> bool:
    _log_boto_audit()

    all_ok = True

    with closing(_connect()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
            (total_rows,) = cur.fetchone()
            ok = total_rows > 0
            all_ok &= _print_result(
                "Row count (>0)",
                ok,
                f"row_count={total_rows}",
            )

            cur.execute(
                f"""
                SELECT
                    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN transaction_timestamp IS NULL THEN 1 ELSE 0 END)
                FROM {TABLE};
                """
            )
            null_tid, null_amt, null_ts = cur.fetchone()
            null_tid = int(null_tid or 0)
            null_amt = int(null_amt or 0)
            null_ts = int(null_ts or 0)
            null_total = null_tid + null_amt + null_ts
            ok = null_total == 0
            all_ok &= _print_result(
                "Null sentinels (transaction_id, amount, transaction_timestamp)",
                ok,
                f"null_transaction_id={null_tid}, null_amount={null_amt}, null_transaction_timestamp={null_ts}",
            )

            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {TABLE}
                WHERE amount IS NOT NULL
                  AND (amount < 0.01 OR amount > 10000)
                ;
                """
            )
            (bad_amounts,) = cur.fetchone()
            bad_amounts = int(bad_amounts or 0)
            ok = bad_amounts == 0
            all_ok &= _print_result(
                "Amount range [0.01, 10000] (non-null amounts)",
                ok,
                f"violating_rows={bad_amounts}",
            )

            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {TABLE}
                WHERE fraud_score IS NULL
                   OR fraud_score < 0
                   OR fraud_score > 1
                ;
                """
            )
            (bad_scores,) = cur.fetchone()
            bad_scores = int(bad_scores or 0)
            ok = bad_scores == 0
            all_ok &= _print_result(
                "Fraud score range [0.0, 1.0] (NULL treated as violation)",
                ok,
                f"violating_rows={bad_scores}",
            )

            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT transaction_id
                    FROM {TABLE}
                    GROUP BY transaction_id
                    HAVING COUNT(*) > 1
                ) AS duplicate_ids
                ;
                """
            )
            (dup_ids,) = cur.fetchone()
            dup_ids = int(dup_ids or 0)
            ok = dup_ids == 0
            all_ok &= _print_result(
                "Duplicate transaction_id groups",
                ok,
                f"duplicate_id_groups={dup_ids}",
            )

    summary = "All checks passed." if all_ok else "One or more checks failed."
    print(summary)
    return all_ok


def main() -> None:
    try:
        ok = run_checks()
    except psycopg2.Error as exc:
        print(f"[FAIL] Redshift error: {exc}")
        sys.exit(1)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
