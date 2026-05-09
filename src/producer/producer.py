#!/usr/bin/env python3
"""
Generates plausible financial transaction payloads with Faker, optionally elevated to a
"suspicious" amount band (~5%), and publishes JSON records to a Kinesis Data Stream for
pipeline testing (~2 Hz by default).

Environment: relies on boto3 credential chain and AWS_DEFAULT_REGION when set.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from faker import Faker

LOGGER = logging.getLogger(__name__)

STREAM_NAME = "fraud-detection-transactions"
SLEEP_SECONDS = 0.5

MERCHANT_CATEGORIES = (
    "retail",
    "gas_station",
    "restaurant",
    "online",
    "atm",
    "travel",
)

# Rough ISO 3166-1 alpha-2 mix; ~60% US, remainder weighted across common markets.
_WEIGHTED_COUNTRY_CHOICES: list[tuple[str, float]] = [
    ("US", 60.0),
    ("MX", 5.5),
    ("CA", 5.5),
    ("GB", 4.5),
    ("JP", 3.5),
    ("DE", 3.0),
    ("FR", 2.5),
    ("BR", 2.5),
    ("IN", 2.0),
    ("CN", 1.6),
]


@dataclass
class Transaction:
    transaction_id: str
    card_number_hash: str
    merchant_id: str
    merchant_category: str
    amount: float
    transaction_country: str
    transaction_timestamp: str
    card_present: bool
    is_international: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


TRANSACTION_PAYLOAD_KEYS = frozenset(Transaction.__dataclass_fields__.keys())


def _pick_country(fake: Faker) -> str:
    total_w = sum(w for _, w in _WEIGHTED_COUNTRY_CHOICES)
    r = random.uniform(0, total_w)
    acc = 0.0
    for country, weight in _WEIGHTED_COUNTRY_CHOICES:
        acc += weight
        if r <= acc:
            return country
    return "US"


def _amount_normal() -> float:
    return round(random.uniform(1.0, 5000.0), 2)


def _amount_suspicious() -> float:
    """Elevated-band amount for staged anomaly injection (~5% of rows)."""

    return round(random.uniform(800.0, 5000.0), 2)


def _merchant_id(fake: Faker) -> str:
    uid_hex = fake.unique.uuid4().replace("-", "")
    return f"MERCH-{uid_hex[:10].upper()}"


def _card_present_for_category(category: str) -> bool:
    """Category-aware weighting — ATMs skew card-present; online skew not present."""

    if category == "online":
        return random.random() < 0.12
    if category == "atm":
        return random.random() < 0.98
    if category == "gas_station":
        return random.random() < 0.85
    if category == "retail":
        return random.random() < 0.78
    if category == "restaurant":
        return random.random() < 0.80
    if category == "travel":
        return random.random() < 0.45
    return random.random() < 0.6


def build_transaction(
    fake: Faker,
    *,
    is_suspicious: bool | None = None,
) -> Transaction:
    card_num = fake.credit_card_number()
    txn_id = f"TXN-{uuid.uuid4()}"
    card_hash = hashlib.sha256(card_num.encode("utf-8")).hexdigest()

    if is_suspicious is None:
        suspicious = random.random() < 0.05
    else:
        suspicious = is_suspicious
    amount = _amount_suspicious() if suspicious else _amount_normal()

    category = random.choice(MERCHANT_CATEGORIES)
    country = _pick_country(fake)
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    return Transaction(
        transaction_id=txn_id,
        card_number_hash=card_hash,
        merchant_id=_merchant_id(fake),
        merchant_category=category,
        amount=amount,
        transaction_country=country,
        transaction_timestamp=ts,
        card_present=_card_present_for_category(category),
        is_international=random.random() < 0.15,
    )


def generate_transaction(
    fake: Faker | None = None,
    *,
    is_suspicious: bool | None = None,
) -> dict[str, object]:
    """Build a synthetic transaction dictionary (public API for callers and tests)."""

    fk = fake or Faker()
    txn = build_transaction(fk, is_suspicious=is_suspicious)
    return txn.to_dict()


def send_to_kinesis(
    record: dict[str, object] | Transaction,
    *,
    client: object | None = None,
    stream_name: str = STREAM_NAME,
) -> bool:
    """JSON-encode ``record`` and ``put_record`` to Kinesis. ``client`` can be mocked in tests."""

    if isinstance(record, Transaction):
        body = record.to_dict()
        partition_key = str(record.transaction_id)
    else:
        body = dict(record)
        partition_key = str(body["transaction_id"])

    payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    kinesis_client = client if client is not None else boto3.client("kinesis")
    return _put_record_safe(kinesis_client, stream_name, partition_key, payload)


def _put_record_safe(
    client: object,
    stream_name: str,
    partition_key: str,
    payload_bytes: bytes,
) -> bool:
    try:
        client.put_record(
            StreamName=stream_name,
            Data=payload_bytes,
            PartitionKey=partition_key[:256],
        )
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "unknown")
        LOGGER.error("Kinesis rejected record: ClientError [%s]: %s", code, exc, exc_info=True)
    except BotoCoreError as exc:
        LOGGER.error("Kinesis SDK error during put_record (BotoCoreError): %s", exc, exc_info=True)

    return False


def stream_loop(interval_s: float = SLEEP_SECONDS) -> None:
    fake = Faker()
    fake.unique.clear()
    kinesis = boto3.client("kinesis")

    LOGGER.info(
        "Starting producer targeting stream %s at ~%.2f records/sec …",
        STREAM_NAME,
        1.0 / interval_s if interval_s > 0 else 0.0,
    )

    consecutive_failures = 0
    n = 0

    try:
        while True:
            txn = build_transaction(fake)
            body = txn.to_dict()
            ok = send_to_kinesis(body, client=kinesis)

            consecutive_failures = 0 if ok else consecutive_failures + 1
            if not ok:
                backoff = min(30.0, 2.0**min(consecutive_failures, 4))
                LOGGER.warning(
                    "Backing off %.1fs after failed put (failure streak=%d)",
                    backoff,
                    consecutive_failures,
                )
                time.sleep(backoff)

            n += 1
            if n % 10 == 0:
                LOGGER.info("Published %s records …", n)

            if ok:
                time.sleep(interval_s)
    except KeyboardInterrupt:
        LOGGER.info("Stopping producer after ~%s records (KeyboardInterrupt)", n)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    stream_loop()


if __name__ == "__main__":
    main()
