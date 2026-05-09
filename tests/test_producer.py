"""Unit tests for ``producer.py`` helpers (no AWS calls unless mocked)."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta

import pytest
from botocore.exceptions import ClientError
from faker import Faker

import producer


def _assert_iso8601_utc(value: str) -> None:
    assert isinstance(value, str)
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    assert dt.tzinfo is not None
    assert dt.utcoffset() == timedelta(0)


@pytest.fixture
def reproducible_fake() -> Faker:
    fake = Faker()
    fake.seed_instance(4711)
    return fake


class TestGenerateTransaction:
    def test_returns_dict_with_all_required_keys(self) -> None:
        payload = producer.generate_transaction()
        assert isinstance(payload, dict)
        assert set(payload) == producer.TRANSACTION_PAYLOAD_KEYS

    def test_card_number_hash_is_sha256_hex(self, reproducible_fake: Faker) -> None:
        payload = producer.generate_transaction(fake=reproducible_fake)
        h = payload["card_number_hash"]
        assert isinstance(h, str)
        assert len(h) == 64
        assert re.fullmatch(r"[0-9a-f]{64}", h) is not None

    def test_amount_within_bounds_many_samples(self) -> None:
        for seed in range(200):
            Faker.seed(seed)
            txn = producer.generate_transaction()
            amt = float(txn["amount"])
            assert isinstance(txn["amount"], float)
            assert 1.0 <= amt <= 5000.0

    def test_transaction_timestamp_is_valid_iso8601_utc(self, reproducible_fake: Faker) -> None:
        payload = producer.generate_transaction(fake=reproducible_fake)
        ts = payload["transaction_timestamp"]
        assert isinstance(ts, str)
        _assert_iso8601_utc(ts)
        tz_part = ts[-1] == "Z" or ts.endswith("+00:00")
        assert tz_part


class TestSuspiciousTransactions:
    def test_suspicious_transactions_have_high_amount_floor(self, reproducible_fake: Faker) -> None:
        for _ in range(50):
            payload = producer.generate_transaction(fake=reproducible_fake, is_suspicious=True)
            assert float(payload["amount"]) >= 800.0


class TestSendToKinesis:
    def test_put_record_calls_with_expected_payload(self, reproducible_fake: Faker) -> None:
        calls: dict[str, object] = {}

        class FakeClient:
            def put_record(self, *, StreamName, Data, PartitionKey):
                calls["kwargs"] = {
                    "StreamName": StreamName,
                    "Data": Data,
                    "PartitionKey": PartitionKey,
                }

        txn = producer.build_transaction(reproducible_fake)
        payload = txn.to_dict()
        ok = producer.send_to_kinesis(payload, client=FakeClient())

        assert ok is True
        captured = calls["kwargs"]
        assert captured["StreamName"] == producer.STREAM_NAME
        assert captured["PartitionKey"] == txn.transaction_id
        decoded = json.loads(bytes(captured["Data"]).decode("utf-8"))
        assert decoded == payload

    def test_returns_false_on_client_error(self, reproducible_fake: Faker) -> None:
        class FlakyClient:
            def put_record(self, **_kwargs):
                raise ClientError(
                    {
                        "Error": {"Code": "ResourceNotFoundException", "Message": "gone"},
                        "ResponseMetadata": {"HTTPStatusCode": 400},
                    },
                    "PutRecord",
                )

        payload = producer.generate_transaction(fake=reproducible_fake)
        assert producer.send_to_kinesis(payload, client=FlakyClient()) is False
