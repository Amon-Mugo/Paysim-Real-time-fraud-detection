"""Tests for the PaySim CSV source: S3 URI parsing and local or S3 reading."""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

import pytest

from generator.config import GeneratorConfig
from generator.csv_source import open_csv, parse_s3_uri
from generator.paysim_reader import read_paysim

CSV_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
]
S3_URI = "s3://raw-bucket/paysim/paysim dataset.csv"


class FakeS3Client:
    """Minimal stand-in for a boto3 S3 client that serves one object."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.requests: list[dict[str, str]] = []

    def get_object(self, **kwargs: str) -> dict[str, Any]:
        self.requests.append(kwargs)
        return {"Body": io.BytesIO(self._payload)}


def _build_csv_text(steps: list[int]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    for step in steps:
        writer.writerow(
            {
                "step": step,
                "type": "TRANSFER",
                "amount": 100.0,
                "nameOrig": f"C{step}",
                "oldbalanceOrg": 500.0,
                "newbalanceOrig": 400.0,
                "nameDest": f"M{step}",
                "oldbalanceDest": 0.0,
                "newbalanceDest": 100.0,
                "isFraud": 0,
            }
        )
    return buffer.getvalue()


def test_parse_s3_uri_splits_bucket_and_key() -> None:
    assert parse_s3_uri(S3_URI) == ("raw-bucket", "paysim/paysim dataset.csv")


@pytest.mark.parametrize(
    "invalid", ["raw-bucket/paysim.csv", "s3://raw-bucket", "s3:///paysim.csv"]
)
def test_parse_s3_uri_rejects_malformed_uris(invalid: str) -> None:
    with pytest.raises(ValueError, match="S3 URI"):
        parse_s3_uri(invalid)


def test_config_rejects_non_s3_uri() -> None:
    with pytest.raises(ValueError, match="csv_s3_uri"):
        GeneratorConfig(csv_s3_uri="https://example.com/paysim.csv")


def test_open_csv_reads_the_local_file_by_default(tmp_path: Path) -> None:
    csv_path = tmp_path / "paysim.csv"
    csv_path.write_text(_build_csv_text([1]), encoding="utf-8")

    with open_csv(GeneratorConfig(csv_path=csv_path)) as handle:
        header = handle.readline().strip()

    assert header.startswith("step,type,amount")


def test_open_csv_streams_the_s3_object() -> None:
    client = FakeS3Client(_build_csv_text([1]).encode("utf-8"))

    with open_csv(GeneratorConfig(csv_s3_uri=S3_URI), s3_client=client) as handle:
        header = handle.readline().strip()

    assert header.startswith("step,type,amount")
    assert client.requests == [
        {"Bucket": "raw-bucket", "Key": "paysim/paysim dataset.csv"}
    ]


def test_reader_returns_the_same_records_from_s3_and_local(tmp_path: Path) -> None:
    text = _build_csv_text([1, 2, 3])
    csv_path = tmp_path / "paysim.csv"
    csv_path.write_text(text, encoding="utf-8")
    client = FakeS3Client(text.encode("utf-8"))

    local = list(read_paysim(GeneratorConfig(csv_path=csv_path)))

    with open_csv(GeneratorConfig(csv_s3_uri=S3_URI), s3_client=client) as handle:
        from_s3 = list(csv.DictReader(handle))

    assert [record.name_orig for record in local] == [
        row["nameOrig"] for row in from_s3
    ]
