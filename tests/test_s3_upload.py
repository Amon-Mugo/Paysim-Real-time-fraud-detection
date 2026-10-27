"""Tests for the S3 file upload helper."""

from __future__ import annotations

from pathlib import Path

import pytest

from generator.s3_upload import upload_file

_S3_URI = "s3://paysim-fraud-pipeline-raw/ground-truth/ground_truth.jsonl"


class _FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, str]] = []

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.uploads.append((filename, bucket, key))


def test_upload_sends_the_file_to_the_parsed_bucket_and_key(tmp_path: Path) -> None:
    source = tmp_path / "ground_truth.jsonl"
    source.write_text("{}\n", encoding="utf-8")
    client = _FakeS3Client()

    upload_file(source, _S3_URI, s3_client=client)

    assert client.uploads == [
        (
            str(source),
            "paysim-fraud-pipeline-raw",
            "ground-truth/ground_truth.jsonl",
        )
    ]


def test_upload_rejects_a_missing_file_without_calling_s3(tmp_path: Path) -> None:
    client = _FakeS3Client()

    with pytest.raises(FileNotFoundError):
        upload_file(tmp_path / "missing.jsonl", _S3_URI, s3_client=client)

    assert client.uploads == []


def test_upload_rejects_an_invalid_uri_without_calling_s3(tmp_path: Path) -> None:
    source = tmp_path / "ground_truth.jsonl"
    source.write_text("{}\n", encoding="utf-8")
    client = _FakeS3Client()

    with pytest.raises(ValueError):
        upload_file(source, "not-an-s3-uri", s3_client=client)

    assert client.uploads == []
