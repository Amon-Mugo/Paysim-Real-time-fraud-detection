"""Tests for the ground truth S3 upload setting and its use in replay."""

from __future__ import annotations

from pathlib import Path

import pytest

from generator import producer as producer_module
from generator.config import GeneratorConfig
from generator.main import _build_parser, _reslove_settings
from generator.producer import replay

_S3_URI = "s3://paysim-fraud-pipeline-raw/ground-truth/ground_truth.jsonl"


class _FakeProducer:
    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


def _settings(tmp_path: Path, s3_uri: str) -> GeneratorConfig:
    return GeneratorConfig(
        ground_truth_path=tmp_path / "ground_truth.jsonl",
        ground_truth_s3_uri=s3_uri,
    )


def _stub_replay_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[Path, str]]:
    uploads: list[tuple[Path, str]] = []
    monkeypatch.setattr(producer_module, "merge_events", lambda settings: iter(()))
    monkeypatch.setattr(
        producer_module,
        "upload_file",
        lambda path, s3_uri: uploads.append((path, s3_uri)),
    )
    return uploads


def test_default_ground_truth_s3_uri_is_empty() -> None:
    assert GeneratorConfig().ground_truth_s3_uri == ""


def test_config_rejects_a_ground_truth_uri_without_the_s3_prefix() -> None:
    with pytest.raises(ValueError):
        GeneratorConfig(ground_truth_s3_uri="https://example.com/ground_truth.jsonl")


def test_settings_carry_ground_truth_s3_uri_from_the_flag() -> None:
    args = _build_parser().parse_args(["--ground-truth-s3-uri", _S3_URI])

    assert _reslove_settings(args).ground_truth_s3_uri == _S3_URI


def test_replay_uploads_ground_truth_when_the_uri_is_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uploads = _stub_replay_dependencies(monkeypatch)
    settings = _settings(tmp_path, _S3_URI)

    replay(settings, producer=_FakeProducer())

    assert uploads == [(settings.ground_truth_path, _S3_URI)]


def test_replay_skips_the_upload_when_the_uri_is_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uploads = _stub_replay_dependencies(monkeypatch)

    replay(_settings(tmp_path, ""), producer=_FakeProducer())

    assert uploads == []
