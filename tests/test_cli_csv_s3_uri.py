"""Tests for the --csv-s3-uri command line flag."""

from __future__ import annotations

from generator.main import _build_parser, _reslove_settings

_S3_URI = "s3://paysim-fraud-pipeline-raw/paysim/paysim.csv"


def test_settings_carry_csv_s3_uri_from_the_flag() -> None:
    args = _build_parser().parse_args(["--csv-s3-uri", _S3_URI])

    assert _reslove_settings(args).csv_s3_uri == _S3_URI
