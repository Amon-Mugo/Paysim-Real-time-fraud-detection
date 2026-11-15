"""Unit tests for airflow/dags/fraud_eval_fetch.py."""

import pytest

from airflow.dags.fraud_eval_fetch import (
    _parse_s3_uri,
    count_transactions,
    fetch_alerts,
    read_ground_truth_lines,
)


class _FakeCursor:
    def __init__(self, columns, rows):
        self.description = [(column,) for column in columns]
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query, params):
        self.executed.append((query, params))

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0]


class _FakeConnection:
    def __init__(self, columns, rows):
        self.cursor_instance = _FakeCursor(columns, rows)

    def cursor(self):
        return self.cursor_instance


class _FakeBody:
    def __init__(self, lines):
        self._lines = lines

    def iter_lines(self):
        return iter(self._lines)


class _FakeS3Client:
    def __init__(self, lines):
        self._lines = lines
        self.requests = []

    def get_object(self, Bucket, Key):
        self.requests.append((Bucket, Key))
        return {"Body": _FakeBody(self._lines)}


def test_parse_s3_uri_splits_bucket_and_key():
    assert _parse_s3_uri("s3://raw-bucket/ground-truth/eval_1.jsonl") == (
        "raw-bucket",
        "ground-truth/eval_1.jsonl",
    )


@pytest.mark.parametrize(
    "uri", ["http://bucket/key", "s3://bucket", "s3://bucket/", "bucket/key", ""]
)
def test_parse_s3_uri_rejects_invalid_uris(uri):
    with pytest.raises(ValueError):
        _parse_s3_uri(uri)


def test_read_ground_truth_lines_decodes_the_s3_body():
    client = _FakeS3Client([b'{"transaction_id": "T1"}', b'{"transaction_id": "T2"}'])

    lines = read_ground_truth_lines(client, "s3://raw-bucket/ground-truth/eval_1.jsonl")

    assert lines == ['{"transaction_id": "T1"}', '{"transaction_id": "T2"}']
    assert client.requests == [("raw-bucket", "ground-truth/eval_1.jsonl")]


def test_fetch_alerts_returns_lower_case_dicts():
    connection = _FakeConnection(
        ["ALERT_ID", "RULE_NAME", "TRANSACTION_IDS"],
        [("A1", "velocity", '["T1", "T2"]'), ("A2", "ml_model", '["T3"]')],
    )

    alerts = fetch_alerts(connection, 600)

    assert alerts == [
        {"alert_id": "A1", "rule_name": "velocity", "transaction_ids": '["T1", "T2"]'},
        {"alert_id": "A2", "rule_name": "ml_model", "transaction_ids": '["T3"]'},
    ]


def test_fetch_alerts_passes_the_window_as_a_query_parameter():
    connection = _FakeConnection(["ALERT_ID", "RULE_NAME", "TRANSACTION_IDS"], [])

    fetch_alerts(connection, 600)

    query, params = connection.cursor_instance.executed[0]
    assert "FROM ALERTS" in query
    assert "INGESTED_AT >= DATEADD(second, -%s, CURRENT_TIMESTAMP())" in query
    assert params == (600,)


def test_count_transactions_returns_the_scalar_and_passes_the_window():
    connection = _FakeConnection(["COUNT(*)"], [(1234,)])

    assert count_transactions(connection, 900) == 1234

    query, params = connection.cursor_instance.executed[0]
    assert "FROM TRANSACTIONS_RECENT" in query
    assert params == (900,)
