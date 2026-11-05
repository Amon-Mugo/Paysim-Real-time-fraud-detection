# tests/test_snowflake_writer.py
"""Tests for the Snowflake writer's credential loading, options and SQL runner."""

from __future__ import annotations

import base64
import json
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from spark.config import DetectionConfig
from spark.snowflake_writer import (
    SPARK_SNOWFLAKE_FORMAT,
    SnowflakeCredentials,
    build_connect_kwargs,
    build_snowflake_sinks,
    build_spark_options,
    load_credentials,
    make_run_sql,
    make_stage_batch,
)

SECRET_NAME = "paysim-fraud-pipeline/snowflake"
USER = "FRAUD_WRITER_SVC"
PASSPHRASE = "correct horse battery"
ACCOUNT = "xy12345.us-east-1"
STAGING_TABLE = "STG_ALERTS_VELOCITY"
CONFIG = replace(DetectionConfig(), snowflake_account=ACCOUNT)
CREDENTIALS = SnowflakeCredentials(user=USER, private_key_der=b"der-bytes")


class _FakeSecretsClient:
    """Return a fixed secret payload and remember which secret was requested."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self.requested: list[str] = []

    def get_secret_value(self, SecretId: str) -> dict[str, str]:
        self.requested.append(SecretId)
        return {"SecretString": json.dumps(self._payload)}


class _FakeCursor:
    """Act as a cursor context manager that records executed statements."""

    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def execute(self, statement: str) -> None:
        self._connection.executed.append(statement)
        if self._connection.fail_with is not None:
            raise self._connection.fail_with


class _FakeConnection:
    """Record statements and whether the connection was closed."""

    def __init__(self, fail_with: Exception | None = None) -> None:
        self.executed: list[str] = []
        self.closed = False
        self.fail_with = fail_with

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True


class _FakeConnect:
    """Stand in for snowflake.connector.connect and keep every connection it opens."""

    def __init__(self, fail_with: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.connections: list[_FakeConnection] = []
        self._fail_with = fail_with

    def __call__(self, **kwargs: Any) -> _FakeConnection:
        self.calls.append(kwargs)
        connection = _FakeConnection(self._fail_with)
        self.connections.append(connection)
        return connection


class _RecordingWriter:
    """Mimic the DataFrameWriter builder chain and record each call in order."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def _record(self, name: str, *args: Any) -> _RecordingWriter:
        self.calls.append((name, args))
        return self

    def format(self, source: str) -> _RecordingWriter:
        return self._record("format", source)

    def options(self, **kwargs: Any) -> _RecordingWriter:
        return self._record("options", kwargs)

    def option(self, key: str, value: str) -> _RecordingWriter:
        return self._record("option", key, value)

    def mode(self, save_mode: str) -> _RecordingWriter:
        return self._record("mode", save_mode)

    def save(self) -> None:
        self._record("save")


def _pem(key: rsa.RSAPrivateKey, encryption: Any) -> str:
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption,
    ).decode("utf-8")


def _public_numbers(key: Any) -> Any:
    return key.public_key().public_numbers()


@pytest.fixture(scope="module")
def private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def pem(private_key: rsa.RSAPrivateKey) -> str:
    return _pem(private_key, serialization.NoEncryption())


@pytest.fixture(scope="module")
def secrets_client(pem: str) -> _FakeSecretsClient:
    return _FakeSecretsClient({"user": USER, "private_key": pem})


def test_load_credentials_returns_the_user_and_a_der_key(pem, secrets_client):
    credentials = load_credentials(SECRET_NAME, secrets_client)

    assert credentials.user == USER
    assert isinstance(credentials.private_key_der, bytes)


def test_loaded_der_key_matches_the_stored_pem_key(private_key, secrets_client):
    credentials = load_credentials(SECRET_NAME, secrets_client)

    loaded = serialization.load_der_private_key(
        credentials.private_key_der, password=None
    )
    assert _public_numbers(loaded) == _public_numbers(private_key)


def test_load_credentials_decrypts_a_passphrase_protected_key(private_key):
    encrypted_pem = _pem(
        private_key, serialization.BestAvailableEncryption(PASSPHRASE.encode("utf-8"))
    )
    client = _FakeSecretsClient(
        {
            "user": USER,
            "private_key": encrypted_pem,
            "private_key_passphrase": PASSPHRASE,
        }
    )

    credentials = load_credentials(SECRET_NAME, client)

    loaded = serialization.load_der_private_key(
        credentials.private_key_der, password=None
    )
    assert _public_numbers(loaded) == _public_numbers(private_key)


def test_load_credentials_requests_the_named_secret(secrets_client):
    load_credentials(SECRET_NAME, secrets_client)

    assert secrets_client.requested[-1] == SECRET_NAME


@pytest.mark.parametrize(
    "overrides, missing_key",
    [
        pytest.param({"user": ""}, "user", id="empty-user"),
        pytest.param({"user": None}, "user", id="null-user"),
        pytest.param({"private_key": ""}, "private_key", id="empty-key"),
        pytest.param({"private_key": None}, "private_key", id="null-key"),
    ],
)
def test_load_credentials_rejects_missing_or_empty_keys(pem, overrides, missing_key):
    client = _FakeSecretsClient({"user": USER, "private_key": pem, **overrides})

    with pytest.raises(ValueError, match="missing keys") as error:
        load_credentials(SECRET_NAME, client)

    assert missing_key in str(error.value)


def test_credentials_repr_hides_the_private_key():
    assert "private_key_der" not in repr(CREDENTIALS)
    assert "der-bytes" not in repr(CREDENTIALS)


def test_spark_options_target_the_account_and_session_settings():
    options = build_spark_options(CONFIG, CREDENTIALS)

    assert options["sfURL"] == f"{ACCOUNT}.snowflakecomputing.com"
    assert options["sfUser"] == USER
    assert options["sfDatabase"] == CONFIG.snowflake_database
    assert options["sfSchema"] == CONFIG.snowflake_schema
    assert options["sfWarehouse"] == CONFIG.snowflake_warehouse
    assert options["sfRole"] == CONFIG.snowflake_role


def test_spark_options_carry_the_key_as_base64_der():
    options = build_spark_options(CONFIG, CREDENTIALS)

    assert base64.b64decode(options["pem_private_key"]) == CREDENTIALS.private_key_der


def test_spark_options_require_an_account():
    config = replace(CONFIG, snowflake_account="")

    with pytest.raises(ValueError, match="SNOWFLAKE_ACCOUNT"):
        build_spark_options(config, CREDENTIALS)


def test_connect_kwargs_describe_a_key_pair_session():
    kwargs = build_connect_kwargs(CONFIG, CREDENTIALS)

    assert kwargs == {
        "account": ACCOUNT,
        "user": USER,
        "private_key": CREDENTIALS.private_key_der,
        "database": CONFIG.snowflake_database,
        "schema": CONFIG.snowflake_schema,
        "warehouse": CONFIG.snowflake_warehouse,
        "role": CONFIG.snowflake_role,
        "enable_server_session_keep_alive_auto_detection": False,
    }


def test_connect_kwargs_require_an_account():
    config = replace(CONFIG, snowflake_account="")

    with pytest.raises(ValueError, match="SNOWFLAKE_ACCOUNT"):
        build_connect_kwargs(config, CREDENTIALS)


def test_run_sql_executes_the_statement_with_the_connect_kwargs():
    connect = _FakeConnect()
    kwargs = build_connect_kwargs(CONFIG, CREDENTIALS)

    make_run_sql(kwargs, connect)("TRUNCATE TABLE STG_ALERTS_ML")

    assert connect.calls == [kwargs]
    assert connect.connections[0].executed == ["TRUNCATE TABLE STG_ALERTS_ML"]


def test_run_sql_closes_the_connection_after_success():
    connect = _FakeConnect()

    make_run_sql({}, connect)("SELECT 1")

    assert connect.connections[0].closed


def test_run_sql_closes_the_connection_and_reraises_when_execute_fails():
    connect = _FakeConnect(fail_with=RuntimeError("statement failed"))

    with pytest.raises(RuntimeError, match="statement failed"):
        make_run_sql({}, connect)("SELECT 1")

    assert connect.connections[0].closed


def test_run_sql_opens_a_fresh_connection_per_statement():
    connect = _FakeConnect()
    run_sql = make_run_sql({}, connect)

    run_sql("SELECT 1")
    run_sql("SELECT 2")

    assert len(connect.connections) == 2
    assert [conn.executed for conn in connect.connections] == [
        ["SELECT 1"],
        ["SELECT 2"],
    ]
    assert all(conn.closed for conn in connect.connections)


def test_stage_batch_appends_to_the_staging_table_through_the_connector():
    writer = _RecordingWriter()
    options = build_spark_options(CONFIG, CREDENTIALS)

    make_stage_batch(options)(SimpleNamespace(write=writer), STAGING_TABLE)

    assert writer.calls == [
        ("format", (SPARK_SNOWFLAKE_FORMAT,)),
        ("options", (options,)),
        ("option", ("dbtable", STAGING_TABLE)),
        ("option", ("column_mapping", "name")),
        ("mode", ("append",)),
        ("save", ()),
    ]


def test_sinks_read_the_configured_secret_and_open_sessions_with_it(pem):
    client = _FakeSecretsClient({"user": USER, "private_key": pem})
    connect = _FakeConnect()

    stage_batch, run_sql = build_snowflake_sinks(
        CONFIG, secrets_client=client, connect=connect
    )
    run_sql("SELECT 1")

    assert callable(stage_batch)
    assert client.requested == [CONFIG.snowflake_secret_name]
    assert connect.calls[0]["user"] == USER
    assert connect.calls[0]["account"] == ACCOUNT
    assert connect.connections[0].executed == ["SELECT 1"]


def test_sinks_require_an_account(pem):
    client = _FakeSecretsClient({"user": USER, "private_key": pem})
    config = replace(CONFIG, snowflake_account="")

    with pytest.raises(ValueError, match="SNOWFLAKE_ACCOUNT"):
        build_snowflake_sinks(config, secrets_client=client, connect=_FakeConnect())
