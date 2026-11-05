# used in esyablishing a secure connections between the user and the snowflake accout
# also takes the foreachbatch defined in snowflake_merge as connections

from __future__ import annotations
import json
import boto3
import base64  # used for cryptogrphy of sting types length
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from pyspark.sql import DataFrame
from spark.config import DetectionConfig
from spark.snowflake_merge import RunSql, StageBatch

SPARK_SNOWFLAKE_FORMAT = (
    "net.snowflake.spark.snowflake"  # format required for spark-snowflake connection
)
_REQUIRED_SECRET_KEYS = ("user", "private_key")  # secret payload being fetched from aws
Connect = Callable[..., Any]


@dataclass(frozen=True)
class SnowflakeCredentials:
    user: str  # the name of snowflake username
    private_key_der: bytes = field(
        repr=False
    )  # used to ensure stored private key is in raw DER byte
    # repr=false prevents stings representations due to security


def _require_account(config: DetectionConfig) -> str:
    if not config.snowflake_account:
        raise ValueError("SNOWFLAKE_ACCOUNT must be set at land data in snowflake")
    return config.snowflake_account


def _der_from_pem(pem: str, passphrase: str | None) -> bytes:
    key = serialization.load_pem_private_key(
        pem.encode("utf-8"),
        password=passphrase.encode("utf-8") if passphrase else None,
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,  # encodes the output as DER bytes
        format=serialization.PrivateFormat.PKCS8,  # the standard syntax for string private key information
        encryption_algorithm=serialization.NoEncryption(),  # due to the snowflake connections requiremnets
    )


# used in retriving the credentials and validating them
def load_credentials(
    secret_name: str, secrets_client: Any | None = None
) -> SnowflakeCredentials:
    client = secrets_client or boto3.client("secretsmanager")
    # uses the secrets_clients for testing if there is any or default to the aws boto3.client
    response = client.get_secret_value(SecretId=secret_name)
    # used to retrive the secrets
    payload = json.loads(response["SecretString"])
    missing = [key for key in _REQUIRED_SECRET_KEYS if not payload.get(key)]
    if missing:
        raise ValueError(f"secret{secret_name} is missing keys {missing}")

    return SnowflakeCredentials(
        user=payload["user"],
        private_key_der=_der_from_pem(
            payload["private_key"], payload.get("private_key_passphrase")
        ),
    )


# used in building the configuration for spark streaming of the snowflake and credentilas
def build_spark_options(
    config: DetectionConfig, credentials: SnowflakeCredentials
) -> dict[str, str]:

    account = _require_account(config)  # check if account follows the proper configs
    return {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": credentials.user,
        "pem_private_key": base64.b64encode(credentials.private_key_der).decode(
            "ascii"
        ),
        "sfDatabase": config.snowflake_database,
        "sfSchema": config.snowflake_schema,
        "sfWarehouse": config.snowflake_warehouse,
        "sfRole": config.snowflake_role,
    }


# used for short lived keys credentials in python databse to execute admin sql statements like
# merge and trancate
def build_connect_kwargs(
    config: DetectionConfig, credentials: SnowflakeCredentials
) -> dict[str, Any]:
    return {
        "account": _require_account(config),
        "user": credentials.user,
        "private_key": credentials.private_key_der,
        "database": config.snowflake_database,
        "schema": config.snowflake_schema,
        "warehouse": config.snowflake_warehouse,
        "role": config.snowflake_role,
        "enable_server_session_keep_alive_auto_detection": False,
    }


# used to write the credentials and the data into the snowflake table
def make_stage_batch(options: dict[str, str]) -> StageBatch:
    # options reperesents the url,credentials,privatekey,warehouse
    def stage_batch(batch: DataFrame, staging_table: str) -> None:
        (
            batch.write.format(
                SPARK_SNOWFLAKE_FORMAT
            )  # used format the required connections
            .options(**options)  # used to unpak the credentials,url,role,etc
            .option("dbtable", staging_table)
            .option(
                "column_mapping", "name"
            )  # maps the requied column names to snowflake table columns
            .mode("append")
            .save()
        )

    return stage_batch


# uses the snowflake connector to open short lived connections to execute sql statements
def make_run_sql(connect_kwargs: dict[str, Any], connect: Connect) -> RunSql:
    def run_sql(statement: str) -> None:
        connection = connect(**connect_kwargs)  # for opening the connection
        try:
            with connection.cursor() as cursor:  # opens up databse
                cursor.execute(statement)
        finally:
            connection.close()

    return run_sql


# the main orchestration point
def build_snowflake_sinks(
    config: DetectionConfig,
    secrets_client: Any | None = None,
    connect: Connect = snowflake.connector.connect,
) -> tuple[StageBatch, RunSql]:
    credentials = load_credentials(config.snowflake_secret_name, secrets_client)
    stage_batch = make_stage_batch(build_spark_options(config, credentials))
    run_sql = make_run_sql(build_connect_kwargs(config, credentials), connect)

    return stage_batch, run_sql
