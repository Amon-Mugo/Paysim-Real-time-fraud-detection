"""M8 replay-and-evaluate DAG for the fraud and AML pipeline.

Discovers the live MSK cluster, then runs the PaySim generator as a Fargate
task that replays steps after the training cutoff and uploads ground truth to
S3. Later tasks (EMR job, landing wait, evaluation, cleanup) attach below.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.sdk import DAG, Param, task
from cryptography.hazmat.primitives import serialization
import snowflake.connector

AWS_REGION = "us-east-1"
MSK_CLUSTER_NAME = "paysim-fraud-pipeline"
ECS_CLUSTER = "paysim-pipeline-generator"
GENERATOR_TASK_DEFINITION = "paysim-pipeline-generator"
GENERATOR_CONTAINER = "generator"
DEFAULT_MIN_STEP = 521
EMR_APPLICATION_NAME = "paysim-emr"
EMR_EXECUTION_ROLE_NAME = "paysim-emr-serverless-execution"
EMR_JOB_NAME = "paysim-fraud-detection"
EMR_LOG_GROUP = f"/aws/emr-serverless/{EMR_JOB_NAME}"
EMR_ENDED_STATES = {"FAILED", "CANCELLING", "CANCELLED", "SUCCESS"}
EMR_ACTIVE_STATES = ["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING"]
EMR_JARS = (
    "/usr/lib/spark/connector/lib/spark-sql-kafka-0-10.jar,"
    "/usr/lib/spark/connector/lib/spark-token-provider-kafka-0-10.jar,"
    "/usr/share/aws/iceberg/lib/iceberg-spark-runtime-4.0_2.13-1.10.1-amzn-0.jar"
)
EMR_RESOURCE_CONF = (
    "--conf spark.driver.cores=1 --conf spark.driver.memory=4g "
    "--conf spark.executor.cores=1 --conf spark.executor.memory=4g "
    "--conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false"
)
MODEL_ALIAS = "models"
RUNNING_POLL_SECONDS = 20
RUNNING_TIMEOUT_SECONDS = 20 * 60
SNOWFLAKE_SECRET_ID = "paysim-fraud-pipeline/snowflake"
SNOWFLAKE_ROLE = "FRAUD_WRITER"
SNOWFLAKE_WAREHOUSE = "FRAUD_WH"
SNOWFLAKE_DATABASE = "FRAUD_DETECTION"
SNOWFLAKE_SCHEMA = "MONITORING"
LANDING_TABLE = "TRANSACTIONS_RECENT"
LANDING_POLL_SECONDS = 60
LANDING_STABLE_POLLS = 3
LANDING_TIMEOUT_SECONDS = 60 * 60

logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Environment variable {name} is not set; add it to .env")
    return value


def _notify_failure(context: dict) -> None:
    task_instance = context["task_instance"]
    logger.error(
        "Task failed: dag=%s task=%s run=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
    )


SUBNET_IDS = _require_env("FRAUD_SUBNET_IDS").split(",")
SECURITY_GROUP_ID = _require_env("FRAUD_SECURITY_GROUP_ID")
RAW_BUCKET = _require_env("FRAUD_RAW_BUCKET")
CURATED_BUCKET = _require_env("FRAUD_CURATED_BUCKET")
CURATED_URI = f"s3://{CURATED_BUCKET}"
SNOWFLAKE_ACCOUNT = _require_env("FRAUD_SNOWFLAKE_ACCOUNT")


@task
def check_msk() -> str:
    """Fail fast unless the MSK cluster is ACTIVE; return its IAM bootstrap brokers."""
    client = boto3.client("kafka", region_name=AWS_REGION)
    clusters = client.list_clusters_v2(ClusterNameFilter=MSK_CLUSTER_NAME)[
        "ClusterInfoList"
    ]
    matches = [c for c in clusters if c["ClusterName"] == MSK_CLUSTER_NAME]
    if not matches:
        raise AirflowFailException(
            f"MSK cluster {MSK_CLUSTER_NAME} not found; apply it first"
        )
    cluster = matches[0]
    if cluster["State"] != "ACTIVE":
        raise AirflowFailException(
            f"MSK cluster state is {cluster['State']}, expected ACTIVE"
        )
    response = client.get_bootstrap_brokers(ClusterArn=cluster["ClusterArn"])
    return response["BootstrapBrokerStringSaslIam"]


def _build_env_conf(env: dict[str, str]) -> str:
    """Return a driver and an executor --conf pair for every environment variable."""
    pairs = []
    for name, value in env.items():
        pairs.append(f"--conf spark.emr-serverless.driverEnv.{name}={value}")
        pairs.append(f"--conf spark.executorEnv.{name}={value}")
    return " ".join(pairs)


def _build_spark_submit_parameters(
    brokers: str, curated_uri: str, snowflake_account: str, run_suffix: str
) -> str:
    env_conf = _build_env_conf(
        {
            "SPARK_DOWNLOAD_PACKAGES": "false",
            "ICEBERG_CATALOG_TYPE": "glue",
            "ICEBERG_WAREHOUSE": f"{curated_uri}/iceberg/warehouse/",
            "ICEBERG_DATABASE": "paysim_fraud_db",
            "KAFKA_BOOTSTRAP_SERVERS": brokers,
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "AWS_REGION": AWS_REGION,
            "SNOWFLAKE_ACCOUNT": snowflake_account,
            "PYSPARK_PYTHON": "/opt/venv/bin/python",
            "PYSPARK_DRIVER_PYTHON": "/opt/venv/bin/python",
            "DETECTION_CHECKPOINT_LOCATION": f"{curated_uri}/checkpoints/detection/eval_{run_suffix}",
            "DETECTION_MODEL_DIR": f"./{MODEL_ALIAS}/stub-v1",
        }
    )
    return " ".join(
        [
            f"--py-files {curated_uri}/code/spark.zip",
            f"--archives {curated_uri}/models/fraud/stub-v1.zip#{MODEL_ALIAS}",
            f"--jars {EMR_JARS}",
            EMR_RESOURCE_CONF,
            env_conf,
        ]
    )


def _find_emr_application_id(client) -> str:
    paginator = client.get_paginator("list_applications")
    for page in paginator.paginate():
        for application in page["applications"]:
            if application["name"] == EMR_APPLICATION_NAME:
                return application["id"]
    raise AirflowFailException(
        f"EMR Serverless application {EMR_APPLICATION_NAME} not found"
    )


def _execution_role_arn() -> str:
    account_id = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()[
        "Account"
    ]
    return f"arn:aws:iam::{account_id}:role/{EMR_EXECUTION_ROLE_NAME}"


def _wait_for_running(client, application_id: str, job_run_id: str) -> None:
    deadline = time.monotonic() + RUNNING_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        job_run = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)[
            "jobRun"
        ]
        state = job_run["state"]
        if state == "RUNNING":
            return
        if state in EMR_ENDED_STATES:
            details = job_run.get("stateDetails", "")
            raise AirflowFailException(
                f"EMR job run {job_run_id} ended in {state}: {details}"
            )
        logger.info("EMR job run %s is %s", job_run_id, state)
        time.sleep(RUNNING_POLL_SECONDS)
    client.cancel_job_run(applicationId=application_id, jobRunId=job_run_id)
    raise AirflowFailException(
        f"EMR job run {job_run_id} did not reach RUNNING within {RUNNING_TIMEOUT_SECONDS} seconds"
    )


@task
def start_emr_job(brokers: str, run_suffix: str) -> dict[str, str]:
    """Start the streaming job on EMR Serverless and return once the run is RUNNING."""
    client = boto3.client("emr-serverless", region_name=AWS_REGION)
    application_id = _find_emr_application_id(client)
    parameters = _build_spark_submit_parameters(
        brokers, CURATED_URI, SNOWFLAKE_ACCOUNT, run_suffix
    )
    response = client.start_job_run(
        applicationId=application_id,
        executionRoleArn=_execution_role_arn(),
        name=EMR_JOB_NAME,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"{CURATED_URI}/code/job.py",
                "sparkSubmitParameters": parameters,
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "cloudWatchLoggingConfiguration": {
                    "enabled": True,
                    "logGroupName": EMR_LOG_GROUP,
                }
            }
        },
    )
    job_run_id = response["jobRunId"]
    logger.info("Started EMR job run %s on application %s", job_run_id, application_id)
    _wait_for_running(client, application_id, job_run_id)
    return {"application_id": application_id, "job_run_id": job_run_id}


def _snowflake_connect_kwargs() -> dict:
    secret = boto3.client("secretsmanager", region_name=AWS_REGION).get_secret_value(
        SecretId=SNOWFLAKE_SECRET_ID
    )
    credentials = json.loads(secret["SecretString"])
    passphrase = credentials.get("private_key_passphrase")
    private_key = serialization.load_pem_private_key(
        credentials["private_key"].encode(),
        password=passphrase.encode() if passphrase else None,
    )
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return {
        "account": SNOWFLAKE_ACCOUNT,
        "user": credentials["user"],
        "private_key": private_key_der,
        "role": SNOWFLAKE_ROLE,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
    }


def _count_landed_rows(connection, seconds_since_start: int) -> int:
    query = f"SELECT COUNT(*) FROM {LANDING_TABLE} WHERE INGESTED_AT >= DATEADD(second, -%s, CURRENT_TIMESTAMP())"
    with connection.cursor() as cursor:
        cursor.execute(query, (seconds_since_start,))
        return cursor.fetchone()[0]


def _get_job_started_at(application_id: str, job_run_id: str) -> datetime:
    client = boto3.client("emr-serverless", region_name=AWS_REGION)
    job_run = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)[
        "jobRun"
    ]
    return job_run["createdAt"]


@task
def wait_for_landing(emr_job: dict[str, str]) -> dict[str, int | str]:
    """Wait until the rows landed in Snowflake since the EMR job started stop growing."""
    started_at = _get_job_started_at(emr_job["application_id"], emr_job["job_run_id"])
    deadline = time.monotonic() + LANDING_TIMEOUT_SECONDS
    last_count = -1
    stable_polls = 0
    connection = snowflake.connector.connect(**_snowflake_connect_kwargs())
    try:
        while time.monotonic() < deadline:
            elapsed = int((datetime.now(timezone.utc) - started_at).total_seconds())
            count = _count_landed_rows(connection, elapsed)
            logger.info("Rows landed since job start: %s", count)
            stable_polls = stable_polls + 1 if count > 0 and count == last_count else 0
            last_count = count
            if stable_polls >= LANDING_STABLE_POLLS:
                return {"row_count": count, "job_started_at": started_at.isoformat()}
            time.sleep(LANDING_POLL_SECONDS)
    finally:
        connection.close()
    raise AirflowFailException(
        f"Landing did not stabilise within {LANDING_TIMEOUT_SECONDS} seconds (last count {last_count})"
    )


@task(trigger_rule="all_done")
def cancel_emr_job() -> list[str]:
    """Cancel any active run of the streaming job so the EMR application can stop."""
    client = boto3.client("emr-serverless", region_name=AWS_REGION)
    application_id = _find_emr_application_id(client)
    cancelled = []
    paginator = client.get_paginator("list_job_runs")
    for page in paginator.paginate(
        applicationId=application_id, states=EMR_ACTIVE_STATES
    ):
        for job_run in page["jobRuns"]:
            if job_run["name"] == EMR_JOB_NAME:
                client.cancel_job_run(
                    applicationId=application_id, jobRunId=job_run["id"]
                )
                cancelled.append(job_run["id"])
    logger.info("Cancelled EMR job runs: %s", cancelled)
    return cancelled


with DAG(
    dag_id="fraud_replay_eval",
    description="Replay PaySim through the pipeline on AWS and evaluate detections",
    schedule=None,
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={"min_step": Param(DEFAULT_MIN_STEP, type="integer", minimum=1)},
    default_args={"retries": 0, "on_failure_callback": _notify_failure},
    tags=["fraud-aml", "m8"],
) as dag:
    brokers = check_msk()

    run_generator = EcsRunTaskOperator(
        task_id="run_generator",
        cluster=ECS_CLUSTER,
        task_definition=GENERATOR_TASK_DEFINITION,
        launch_type="FARGATE",
        region_name=AWS_REGION,
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": SUBNET_IDS,
                "securityGroups": [SECURITY_GROUP_ID],
                "assignPublicIp": "DISABLED",
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": GENERATOR_CONTAINER,
                    "environment": [
                        {
                            "name": "KAFKA_BOOTSTRAP_SERVERS",
                            "value": "{{ ti.xcom_pull(task_ids='check_msk') }}",
                        },
                        {"name": "KAFKA_IAM_AUTH", "value": "true"},
                        {"name": "AWS_REGION", "value": AWS_REGION},
                        {
                            "name": "GENERATOR_CSV_S3_URI",
                            "value": f"s3://{RAW_BUCKET}/paysim/paysim_dataset.csv",
                        },
                        {
                            "name": "GENERATOR_GROUND_TRUTH_S3_URI",
                            "value": f"s3://{RAW_BUCKET}/ground-truth/eval_{{{{ ts_nodash }}}}.jsonl",
                        },
                        {
                            "name": "GENERATOR_MIN_STEP",
                            "value": "{{ params.min_step }}",
                        },
                    ],
                }
            ]
        },
        wait_for_completion=True,
    )

    emr_job = start_emr_job(brokers=brokers, run_suffix="{{ ts_nodash }}")

    landing = wait_for_landing(emr_job=emr_job)

    cancel = cancel_emr_job()

    brokers >> run_generator >> emr_job >> landing >> cancel
