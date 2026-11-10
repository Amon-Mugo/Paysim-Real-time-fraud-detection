#!/usr/bin/env bash
# Start the fraud/AML detection job on EMR Serverless.
# Usage: scripts/start_emr_job.sh <msk-bootstrap-brokers>

set -euo pipefail

readonly REGION="us-east-1"
readonly APPLICATION_ID="00ga3j0459b0sp09"
readonly EXECUTION_ROLE_ARN="arn:aws:iam::011294328070:role/paysim-emr-serverless-execution"
readonly CURATED_BUCKET="s3://paysim-pipeline-curated-amonmugo"
readonly JOB_NAME="paysim-fraud-detection"
readonly MODEL_ALIAS="models"

usage() {
  echo "Usage: $0 <msk-bootstrap-brokers>" >&2
  exit 1
}

# Print one driver and one executor --conf pair for every NAME=VALUE argument.
build_env_conf() {
  local pair name value
  for pair in "$@"; do
    name="${pair%%=*}"
    value="${pair#*=}"
    printf -- '--conf spark.emr-serverless.driverEnv.%s=%s ' "${name}" "${value}"
    printf -- '--conf spark.executorEnv.%s=%s ' "${name}" "${value}"
  done
}

build_spark_submit_parameters() {
  local brokers="$1"
  local env_conf
  env_conf="$(build_env_conf \
    "SPARK_DOWNLOAD_PACKAGES=false" \
    "ICEBERG_CATALOG_TYPE=glue" \
    "ICEBERG_WAREHOUSE=${CURATED_BUCKET}/iceberg/warehouse/" \
    "ICEBERG_DATABASE=paysim_fraud_db" \
    "KAFKA_BOOTSTRAP_SERVERS=${brokers}" \
    "KAFKA_SECURITY_PROTOCOL=SASL_SSL" \
    "AWS_REGION=us-east-1" \
    "SNOWFLAKE_ACCOUNT=DPEJXQE-YR77482" \
    "PYSPARK_PYTHON=/opt/venv/bin/python" \
    "PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python" \
    "DETECTION_CHECKPOINT_LOCATION=${CURATED_BUCKET}/checkpoints/detection" \
    "DETECTION_MODEL_DIR=./${MODEL_ALIAS}/stub-v1")"
  echo "--py-files ${CURATED_BUCKET}/code/spark.zip" \
    "--archives ${CURATED_BUCKET}/models/fraud/stub-v1.zip#${MODEL_ALIAS}" \
    "--jars /usr/lib/spark/connector/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/connector/lib/spark-token-provider-kafka-0-10.jar,/usr/share/aws/iceberg/lib/iceberg-spark-runtime-4.0_2.13-1.10.1-amzn-0.jar --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false" \
    "${env_conf}"
}

build_job_driver() {
  local parameters="$1"
  printf '{"sparkSubmit":{"entryPoint":"%s/code/job.py","sparkSubmitParameters":"%s"}}' \
    "${CURATED_BUCKET}" "${parameters}"
}

build_monitoring_configuration() {
  printf '{"monitoringConfiguration":{"cloudWatchLoggingConfiguration":{"enabled":true,"logGroupName":"/aws/emr-serverless/%s"}}}' \
    "${JOB_NAME}"
}

main() {
  [[ $# -eq 1 ]] || usage
  local brokers="$1"
  local parameters
  parameters="$(build_spark_submit_parameters "${brokers}")"

  aws emr-serverless start-job-run \
    --region "${REGION}" \
    --application-id "${APPLICATION_ID}" \
    --execution-role-arn "${EXECUTION_ROLE_ARN}" \
    --name "${JOB_NAME}" \
    --job-driver "$(build_job_driver "${parameters}")" \
    --configuration-overrides "$(build_monitoring_configuration)" \
    --query jobRunId \
    --output text
}

main "$@"