#!/usr/bin/env bash
set -euo pipefail

export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__DAGS_FOLDER=/lakehouse/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////lake/airflow/airflow.db
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__API__PORT=8080
export AIRFLOW__API__WORKERS=1
export AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME=14400
export AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR=True
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=/lake/airflow/logs

mkdir -p /lake/airflow/logs
airflow db migrate
airflow api-server &
airflow dag-processor &
airflow scheduler &
wait -n
exit 1
