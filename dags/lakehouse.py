from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule, chain

CONFIG = "/lakehouse/config.yaml"
NICE = 15
START = datetime(2026, 11, 15)
RETRIES = 1
RETRY_DELAY = timedelta(minutes=5)
HOURLY_JOBS = ("api-hourly", "silver", "gold", "vacuum-live", "sources-hourly", "platform")
NIGHTLY_JOBS = (
    "api-nightly",
    "silver",
    "gold",
    "experiments",
    "maintenance",
    "sources-nightly",
    "videos-nightly",
    "platform",
)
INDEPENDENT = ("sources-hourly", "sources-nightly", "videos-nightly", "platform")
MINING_JOBS = ("mine-records", "verify-replays", "mining-gold", "platform")


def job(name: str) -> BashOperator:
    command = (
        f"nice -n {NICE} python -m retro_speedlab_lakehouse job {name} {CONFIG} "
        "--dag {{ dag.dag_id }} --run-id {{ run_id }}"
    )
    rule = TriggerRule.ALL_DONE if name in INDEPENDENT else TriggerRule.ALL_SUCCESS
    return BashOperator(task_id=name, bash_command=command, retries=RETRIES, retry_delay=RETRY_DELAY, trigger_rule=rule)


def chained(dag_id: str, schedule: str, jobs: tuple[str, ...], timeout: timedelta) -> DAG:
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=START,
        catchup=False,
        is_paused_upon_creation=False,
        max_active_runs=1,
        dagrun_timeout=timeout,
        tags=["lakehouse"],
    ) as dag:
        chain(*[job(name) for name in jobs])
    return dag


hourly = chained("lakehouse_hourly", "10 * * * *", HOURLY_JOBS, timedelta(minutes=40))
nightly = chained("lakehouse_nightly", "30 2 * * *", NIGHTLY_JOBS, timedelta(hours=3))
mining = chained("lakehouse_mining", "30 3 * * *", MINING_JOBS, timedelta(hours=2, minutes=30))
