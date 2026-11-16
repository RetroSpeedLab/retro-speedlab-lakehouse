from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule, chain

CONFIG = "/lakehouse/config.yaml"
NICE = 15
START = datetime(2026, 11, 15)
RETRIES = 1
RETRY_DELAY = timedelta(minutes=5)
HOURLY_JOBS = ("api-hourly", "silver", "gold", "vacuum-live", "platform")
HOURLY_ASIDE = ("sources-hourly", "engagement-hourly")
NIGHTLY_JOBS = (
    "api-nightly",
    "silver",
    "gold",
    "experiments",
    "search-allocation",
    "search-bandit",
    "engagement-nightly",
    "maintenance",
    "platform",
)
NIGHTLY_ASIDE = ("sources-nightly", "videos-nightly")
MINING_JOBS = ("mine-records", "verify-replays", "mining-gold", "platform")
LAST = "platform"
HOURLY_PRIORITY = 10
NIGHTLY_PRIORITY = 1
HOURLY_TASK_TIMEOUT = timedelta(minutes=25)
NIGHTLY_TASK_TIMEOUT = timedelta(minutes=40)
MINING_TIMEOUT = timedelta(hours=2, minutes=30)
ABSOLUTE = "absolute"


def job(name: str, priority: int, timeout: timedelta) -> BashOperator:
    command = (
        f"nice -n {NICE} python -m retro_speedlab_lakehouse job {name} {CONFIG} "
        "--dag {{ dag.dag_id }} --run-id {{ run_id }}"
    )
    return BashOperator(
        task_id=name,
        bash_command=command,
        retries=RETRIES,
        retry_delay=RETRY_DELAY,
        trigger_rule=TriggerRule.ALL_DONE if name == LAST else TriggerRule.ALL_SUCCESS,
        priority_weight=priority,
        weight_rule=ABSOLUTE,
        execution_timeout=timeout,
    )


def chained(
    dag_id: str,
    schedule: str,
    jobs: tuple[str, ...],
    aside: tuple[str, ...],
    timeout: timedelta,
    priority: int,
    task_timeout: timedelta,
) -> DAG:
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
        tasks = [job(name, priority, task_timeout) for name in jobs]
        chain(*tasks)
        tasks[-1].set_upstream([job(name, priority, task_timeout) for name in aside])
    return dag


hourly = chained(
    "lakehouse_hourly",
    "10 * * * *",
    HOURLY_JOBS,
    HOURLY_ASIDE,
    timedelta(minutes=55),
    HOURLY_PRIORITY,
    HOURLY_TASK_TIMEOUT,
)
nightly = chained(
    "lakehouse_nightly",
    "30 2 * * *",
    NIGHTLY_JOBS,
    NIGHTLY_ASIDE,
    timedelta(hours=3),
    NIGHTLY_PRIORITY,
    NIGHTLY_TASK_TIMEOUT,
)
mining = chained("lakehouse_mining", "30 3 * * *", MINING_JOBS, (), MINING_TIMEOUT, NIGHTLY_PRIORITY, MINING_TIMEOUT)
