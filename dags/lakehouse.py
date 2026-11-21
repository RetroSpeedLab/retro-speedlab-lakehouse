from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule, chain

from retro_speedlab_lakehouse.alerts.airflow import FAILED, SUCCESS, TaskReporter
from retro_speedlab_lakehouse.batch.night import NIGHT_END, NIGHT_START

CONFIG = "/lakehouse/config.yaml"
NICE = 15
START = datetime(2026, 11, 15)
RETRIES = 1
RETRY_DELAY = timedelta(minutes=5)
HOURLY_SCHEDULE = f"10 0-{NIGHT_START.hour - 1},{NIGHT_END.hour}-23 * * *"
HOURLY_JOBS = ("api-hourly", "silver", "gold")
HOURLY_ASIDE = ("sources-hourly", "engagement-hourly", "tracking-hourly")
NIGHTLY_JOBS = (
    "api-nightly",
    "silver",
    "gold",
    "experiments",
    "search-allocation",
    "search-bandit",
    "engagement-nightly",
    "experiments-export",
    "platform",
)
NIGHTLY_ASIDE = ("sources-nightly", "videos-nightly", "maintenance")
MINING_JOBS = ("mine-records", "verify-replays", "mining-gold")
HOURLY_PRIORITY = 10
NIGHTLY_PRIORITY = 3
MINING_PRIORITY = 2
NIGHTLY_ASIDE_PRIORITY = 1
TASK_TIMEOUT = timedelta(minutes=40)
NIGHTLY_TASK_TIMEOUT = timedelta(minutes=90)
MINING_TIMEOUT = timedelta(hours=2, minutes=30)
ABSOLUTE = "absolute"


def job(name: str, priority: int, timeout: timedelta, trigger_rule: TriggerRule) -> BashOperator:
    command = (
        f"nice -n {NICE} python -m retro_speedlab_lakehouse job {name} {CONFIG} "
        "--dag {{ dag.dag_id }} --run-id {{ run_id }}"
    )
    return BashOperator(
        task_id=name,
        bash_command=command,
        retries=RETRIES,
        retry_delay=RETRY_DELAY,
        trigger_rule=trigger_rule,
        priority_weight=priority,
        weight_rule=ABSOLUTE,
        execution_timeout=timeout,
        on_failure_callback=TaskReporter(Path(CONFIG), FAILED),
        on_success_callback=TaskReporter(Path(CONFIG), SUCCESS),
    )


def chained(
    dag_id: str,
    schedule: str,
    jobs: tuple[str, ...],
    aside: tuple[str, ...],
    timeout: timedelta,
    priority: int,
    aside_priority: int,
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
        tasks = [job(name, priority, task_timeout, TriggerRule.ALL_SUCCESS) for name in jobs[:-1]]
        last = job(jobs[-1], priority, task_timeout, TriggerRule.ALL_DONE)
        chain(*tasks, last)
        last.set_downstream([job(name, aside_priority, task_timeout, TriggerRule.ALL_DONE) for name in aside])
    return dag


hourly = chained(
    "lakehouse_hourly",
    HOURLY_SCHEDULE,
    HOURLY_JOBS,
    HOURLY_ASIDE,
    timedelta(minutes=115),
    HOURLY_PRIORITY,
    HOURLY_PRIORITY,
    TASK_TIMEOUT,
)
nightly = chained(
    "lakehouse_nightly",
    "30 2 * * *",
    NIGHTLY_JOBS,
    NIGHTLY_ASIDE,
    timedelta(hours=3, minutes=30),
    NIGHTLY_PRIORITY,
    NIGHTLY_ASIDE_PRIORITY,
    NIGHTLY_TASK_TIMEOUT,
)
mining = chained(
    "lakehouse_mining", "30 3 * * *", MINING_JOBS, (), MINING_TIMEOUT, MINING_PRIORITY, MINING_PRIORITY, MINING_TIMEOUT
)
