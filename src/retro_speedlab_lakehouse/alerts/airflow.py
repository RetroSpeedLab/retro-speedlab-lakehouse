from pathlib import Path
from typing import Any

from airflow.sdk.definitions.context import Context

from retro_speedlab_lakehouse.relay.outbox import backend_client
from retro_speedlab_lakehouse.settings import load_backend_settings

TASKS_PATH = "/alerts/airflow"
TIMEOUT_SECONDS = 15.0
ERROR_LENGTH = 500
SUCCESS = "success"
FAILED = "failed"
NO_EXCEPTION = "Airflow failed the task without an exception: it never started, or a restart or timeout killed it"


def task_error(state: str, context: Context) -> str | None:
    if "exception" in context:
        return str(context["exception"])[:ERROR_LENGTH]
    return NO_EXCEPTION if state == FAILED else None


def task_report(state: str, context: Context) -> dict[str, Any]:
    if "ti" not in context or "dag" not in context:
        raise ValueError("Airflow passed no task instance or DAG to the callback")
    ti = context["ti"]
    return {
        "dag": ti.dag_id,
        "task": ti.task_id,
        "run_id": ti.run_id,
        "state": state,
        "tries": ti.try_number,
        "error": task_error(state, context),
        "dag_tasks": sorted(context["dag"].task_ids),
    }


class TaskReporter:
    def __init__(self, config: Path, state: str) -> None:
        self.config = config
        self.state = state

    def __call__(self, context: Context) -> None:
        backend = load_backend_settings(self.config)
        with backend_client(backend.url, backend.api_key, TIMEOUT_SECONDS) as client:
            client.post(TASKS_PATH, json=task_report(self.state, context)).raise_for_status()
