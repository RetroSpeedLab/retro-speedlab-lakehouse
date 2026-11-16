import json
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx
import pytest
import respx
from airflow.sdk.definitions.context import Context
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.alerts.airflow import FAILED, task_report
from retro_speedlab_lakehouse.cockpit.catalog import AUDIENCE_HOURS, COCKPIT_DAYS, SEARCH_HOURS
from retro_speedlab_lakehouse.cockpit.job import build_cockpit
from retro_speedlab_lakehouse.lake.catalog import (
    HOST_METRICS,
    HOST_STATUS,
    QUALITY_FAILURES,
    QUALITY_REPORTS,
    RUN_VERSIONS,
)
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.lake.write import append
from retro_speedlab_lakehouse.quality.gate import Gate, QualityFailure
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.langfuse import langfuse_days
from retro_speedlab_lakehouse.stream import rows, search_rows
from retro_speedlab_lakehouse.stream.job import ensure_served, entities_batch
from retro_speedlab_lakehouse.stream.rows import KAFKA_ROW
from retro_speedlab_lakehouse.stream.search import search_batch

from .conftest import event, kafka_rows, source_settings

NOW = datetime(2026, 11, 16, 12, 30, tzinfo=UTC)


def runner_event(topic: str, key: str, payload: dict[str, Any], at: datetime) -> dict[str, Any]:
    return {"topic": topic, "key": key, "at": at.isoformat(), "payload": payload}


def search_events(now: datetime) -> list[dict[str, Any]]:
    yesterday = now - timedelta(hours=30)
    return [
        runner_event("runs.records", "comet_courier", {"frames": 900}, now - timedelta(minutes=5)),
        runner_event("runs.records", "comet_courier", {"frames": 950}, yesterday),
        runner_event("runs.steps", "comet_courier", {"kind": "improved", "saved": 12}, now - timedelta(minutes=5)),
        runner_event(
            "policy.rounds", "comet_courier", {"round": 1, "reward": 10.0, "loss": 0.5}, now - timedelta(minutes=5)
        ),
        runner_event(
            "twitch.minutes", "retrospeedlab", {"live": True, "viewers": 7, "followers": 40}, now - timedelta(minutes=5)
        ),
        runner_event("chat.events", "retrospeedlab", {"messages": 4, "chatters": 2}, now - timedelta(minutes=5)),
    ]


def test_the_cockpit_compares_the_last_24_hours_with_the_day_before(spark: SparkSession, settings: Settings) -> None:
    now = datetime.now(UTC)
    kafka = [(json.dumps(item).encode("utf-8"), 0, offset, now) for offset, item in enumerate(search_events(now))]
    search_batch(settings, Gate("search"), search_rows.parsed(spark.createDataFrame(kafka, KAFKA_ROW)), 0)
    assert build_cockpit(spark, settings.lake, Gate("cockpit"), now) > 0
    days = {
        (row["metric"], row["days_ago"]): row["value"] for row in read(spark, COCKPIT_DAYS, settings.lake).collect()
    }
    assert (days[("Records", 0)], days[("Records", 1)], days[("Records", 7)]) == (1.0, 1.0, 0.0)
    assert (days[("Frames saved", 0)], days[("Peak viewers", 0)], days[("Twitch followers", 0)]) == (12.0, 7.0, 40.0)
    assert days[("Peak viewers", 1)] is None
    hour = read(spark, SEARCH_HOURS, settings.lake).where("records = 1").orderBy("hour").collect()[-1]
    assert (hour["frames_saved"], hour["ppo_rounds"], hour["ppo_reward_sum"]) == (12.0, 1, 10.0)
    audience = read(spark, AUDIENCE_HOURS, settings.lake).orderBy("hour").collect()[-1]
    assert (audience["viewers_peak"], audience["chat_messages"]) == (7.0, 4.0)


def host_payload(disk: float, at: datetime) -> dict[str, Any]:
    return {
        "host": "main",
        "measured_at": at.isoformat(),
        "disk_percent": disk,
        "fullest_disk": "root",
        "memory_percent": 41.0,
        "memory_used_bytes": 4,
        "memory_total_bytes": 10,
        "load_1": 1.0,
        "load_5": 2.0,
        "load_15": 3.0,
        "cpus": 8,
    }


def test_host_metrics_land_live_and_in_history(spark: SparkSession, settings: Settings) -> None:
    ensure_served(spark, settings)
    reports = [
        event(1, "hosts.metrics", "main", host_payload(80.0, NOW), NOW),
        event(2, "hosts.metrics", "main", host_payload(91.5, NOW + timedelta(minutes=1)), NOW + timedelta(minutes=1)),
    ]
    entities_batch(settings, Gate("entities"), rows.parsed(kafka_rows(spark, reports)), 0)
    live = read(spark, HOST_STATUS, settings.lake).collect()
    assert [(row["host"], row["disk_percent"], row["cpus"]) for row in live] == [("main", 91.5, 8)]
    assert read(spark, HOST_METRICS, settings.lake).count() == 2


def test_a_failed_gate_also_lands_in_the_quality_failures(spark: SparkSession, settings: Settings) -> None:
    gate = Gate("test")
    duplicated = [event(1, "runs.live", "comet_courier", {"seconds": 1.0}, NOW)] * 2
    with pytest.raises(QualityFailure):
        append(RUN_VERSIONS, settings.lake, rows.run_versions(rows.parsed(kafka_rows(spark, duplicated))), gate)
    gate.flush(spark, settings.lake)
    assert read(spark, QUALITY_REPORTS, settings.lake).count() == 1
    failure = read(spark, QUALITY_FAILURES, settings.lake).first()
    assert failure is not None and failure["table"] == "silver.run_versions"


@respx.mock
def test_langfuse_costs_are_loaded_per_utc_day() -> None:
    totals = {"data": [{"count_count": 3, "sum_totalTokens": 900, "sum_totalCost": 0.25}]}
    route = respx.get("https://langfuse.test/api/public/v2/metrics").mock(return_value=httpx.Response(200, json=totals))
    days = langfuse_days(source_settings().langfuse, NOW)
    assert [day.date.isoformat() for day in days[:2]] == ["2026-11-16", "2026-11-15"]
    assert days[0].record.body == {"date": "2026-11-16", "observations": 3, "tokens": 900, "cost_usd": 0.25}
    request: httpx.Request = route.calls[0].request
    first = json.loads(request.url.params["query"])
    assert (first["fromTimestamp"], first["toTimestamp"]) == ("2026-11-16T00:00:00Z", "2026-11-16T12:30:00Z")


class TaskInstance:
    dag_id = "lakehouse_hourly"
    task_id = "gold"
    run_id = "scheduled__2026-11-16"
    try_number = 2


def test_a_failed_task_reports_its_error() -> None:
    context = cast(Context, {"ti": TaskInstance(), "exception": RuntimeError("gold.game_summary failed")})
    assert task_report(FAILED, context) == {
        "dag": "lakehouse_hourly",
        "task": "gold",
        "run_id": "scheduled__2026-11-16",
        "state": "failed",
        "tries": 2,
        "error": "gold.game_summary failed",
    }
