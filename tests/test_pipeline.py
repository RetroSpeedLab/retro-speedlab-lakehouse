import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.batch.api import SNAPSHOT
from retro_speedlab_lakehouse.batch.gold import build_gold
from retro_speedlab_lakehouse.batch.maintenance import maintain, vacuum_live
from retro_speedlab_lakehouse.batch.platform import JobRun, build_platform, record_job
from retro_speedlab_lakehouse.batch.silver import build_silver
from retro_speedlab_lakehouse.lake.catalog import (
    API,
    CORE_TABLES,
    EVENTS,
    GAME_RUNS,
    GAME_SUMMARY,
    GAMES,
    HIGHSCORES,
    LAB_PROGRESS,
    LAB_RUNS,
    LEVEL_COSTS,
    LIVE_RUN,
    PLATFORM_JOBS,
    PLATFORM_TABLES,
    QUALITY_REPORTS,
    RECORDS_DAILY,
    REPORTS,
    RUN_VERSIONS,
    STREAM_STATUS,
)
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.lake.write import append
from retro_speedlab_lakehouse.quality.gate import Gate, QualityFailure
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.stream import rows
from retro_speedlab_lakehouse.stream.job import ensure_served, entities_batch, live_batch
from retro_speedlab_lakehouse.stream.status import StreamStatus
from tests.conftest import event, fixture, kafka_rows

START = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)


def run(seconds: float, won: bool) -> dict[str, Any]:
    speedrun = fixture("speedrun.json")
    return {**speedrun, "seconds": seconds, "won": won, "updated_at": START.isoformat()}


def events() -> list[dict[str, Any]]:
    lab_run = fixture("lab-runs.json")[0]
    report = {
        "id": 107,
        "game": "comet_courier",
        "name": "2026-11-14T2007.md",
        "headline": "Lab run saves 27.7 seconds",
        "lines": ["One", "Two"],
        "created": START.isoformat(),
        "updated": START.isoformat(),
    }
    highscore = {
        "id": 7,
        "game": "comet_courier",
        "version": "1.0",
        "name": "Ada",
        "frames": 9000,
        "set_at": "2026-11-15T12:00:00Z",
    }
    return [
        event(1, "runs.snapshots", "comet_courier", run(150.0, True), START),
        event(2, "runs.live", "comet_courier", run(144.6, True), START + timedelta(seconds=3)),
        event(
            3, "lab.runs", "comet_courier/2026-11-14T2007", {k: v for k, v in lab_run.items() if k != "progress"}, START
        ),
        event(4, "lab.reports", "comet_courier/2026-11-14T2007.md", report, START),
        event(5, "highscores.entries", "7", highscore, START),
        event(6, "runs.live", "comet_courier", run(144.0, True), START + timedelta(seconds=6)),
    ]


def api_snapshots(spark: SparkSession, settings: Settings) -> None:
    taken = [
        ("/games", None, 200, json.dumps(fixture("games.json")), START),
        ("/games/{slug}/lab-runs", "comet_courier", 200, json.dumps(fixture("lab-runs.json")), START),
    ]
    snapshots = spark.createDataFrame(taken, SNAPSHOT).withColumn("date", F.to_date("fetched_at"))
    append(API, settings.lake, snapshots, Gate("test"))


def stream_events(spark: SparkSession, settings: Settings) -> None:
    ensure_served(spark, settings)
    parsed = rows.parsed(kafka_rows(spark, events()))
    entities_batch(settings, Gate("entities"), parsed, 0)
    entities_batch(settings, Gate("entities"), parsed, 0)
    live_batch(settings, StreamStatus(settings, Gate("live")), parsed.where("topic = 'runs.live'"))


def test_stream_serves_the_newest_live_run_and_keeps_every_event(spark: SparkSession, settings: Settings) -> None:
    stream_events(spark, settings)
    live = read(spark, LIVE_RUN, settings.lake).collect()
    assert [(row["slot"], row["game"], row["event_id"]) for row in live] == [("live", "comet_courier", 6)]
    assert json.loads(live[0]["payload"])["seconds"] == 144.0
    assert read(spark, EVENTS, settings.lake).count() == 6
    assert read(spark, RUN_VERSIONS, settings.lake).count() == 3
    assert read(spark, GAME_RUNS, settings.lake).select("event_id", "seconds").collect()[0].asDict() == {
        "event_id": 6,
        "seconds": 144.0,
    }
    assert read(spark, REPORTS, settings.lake).first()["lines"] == ["One", "Two"]  # type: ignore[index]
    assert read(spark, LAB_RUNS, settings.lake).first()["outcome"] == "deployed"  # type: ignore[index]
    assert read(spark, HIGHSCORES, settings.lake).first()["frames"] == 9000  # type: ignore[index]
    status = read(spark, STREAM_STATUS, settings.lake).first()
    assert status is not None and status["events_per_minute"] == 2


def test_an_older_event_never_replaces_a_newer_run(spark: SparkSession, settings: Settings) -> None:
    stream_events(spark, settings)
    older = rows.parsed(kafka_rows(spark, [event(2, "runs.live", "comet_courier", run(200.0, False), START)]))
    live_batch(settings, StreamStatus(settings, Gate("live")), older)
    assert read(spark, LIVE_RUN, settings.lake).first()["event_id"] == 6  # type: ignore[index]


def test_silver_and_gold_answer_the_questions(spark: SparkSession, settings: Settings) -> None:
    stream_events(spark, settings)
    api_snapshots(spark, settings)
    build_silver(spark, settings.lake, Gate("silver"))
    assert read(spark, GAMES, settings.lake).count() == len(fixture("games.json"))
    assert read(spark, LAB_PROGRESS, settings.lake).collect()[0]["seconds"] == -2.5
    build_gold(spark, settings.lake, Gate("gold"), START + timedelta(hours=1))
    summary = read(spark, GAME_SUMMARY, settings.lake).where("game = 'comet_courier'").first()
    assert summary is not None
    assert summary["record_seconds"] == 144.0
    assert summary["levels_cleared"] == 6
    assert summary["lab_runs"] == 1 and summary["lab_deployed"] == 1
    assert summary["costliest_level"] == "Stage 4"
    assert summary["saved_since_yesterday_seconds"] is None
    costliest = read(spark, LEVEL_COSTS, settings.lake).where("costliest").collect()
    assert [row["level"] for row in costliest] == ["Stage 4"]
    records = read(spark, RECORDS_DAILY, settings.lake).collect()
    assert [(row["record_seconds"], row["saved_seconds"]) for row in records] == [(144.0, 0.0)]


def test_platform_measures_tables_and_records_jobs(spark: SparkSession, settings: Settings) -> None:
    stream_events(spark, settings)
    gate = Gate("platform")
    record_job(spark, settings.lake, gate, JobRun("hourly", "run-1", "silver", START, START, "success", 3, None))
    build_platform(spark, CORE_TABLES, settings.lake, gate, START)
    tables = {row["table"]: row for row in read(spark, PLATFORM_TABLES, settings.lake).collect()}
    assert tables["bronze.events"]["rows"] == 6 and tables["bronze.events"]["store"] == "black-omen"
    assert tables["live.live_run"]["store"] == "lake"
    assert read(spark, PLATFORM_JOBS, settings.lake).count() == 1
    assert maintain(spark, CORE_TABLES, settings.lake, START) >= 8
    assert vacuum_live(spark, CORE_TABLES, settings.lake) == 2


def test_the_gate_blocks_a_failing_write_and_keeps_its_report(spark: SparkSession, settings: Settings) -> None:
    gate = Gate("test")
    duplicated = rows.parsed(
        kafka_rows(
            spark,
            [
                event(1, "runs.live", "comet_courier", run(1.0, False), START),
                event(1, "runs.live", "comet_courier", run(1.0, False), START),
            ],
        )
    )
    with pytest.raises(QualityFailure, match="silver.run_versions"):
        append(RUN_VERSIONS, settings.lake, rows.run_versions(duplicated), gate)
    gate.flush(spark, settings.lake)
    report = read(spark, QUALITY_REPORTS, settings.lake).first()
    assert report is not None and report["status"] == "FAIL" and report["table"] == "silver.run_versions"
