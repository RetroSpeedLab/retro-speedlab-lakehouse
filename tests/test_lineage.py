import json
from datetime import UTC, datetime

from pyspark.sql import Row, SparkSession

from retro_speedlab_lakehouse.lake.catalog import GAME_SUMMARY, GAMES, LAB_RUNS, LINEAGE_EDGES, LINEAGE_NODES
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.lineage import observed
from retro_speedlab_lakehouse.lineage.events import COMPLETE, FAIL, LineageEvent
from retro_speedlab_lakehouse.lineage.graph import build_lineage, edges_of, kind_of, record_job_lineage
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings

START = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)


def test_event_is_open_lineage() -> None:
    event = LineageEvent(
        "lakehouse-hourly",
        "run-1",
        "gold",
        COMPLETE,
        START,
        ("silver.games", "service:backend"),
        ("gold.game_summary",),
    )
    body = json.loads(event.open_lineage)
    assert body["eventType"] == COMPLETE
    assert body["job"] == {"namespace": "lakehouse-hourly", "name": "gold"}
    assert body["inputs"] == [
        {"namespace": "lake", "name": "silver.games"},
        {"namespace": "service", "name": "backend"},
    ]
    assert body["run"]["runId"] == event.run


def test_graph_keeps_the_newest_finished_run_of_each_job() -> None:
    base = {
        "job": "gold",
        "event_type": COMPLETE,
        "event_time": START,
        "inputs": ["silver.games"],
        "outputs": ["gold.a"],
    }
    older = Row(**base)
    newer = Row(**{**base, "event_time": START.replace(hour=13), "outputs": ["gold.b"]})
    failed = Row(**{**base, "event_time": START.replace(hour=14), "event_type": FAIL, "outputs": ["gold.c"]})
    assert set(edges_of([older, newer, failed])) == {("silver.games", "gold.b", "gold")}
    assert kind_of("topic:runs.live") == "topic"
    assert kind_of("silver.games") == "silver"


def test_job_lineage_becomes_the_graph(spark: SparkSession, settings: Settings) -> None:
    observed.clear()
    observed.read_of(GAMES)
    observed.write_of(GAME_SUMMARY)
    observed.write_of(LAB_RUNS)
    gate = Gate("lineage")
    record_job_lineage(spark, settings, gate, "lakehouse-hourly", "run-1", "gold", False, START)
    build_lineage(spark, settings, gate, START)
    edges = {(row["source"], row["target"], row["job"]) for row in read(spark, LINEAGE_EDGES, settings.lake).collect()}
    nodes = {row["node"]: row["kind"] for row in read(spark, LINEAGE_NODES, settings.lake).collect()}
    assert ("silver.games", "gold.game_summary", "gold") in edges
    assert ("topic:runs.live", "live.live_run", "stream-live") in edges
    assert nodes["gold.game_summary"] == "gold"
    assert nodes["topic:runs.live"] == "topic"
    observed.clear()
