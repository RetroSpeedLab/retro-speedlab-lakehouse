from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.experiments.arms import Arm, SearchHour
from retro_speedlab_lakehouse.experiments.roi import NO_CHANGE, Effect, LabRun, Spend, lab_roi_rows
from retro_speedlab_lakehouse.experiments.sequential import Harm, harm
from retro_speedlab_lakehouse.lake.catalog import EVENTS, PROMOTION_ROI, SPLIT_COMPARISON
from retro_speedlab_lakehouse.lake.tables import append_rows, read
from retro_speedlab_lakehouse.promotion.roi import build_promotion_roi
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.catalog import HUMAN_DROPOFF
from retro_speedlab_lakehouse.sources.dropoff import umami_dropoff
from retro_speedlab_lakehouse.sources.job import ingest_sources
from retro_speedlab_lakehouse.sources.record import SnapshotSource
from retro_speedlab_lakehouse.sources.registry import DROPOFF
from retro_speedlab_lakehouse.sources.tempo import tempo_lab_costs
from retro_speedlab_lakehouse.stream import rows

from .conftest import event, kafka_rows

NOW = datetime(2026, 11, 17, 12, tzinfo=UTC)
UMAMI = "https://umami.test/api"
GAME = "comet_courier"
PER_GAME = {"campaign-ended": [{"value": GAME, "total": 7}], "campaign-won": [{"value": GAME, "total": 3}]}
LEVELS = [{"value": "0", "total": 4}, {"value": "2", "total": 3}]
SPLITS = [(GAME, 0, "Stage 1", 900, 600, 2), (GAME, 1, "Stage 2", 1200, 800, 2)]
SPLIT_COLUMNS = "game string, level_index int, level string, human_median_frames long, ai_frames long, humans long"


def umami_values(request: httpx.Request) -> httpx.Response:
    params = request.url.params
    if params["propertyName"] == "level":
        assert params["path"] == f"c./{GAME}/"
        return httpx.Response(200, json=LEVELS)
    return httpx.Response(200, json=PER_GAME[params["event"]])


@respx.mock
def test_human_dropoff_counts_who_reached_and_ended_each_level(spark: SparkSession, settings: Settings) -> None:
    respx.post(f"{UMAMI}/auth/login").respond(json={"token": "token"})
    respx.get(f"{UMAMI}/websites/games/event-data/values").mock(side_effect=umami_values)
    append_rows(SPLIT_COMPARISON, settings.lake, spark.createDataFrame(SPLITS, SPLIT_COLUMNS))
    dropoff = SnapshotSource("umami", DROPOFF, lambda now: umami_dropoff(settings.sources.umami, now))
    ingest_sources(spark, settings.lake, Gate("sources"), [dropoff], [dropoff], NOW)
    found = read(spark, HUMAN_DROPOFF, settings.lake).orderBy("level_index").collect()
    assert [(row["level_index"], row["reached"], row["ended"]) for row in found] == [(0, 10, 4), (1, 6, 0), (2, 6, 3)]
    assert [row["ended_share"] for row in found] == pytest.approx([0.4, 0.0, 0.5])
    assert {(row["runs"], row["won"]) for row in found} == {(10, 3)}
    assert [(row["level"], row["human_median_frames"], row["ai_frames"]) for row in found[:2]] == [
        ("Stage 1", 900, 600),
        ("Stage 2", 1200, 800),
    ]
    assert found[2]["humans"] == 0


@respx.mock
def test_each_lab_runs_scores_span_in_tempo_is_its_cost(settings: Settings) -> None:
    attributes = [
        {"key": "lab.stamp", "value": {"stringValue": "2026-11-15T1935"}},
        {"key": "score.steps", "value": {"doubleValue": 120}},
        {"key": "score.tokens", "value": {"doubleValue": 400_000}},
        {"key": "score.cost_usd", "value": {"doubleValue": 2.07}},
    ]
    span = {"startTimeUnixNano": "1763235300000000000", "attributes": attributes}
    route = respx.get("https://tempo.test/api/search").respond(json={"traces": [{"spanSets": [{"spans": [span]}]}]})
    costs = tempo_lab_costs(settings.sources.tempo, NOW)
    assert [(cost.stamp, cost.observations, cost.tokens, cost.cost_usd) for cost in costs] == [
        ("2026-11-15T1935", 120, 400_000, 2.07)
    ]
    assert 'name = "lab scores"' in route.calls.last.request.url.params["q"]


def arm(saved: list[int]) -> Arm:
    return Arm(
        [SearchHour(GAME, NOW + timedelta(hours=index), 40, 5, value, 0, 0) for index, value in enumerate(saved)]
    )


def test_a_clearly_harmful_change_is_flagged_after_a_few_hours() -> None:
    before = arm([100 + index % 5 for index in range(24)])
    flagged = harm(before, arm([index % 3 for index in range(24)]))
    assert flagged is not None and flagged.flagged
    assert flagged.after_hours is not None and flagged.after_hours < 24
    assert harm(before, arm([100 + index % 4 for index in range(24)])) == Harm(False, None)
    assert harm(before, arm([5])) is None


def test_lab_roi_counts_only_runs_with_cost_and_a_measured_effect() -> None:
    finished = datetime(2026, 11, 15, 20, tzinfo=UTC)
    runs = [
        LabRun(GAME, "2026-11-15T1900", "notes", finished, 1.0),
        LabRun(GAME, "2026-11-15T1935", "deployed", finished + timedelta(hours=1), 2.0),
        LabRun(GAME, "2026-11-16T1335", "deployed", finished + timedelta(days=1), 3.0),
        LabRun(GAME, "2026-11-16T1935", "rejected", finished + timedelta(days=1, hours=6), 1.5),
    ]
    spends = {stamp: Spend(10, 1000, cost) for stamp, cost in [("2026-11-15T1900", 1.0), ("2026-11-15T1935", 2.0)]}
    effects = {(GAME, "2026-11-15T1935"): Effect(30, 600.0, 300.0, 900.0, Harm(False, None))}
    found = lab_roi_rows(runs, spends | {"2026-11-16T1335": Spend(5, 500, 3.0)}, effects, NOW)
    columns = [(row[1], row[9], row[12], row[13], row[14], row[15]) for row in found]
    assert columns == [
        ("2026-11-15T1900", 0.0, 0.0, True, 1.0, 0.0),
        ("2026-11-15T1935", 600.0, 300.0, True, 3.0, 600.0),
        ("2026-11-16T1335", None, None, False, 3.0, 600.0),
        ("2026-11-16T1935", NO_CHANGE.frames, None, False, 3.0, 600.0),
    ]
    assert found[1][16] == pytest.approx(200.0)
    assert (found[1][17], found[2][17]) == (False, None)


def promotion_events() -> list[dict[str, Any]]:
    at = datetime(2026, 11, 16, 12, tzinfo=UTC)
    found: list[dict[str, Any]] = []
    for draft in range(3):
        key = f"record:{GAME}:{draft}"
        usage = {"event": key, "channels": ["bluesky", "mastodon"], "cost_usd": 0.002}
        draft_row = {"id": draft, "event": key, "channel": "bluesky", "variant": 0, "status": "published"}
        metric = {"draft_id": draft, "views": None, "likes": 10 + draft * 2, "subscribers_gained": None}
        found += [
            event(draft * 10 + 1, "promotion.usage", key, usage, at),
            event(draft * 10 + 2, "promotion.drafts", str(draft), {**draft_row, "status": "pending"}, at),
            event(draft * 10 + 3, "promotion.drafts", str(draft), draft_row, at),
            event(draft * 10 + 4, "promotion.metrics", str(draft), metric, at),
        ]
    return found


def test_promotion_roi_shares_the_claude_cost_and_gives_likes_per_dollar(
    spark: SparkSession, settings: Settings
) -> None:
    bronze = rows.bronze_events(rows.parsed(kafka_rows(spark, promotion_events())))
    append_rows(EVENTS, settings.lake, bronze)
    assert build_promotion_roi(spark, settings.lake, Gate("gold"), NOW) == 1
    (found,) = read(spark, PROMOTION_ROI, settings.lake).collect()
    assert (found["channel"], found["variant"], found["drafts"], found["likes"]) == ("bluesky", 0, 3, 36)
    assert found["cost_usd"] == pytest.approx(0.003)
    assert found["likes_per_dollar"] == pytest.approx(12 / 0.001)
    assert found["likes_lower"] < found["likes_per_dollar"] < found["likes_upper"]
    assert found["views_per_dollar"] is None
