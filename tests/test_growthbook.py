import json
from datetime import UTC, datetime

import httpx
import respx
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from retro_speedlab_lakehouse.growthbook.api import sync_experiments
from retro_speedlab_lakehouse.growthbook.engagement import site_tests
from retro_speedlab_lakehouse.growthbook.promotion import promotion_variants
from retro_speedlab_lakehouse.growthbook.search import SEARCH
from retro_speedlab_lakehouse.settings import Settings

RECORD = StructType([StructField("body", StringType(), False)])
HOUR = "2026-11-16T10:00:00+00:00"
API = "http://growthbook.test:3100/api/v1"


def body(event: str, cells: dict[str, int]) -> tuple[str]:
    return (json.dumps({"site": "www", "event": event, "hour": HOUR, "cells": cells}),)


def test_every_view_of_a_site_test_is_one_unit_and_its_goals_convert_that_many(
    spark: SparkSession, settings: Settings
) -> None:
    records = spark.createDataFrame(
        [
            body("variant-view", {"cta_copy|watch_live": 3, "cta_copy|beat_the_ai": 1, "cta_copy|bogus": 9}),
            body("variant-goal", {"cta_copy|watch_live|click": 1, "cta_copy|beat_the_ai|click": 2}),
        ],
        RECORD,
    )
    family = site_tests(records, settings.engagement)

    rows = family.units.collect()
    units = {(row["variation"], row["unit"]) for row in rows}
    clicks = {(row["variation"], row["unit"]) for row in rows if row["value"] == 1.0}
    assert [experiment.key for experiment in family.experiments] == ["cta_copy"]
    assert sorted(variation for variation, _ in units) == ["beat_the_ai", "beat_the_ai", *["watch_live"] * 3]
    assert sorted(variation for variation, _ in clicks) == ["beat_the_ai", "beat_the_ai", "watch_live"]


def test_each_promotion_draft_is_a_unit_of_its_network_with_its_approval(spark: SparkSession) -> None:
    decided = datetime(2026, 11, 18, tzinfo=UTC)
    decisions = spark.createDataFrame(
        [("d1", "bluesky", 0, True, decided, 4), ("d2", "bluesky", 1, False, decided, None)],
        "draft string, channel string, variant int, approved boolean, decided_at timestamp, engagement_24h long",
    )
    family = promotion_variants(decisions)

    found = {(row["unit"], row["metric"]): row["value"] for row in family.units.collect()}
    assert [experiment.key for experiment in family.experiments] == ["promotion_variants_bluesky"]
    assert found == {
        ("promotion:d1", "approved"): 1.0,
        ("promotion:d1", "interactions_24h"): 4.0,
        ("promotion:d2", "approved"): None,
        ("promotion:d2", "interactions_24h"): None,
    }


@respx.mock
def test_a_missing_experiment_is_created_with_its_split_and_every_one_is_refreshed(settings: Settings) -> None:
    respx.get(f"{API}/experiments").mock(return_value=httpx.Response(200, json={"experiments": []}))
    created = respx.post(f"{API}/experiments").mock(
        return_value=httpx.Response(200, json={"experiment": {"id": "exp_1"}})
    )
    refreshed = respx.post(f"{API}/experiments/exp_1/snapshot").mock(return_value=httpx.Response(200, json={}))
    started = datetime(2026, 11, 1, tzinfo=UTC)

    assert sync_experiments(settings.growthbook, [SEARCH], {SEARCH.key: started}) == 1
    sent = json.loads(created.calls.last.request.content)
    assert sent["trackingKey"] == "search_bandit" and sent["datasourceId"] == "lake"
    assert sent["phases"][0]["trafficSplit"] == [
        {"variationId": "holdout", "weight": 0.2},
        {"variationId": "bandit", "weight": 0.8},
    ]
    assert refreshed.called


@respx.mock
def test_an_experiment_without_units_is_not_created(settings: Settings) -> None:
    assert sync_experiments(settings.growthbook, [SEARCH], {}) == 0
