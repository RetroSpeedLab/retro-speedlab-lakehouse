from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from retro_speedlab_lakehouse.engagement.events import decision_events
from retro_speedlab_lakehouse.lake.catalog import EVENTS, PROMOTION_PREFERENCES
from retro_speedlab_lakehouse.lake.tables import append_rows, read
from retro_speedlab_lakehouse.promotion.preferences import build_promotion_preferences, promotion_decisions
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.stream import rows

from .conftest import event, kafka_rows

NOW = datetime(2026, 11, 17, 12, tzinfo=UTC)
DECIDED = NOW - timedelta(hours=2)
DRAFTS = [
    ("record", "bluesky", 0, {"text": "Stage 5 now takes 22.48 s, 8.6 s faster."}, "published"),
    ("record", "bluesky", 1, {"text": "Can you beat 22.48 s? The AI just did."}, "rejected"),
    ("lab", "bluesky", 0, {"text": "A lab run moved the search 3.5 s on!"}, "approved"),
    ("record", "youtube", 0, {"title": "8.6 s gone from Stage 5", "hook": "8.6 s gone"}, "failed"),
]


def promotion_events() -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for draft, (kind, channel, variant, copy, status) in enumerate(DRAFTS):
        drafted = {"id": draft, "event": f"{kind}:{draft}", "kind": kind, "game": "comet_courier", "channel": channel}
        drafted |= {"variant": variant, "copy": copy, "status": "pending", "decided": None}
        decided = {**drafted, "status": status, "decided": DECIDED.isoformat()}
        found += [
            event(draft * 10 + 1, "promotion.drafts", str(draft), drafted, NOW - timedelta(hours=3)),
            event(draft * 10 + 2, "promotion.drafts", str(draft), decided, DECIDED),
        ]
    pending = {"id": 9, "event": "lab:9", "kind": "lab", "channel": "bluesky", "variant": 0, "copy": {"text": "Soon."}}
    return [*found, event(91, "promotion.drafts", "9", {**pending, "status": "pending", "decided": None}, NOW)]


def bronze(spark: SparkSession) -> DataFrame:
    return rows.bronze_events(rows.parsed(kafka_rows(spark, promotion_events())))


def test_each_decision_keeps_its_text_and_text_features(spark: SparkSession) -> None:
    found = {row["draft"]: row for row in promotion_decisions(bronze(spark)).collect()}

    assert sorted(found) == ["0", "1", "2", "3"]
    features = [(row["approved"], row["text"], row["numbers"], row["opening"], row["tone"]) for row in found.values()]
    assert sorted(features, key=lambda feature: feature[1]) == [
        (True, "8.6 s gone from Stage 5", 2, "number", "plain"),
        (True, "A lab run moved the search 3.5 s on!", 1, "statement", "exclaims"),
        (False, "Can you beat 22.48 s? The AI just did.", 1, "question", "plain"),
        (True, "Stage 5 now takes 22.48 s, 8.6 s faster.", 3, "statement", "plain"),
    ]
    assert found["0"]["length"] == len(DRAFTS[0][3]["text"])


def test_preferences_count_every_decision_so_small_samples_stay_visible(
    spark: SparkSession, settings: Settings
) -> None:
    append_rows(EVENTS, settings.lake, bronze(spark))

    assert build_promotion_preferences(spark, settings.lake, Gate("gold"), NOW) > 0

    found = {
        (row["channel"], row["feature"], row["value"]): row
        for row in read(spark, PROMOTION_PREFERENCES, settings.lake).collect()
    }
    everything = found[("bluesky", "all", "all")]
    assert (everything["decisions"], everything["approved"], everything["rejected"]) == (3, 2, 1)
    assert everything["approval_rate"] == pytest.approx(2 / 3)
    assert everything["lower"] < everything["approval_rate"] < everything["upper"]
    record = found[("bluesky", "kind", "record")]
    assert (record["decisions"], record["approved"]) == (2, 1)
    assert found[("bluesky", "opening", "question")]["approval_rate"] == 0
    assert found[("bluesky", "variant", "1")]["decisions"] == 1
    assert found[("bluesky", "numbers", "3+")]["approved"] == 1
    assert found[("youtube", "length", "under 120")]["decisions"] == 1


def test_decisions_rank_each_networks_variants_by_approval(spark: SparkSession) -> None:
    found = decision_events(promotion_decisions(bronze(spark))).collect()

    tallied = {row["item"]: (row["exposures"], row["outcomes"]) for row in found}
    assert tallied == {"bluesky:0": (2.0, 2.0), "bluesky:1": (1.0, 0.0), "youtube:0": (1.0, 1.0)}
    assert {row["surface"] for row in found} == {"promotion_variants"}
