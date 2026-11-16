import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.engagement.analysis import AB, HOLDOUT, MISMATCH, Arms, judged_all, rate_finding
from retro_speedlab_lakehouse.engagement.channel import channel_outcomes
from retro_speedlab_lakehouse.engagement.events import engagement_events, umami_events
from retro_speedlab_lakehouse.engagement.posterior import BETA, GAMMA, Totals, posteriors, ranked
from retro_speedlab_lakehouse.engagement.ranking import RANKING, ranking_rows, surface_totals
from retro_speedlab_lakehouse.engagement.stream import chat_minutes, exposure_rows, twitch_minutes
from retro_speedlab_lakehouse.engagement.umami import hours_to_load
from retro_speedlab_lakehouse.experiments.analysis import BETTER, TOO_FEW
from retro_speedlab_lakehouse.settings import Settings

NOW = datetime(2026, 11, 16, 12, 5, tzinfo=UTC)
FLOOR = 0.3
EVENT = StructType(
    [
        StructField("event_id", LongType(), True),
        StructField("topic", StringType(), False),
        StructField("key", StringType(), False),
        StructField("event_at", TimestampType(), False),
        StructField("payload", StringType(), False),
    ]
)
RECORD = StructType([StructField("body", StringType(), False)])


def games(clicks: dict[str, int]) -> list[Totals]:
    return [Totals(game, 1000.0, float(clicked)) for game, clicked in clicks.items()]


def test_the_most_engaging_item_leads_most_and_every_item_keeps_its_floor() -> None:
    found = posteriors(games({"comet_courier": 90, "drumfort": 40, "sumspire": 10}), BETA, 10)
    order = ranked(found, FLOOR, 4000, np.random.default_rng(1))
    shares = {item.posterior.totals.item: item.share for item in order}

    assert sum(shares.values()) == pytest.approx(1.0)
    assert max(shares, key=lambda game: shares[game]) == "comet_courier"
    assert min(shares.values()) >= FLOOR / 3
    assert [item.rank for item in order] == [1, 2, 3]
    assert all(item.posterior.interval()[0] < item.posterior.mean < item.posterior.interval()[1] for item in order)


def test_without_data_every_item_leads_about_equally_often() -> None:
    found = posteriors(games(dict.fromkeys(("a", "b", "c", "d"), 0)), BETA, 10)
    rng = np.random.default_rng(2)
    leaders = Counter(
        next(item.posterior.totals.item for item in ranked(found, FLOOR, 500, rng) if item.sampled_rank == 1)
        for _ in range(800)
    )
    assert set(leaders) == {"a", "b", "c", "d"}
    assert min(leaders.values()) > 120


def test_a_count_posterior_learns_messages_per_minute() -> None:
    found = posteriors([Totals("run", 600.0, 1200.0), Totals("search", 600.0, 300.0)], GAMMA, 10)
    assert found[0].mean == pytest.approx(2.0, rel=0.02)
    assert found[0].interval()[0] > found[1].interval()[1]


def test_umami_hours_load_once_and_catch_up_at_most_the_backfill() -> None:
    assert hours_to_load(None, NOW, 2) == [
        datetime(2026, 11, 16, 10, tzinfo=UTC),
        datetime(2026, 11, 16, 11, tzinfo=UTC),
    ]
    assert hours_to_load("2026-11-16T12:00:00+00:00", NOW, 2) == []


def events(spark: SparkSession, rows: list[tuple[str, str, datetime, dict[str, Any]]]) -> DataFrame:
    found = [(index, topic, key, at, json.dumps(payload)) for index, (topic, key, at, payload) in enumerate(rows)]
    return spark.createDataFrame(found, EVENT)


def exposure(item: str, game: str, start: str, end: str) -> dict[str, Any]:
    return {
        "surface": "stream_scenes",
        "item": item,
        "game": game,
        "arm": "adaptive",
        "started_at": f"2026-11-16T{start}Z",
        "ended_at": f"2026-11-16T{end}Z",
        "variants": {"title_style": "challenge"},
    }


def chat(minute: str, messages: int) -> tuple[str, str, datetime, dict[str, Any]]:
    payload = {"channel": "retrospeedlab", "minute": f"2026-11-16T{minute}:00+00:00", "messages": messages}
    return ("chat.events", "retrospeedlab", NOW, payload)


def twitch(minute: str, viewers: int, followers: int) -> tuple[str, str, datetime, dict[str, Any]]:
    payload = {"minute": f"2026-11-16T{minute}:00+00:00", "live": True, "viewers": viewers, "followers": followers}
    return ("twitch.minutes", "retrospeedlab", NOW, payload)


def channel(shown: DataFrame, searched: DataFrame) -> DataFrame:
    return channel_outcomes(exposure_rows(shown), twitch_minutes(searched), chat_minutes(searched))


def test_exposures_are_joined_with_the_chat_minutes_they_overlap(spark: SparkSession) -> None:
    shown = events(
        spark, [("stream.exposures", "stream_scenes", NOW, exposure("run", "drumfort", "10:00:30", "10:02:00"))]
    )
    heard = chat_minutes(events(spark, [chat("10:00", 4), chat("10:01", 6), chat("10:02", 50)]).drop("event_id"))
    records = spark.createDataFrame([], RECORD)
    found = {
        (row["surface"], row["item"]): (row["exposures"], row["outcomes"])
        for row in engagement_events(records, shown, heard, channel(shown, shown.drop("event_id")), NOW, 28).collect()
    }
    assert found[("stream_scenes", "run")] == pytest.approx((1.5, 8.0))
    assert found[("stream_games", "drumfort")] == pytest.approx((1.5, 8.0))


def test_umami_cells_count_views_and_clicks_per_item_and_arm(spark: SparkSession) -> None:
    def body(event: str, cells: dict[str, int]) -> tuple[str]:
        return (json.dumps({"site": "www", "event": event, "hour": "2026-11-16T10:00:00+00:00", "cells": cells}),)

    records = spark.createDataFrame(
        [
            body("rank-view", {"home_games|adaptive|drumfort": 40, "home_games|bogus|drumfort": 3}),
            body("rank-click", {"home_games|adaptive|drumfort": 5, "home_games|fixed|sumspire": 2}),
        ],
        RECORD,
    )
    found = {(row["arm"], row["item"]): (row["exposures"], row["outcomes"]) for row in umami_events(records).collect()}
    assert found == {("adaptive", "drumfort"): (40.0, 5.0), ("fixed", "sumspire"): (2.0, 2.0)}


def test_promotion_posts_join_their_newest_numbers(spark: SparkSession) -> None:
    published = {"kind": "record", "published": "2026-11-16T09:10:00+00:00"}
    first = {"kind": "record", "likes": 1, "reposts": 0, "replies": 0}
    newest = {"kind": "record", "likes": 7, "reposts": 2, "replies": None}
    posted = events(
        spark,
        [
            ("promotion.drafts", "17", NOW, published),
            ("promotion.metrics", "17", NOW, first),
            ("promotion.metrics", "17", NOW, newest),
            ("promotion.metrics", "18", NOW, {"kind": "lab", "likes": 3}),
        ],
    )
    searched = posted.drop("event_id")
    found = engagement_events(
        spark.createDataFrame([], RECORD), posted, chat_minutes(searched), channel(posted, searched), NOW, 28
    )
    assert [(row["item"], row["exposures"], row["outcomes"]) for row in found.collect()] == [("record", 1.0, 9.0)]


def test_every_catalog_game_is_ranked_even_before_it_was_seen(spark: SparkSession, settings: Settings) -> None:
    seen = spark.createDataFrame(
        [("home_games", "drumfort", "fixed", NOW, 300.0, 30.0)],
        "surface string, item string, arm string, hour timestamp, exposures double, outcomes double",
    )
    totals = surface_totals(seen, ["drumfort", "sumspire", "comet_courier"], settings.engagement)
    rows = spark.createDataFrame(ranking_rows(totals, settings.engagement, NOW), RANKING).collect()
    assert sorted(row["item"] for row in rows if row["surface"] == "home_games") == [
        "comet_courier",
        "drumfort",
        "sumspire",
    ]
    assert sorted(row["sampled_rank"] for row in rows if row["surface"] == "home_games") == [1, 2, 3]
    assert {row["period"].astimezone(UTC) for row in rows} == {datetime(2026, 11, 16, 13, tzinfo=UTC)}


def test_a_clear_ab_result_names_a_winner_and_a_mismatch_never_does() -> None:
    arms = Arms("cta_copy", AB, "watch_live", "beat_the_ai", 0.5)
    clear = rate_finding(arms, "click_rate", True, (40, 1000), (90, 1000), 200)
    skewed = rate_finding(
        Arms("hero_text", AB, "current", "challenge", 0.5), "click_rate", True, (40, 1000), (90, 1400), 200
    )
    thin = rate_finding(
        Arms("holdout_home_games", HOLDOUT, "holdout", "adaptive", 0.12), "click_rate", True, (1, 20), (9, 150), 200
    )
    judged = {item.finding.arms.experiment: item for item in judged_all([clear, skewed, thin])}

    assert (judged["cta_copy"].verdict, judged["cta_copy"].winner) == (BETTER, "beat_the_ai")
    assert (judged["hero_text"].verdict, judged["hero_text"].winner) == (MISMATCH, None)
    assert (judged["holdout_home_games"].verdict, judged["holdout_home_games"].winner) == (TOO_FEW, None)


def test_the_holdout_share_is_checked_for_a_sample_ratio_mismatch() -> None:
    arms = Arms("holdout_home_games", HOLDOUT, "holdout", "adaptive", 0.12)
    assert rate_finding(arms, "click_rate", True, (12, 1200), (88, 8800), 200).srm_ok
    assert not rate_finding(arms, "click_rate", True, (50, 5000), (50, 5000), 200).srm_ok


def test_old_exposures_fall_out_of_the_window(spark: SparkSession) -> None:
    old = NOW - timedelta(days=40)
    shown = events(
        spark, [("stream.exposures", "stream_scenes", old, exposure("run", "drumfort", "10:00:00", "10:01:00"))]
    )
    searched = shown.drop("event_id")
    found = engagement_events(
        spark.createDataFrame([], RECORD), shown, chat_minutes(searched), channel(shown, searched), NOW, 28
    )
    assert found.count() == 0


def test_a_twitch_channel_turn_gets_its_viewers_followers_and_chat(spark: SparkSession) -> None:
    turn: dict[str, Any] = {
        **exposure("science_proof", "drumfort", "10:00:00", "10:03:00"),
        "surface": "twitch_channel",
        "variants": {},
    }
    shown = events(
        spark,
        [
            ("stream.exposures", "twitch_channel", NOW, turn),
            ("stream.exposures", "twitch_channel", NOW, turn),
            ("stream.exposures", "stream_scenes", NOW, exposure("run", "drumfort", "10:00:00", "10:03:00")),
        ],
    )
    searched = events(
        spark,
        [
            twitch("10:00", 10, 100),
            twitch("10:01", 20, 101),
            twitch("10:02", 30, 103),
            twitch("10:03", 90, 140),
            chat("10:00", 4),
            chat("10:01", 6),
            chat("10:02", 2),
        ],
    ).drop("event_id")
    outcomes = channel(shown, searched).collect()
    assert len(outcomes) == 1
    measured = outcomes[0].asDict()
    assert (measured["item"], measured["arm"], measured["game"]) == ("science_proof", "adaptive", "drumfort")
    assert measured["hours"] == pytest.approx(0.05)
    assert (measured["average_viewers"], measured["peak_viewers"]) == pytest.approx((20.0, 30.0))
    assert (measured["new_followers"], measured["followers_per_hour"]) == pytest.approx((3.0, 60.0))
    assert (measured["chat_messages"], measured["chat_per_viewer_hour"]) == pytest.approx((12.0, 12.0))

    found = {
        (row["surface"], row["item"]): (row["exposures"], row["outcomes"])
        for row in engagement_events(
            spark.createDataFrame([], RECORD), shown, chat_minutes(searched), channel(shown, searched), NOW, 28
        ).collect()
    }
    assert found[("twitch_channel", "science_proof")] == pytest.approx((0.05, 3.0))
    assert set(found) == {("twitch_channel", "science_proof"), ("stream_scenes", "run"), ("stream_games", "drumfort")}
