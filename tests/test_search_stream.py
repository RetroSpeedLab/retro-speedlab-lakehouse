import json
from datetime import UTC, datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.lake.catalog import CHAT_PULSE, LAB_STATUS, POLICY_PULSE, SEARCH_EVENTS, SEARCH_PULSE
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.stream import search_rows
from retro_speedlab_lakehouse.stream.rows import KAFKA_ROW
from retro_speedlab_lakehouse.stream.search import search_batch


def runner_event(topic: str, key: str, payload: dict[str, Any], at: datetime) -> dict[str, Any]:
    return {"topic": topic, "key": key, "at": at.isoformat(), "payload": {**payload, "at": at.isoformat()}}


def events(now: datetime) -> list[dict[str, Any]]:
    game = {"game": "comet_courier"}
    return [
        runner_event("runs.attempts", "comet_courier", {**game, "outcome": "crossing", "frames": 90}, now),
        runner_event("runs.attempts", "comet_courier", {**game, "outcome": "nothing"}, now - timedelta(minutes=30)),
        runner_event("runs.attempts", "comet_courier", {**game, "outcome": "crossing"}, now - timedelta(hours=2)),
        runner_event("runs.steps", "comet_courier", {**game, "kind": "improved", "saved": 12}, now),
        runner_event("runs.steps", "comet_courier", {**game, "kind": "found", "saved": None}, now),
        runner_event("policy.rounds", "comet_courier", {**game, "round": 3, "reward": 10.0, "loss": 0.5}, now),
        runner_event("policy.rounds", "comet_courier", {**game, "round": 2, "reward": 6.0, "loss": 0.7}, now),
        runner_event(
            "lab.events",
            "comet_courier/2026-11-15T1935",
            {**game, "stamp": "2026-11-15T1935", "kind": "judging"},
            now,
        ),
        runner_event("chat.events", "retrospeedlab", {"channel": "retrospeedlab", "messages": 4, "chatters": 2}, now),
    ]


def test_the_search_stream_keeps_every_event_and_counts_the_last_hour(spark: SparkSession, settings: Settings) -> None:
    now = datetime.now(UTC)
    rows = [(json.dumps(item).encode("utf-8"), 0, offset, now) for offset, item in enumerate(events(now))]
    parsed = search_rows.parsed(spark.createDataFrame(rows, KAFKA_ROW))
    search_batch(settings, Gate("search"), parsed, 0)
    search_batch(settings, Gate("search"), parsed, 0)
    assert read(spark, SEARCH_EVENTS, settings.lake).count() == 9
    pulse = read(spark, SEARCH_PULSE, settings.lake).first()
    assert pulse is not None
    assert (pulse["attempts_last_hour"], pulse["crossings_last_hour"], pulse["frames_saved_last_hour"]) == (2, 1, 12)
    assert pulse["attempts_per_minute"] == 0.1 and pulse["improvements_last_hour"] == 1
    policy = read(spark, POLICY_PULSE, settings.lake).first()
    assert policy is not None and policy["round"] == 2 and policy["rounds_last_hour"] == 2
    lab = read(spark, LAB_STATUS, settings.lake).first()
    assert lab is not None and (lab["game"], lab["kind"]) == ("comet_courier", "judging")
    chat = read(spark, CHAT_PULSE, settings.lake).first()
    assert chat is not None and (chat["messages_last_minute"], chat["chatters_last_minute"]) == (4, 2)


def test_a_round_with_an_undefined_value_still_counts(spark: SparkSession, settings: Settings) -> None:
    now = datetime.now(UTC)
    payload = {"game": "saltskid", "round": 884, "reward": 1174.0, "explained_variance": float("nan")}
    rows = [(json.dumps(runner_event("policy.rounds", "saltskid", payload, now)).encode("utf-8"), 0, 0, now)]
    search_batch(settings, Gate("search"), search_rows.parsed(spark.createDataFrame(rows, KAFKA_ROW)), 0)
    policy = read(spark, POLICY_PULSE, settings.lake).first()
    assert policy is not None and (policy["round"], policy["reward"]) == (884, 1174.0)
