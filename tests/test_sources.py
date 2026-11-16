from datetime import UTC, datetime, timedelta

import httpx
import respx
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.batch.platform import build_platform
from retro_speedlab_lakehouse.lake.catalog import GAMES, SOURCES_STATUS
from retro_speedlab_lakehouse.lake.tables import append_rows, read
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.catalog import GAME_PLAYS, SOURCE_NUMBERS, SOURCE_TABLES
from retro_speedlab_lakehouse.sources.job import SourcesFailed, ingest_sources
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.plays import umami_plays
from retro_speedlab_lakehouse.sources.record import SnapshotSource
from retro_speedlab_lakehouse.sources.registry import NUMBERS, PLAYS
from retro_speedlab_lakehouse.sources.uptime import uptime_numbers

NOW = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
PAGE = {
    "publicGroupList": [
        {
            "monitorList": [
                {"id": 9, "name": "retrospeedlab - Website"},
                {"id": 11, "name": "retrospeedlab - Stream"},
            ]
        }
    ]
}
BEATS = {
    "heartbeatList": {"9": [{"status": 1, "ping": 67}], "11": []},
    "uptimeList": {"9_24": 0.9986, "11_24": 0},
}
UMAMI = "https://umami.test/api"
PER_GAME = {
    "campaign-started": [{"value": "comet_courier", "total": 3}],
    "campaign-won": [{"value": "comet_courier", "total": 1}],
    "endless-started": [{"value": "comet_courier", "total": 2}],
}
COLLECTION = [("comet_courier", 1, "Comet Courier", "1.2.0"), ("blinkwisp", 2, "Blinkwisp", "1.2.1")]


def failing(now: datetime) -> list[Number]:
    raise httpx.ConnectError("unreachable")


def per_game(request: httpx.Request) -> httpx.Response:
    event = request.url.params["event"]
    return httpx.Response(200, json=PER_GAME[event] if event in PER_GAME else [])


@respx.mock
def test_uptime_keeps_only_monitors_with_heartbeats(settings: Settings) -> None:
    respx.get("https://uptime.test/api/status-page/retrospeedlab").respond(json=PAGE)
    respx.get("https://uptime.test/api/status-page/heartbeat/retrospeedlab").respond(json=BEATS)
    numbers = uptime_numbers(settings.sources.uptime)
    assert [(number.label, number.value, number.unit) for number in numbers] == [
        ("Website uptime", 99.86, "%"),
        ("Website response", 67, "ms"),
    ]


def test_a_failing_source_keeps_the_others_and_their_gold_numbers(spark: SparkSession, settings: Settings) -> None:
    gate = Gate("sources")
    first = SnapshotSource("discord", NUMBERS, lambda now: [Number("members", "Members", 4, "", "on the server")])
    later = SnapshotSource("discord", NUMBERS, lambda now: [Number("members", "Members", 5, "", "on the server")])
    ingest_sources(spark, settings.lake, gate, [first], NOW)
    try:
        ingest_sources(spark, settings.lake, gate, [later, SnapshotSource("twitch", NUMBERS, failing)], NOW)
    except SourcesFailed as error:
        assert "twitch" in str(error)
    numbers = read(spark, SOURCE_NUMBERS, settings.lake).collect()
    assert [(row["source"], row["metric"], row["value"]) for row in numbers] == [("discord", "members", 5.0)]
    build_platform(spark, SOURCE_TABLES, settings.lake, gate, NOW + timedelta(hours=1))
    status = read(spark, SOURCES_STATUS, settings.lake).collect()
    assert [(row["source"], row["last_records"], row["total_records"]) for row in status] == [("discord", 1, 2)]


@respx.mock
def test_human_plays_reach_gold_for_every_game_mode_and_window(spark: SparkSession, settings: Settings) -> None:
    respx.post(f"{UMAMI}/auth/login").respond(json={"token": "token"})
    respx.get(f"{UMAMI}/websites/games/event-data/values").mock(side_effect=per_game)
    append_rows(
        GAMES, settings.lake, spark.createDataFrame(COLLECTION, "slug string, number int, title string, version string")
    )
    plays = SnapshotSource("umami", PLAYS, lambda now: umami_plays(settings.sources.umami, now))
    ingest_sources(spark, settings.lake, Gate("sources"), [plays], NOW)
    rows = read(spark, GAME_PLAYS, settings.lake).collect()
    counted = {(row["game"], row["mode"], row["window"]): (row["started"], row["won"]) for row in rows}
    assert len(counted) == len(COLLECTION) * 2 * 3
    assert counted[("comet_courier", "campaign", "all_time")] == (3, 1)
    assert counted[("comet_courier", "endless", "last_day")] == (2, 0)
    assert counted[("blinkwisp", "campaign", "last_week")] == (0, 0)
