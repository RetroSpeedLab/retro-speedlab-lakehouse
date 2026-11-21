import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.lake.catalog import GAMES, SOURCES_STATUS
from retro_speedlab_lakehouse.lake.tables import append_rows, read
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.aim import aim_numbers
from retro_speedlab_lakehouse.sources.bluesky import bluesky_numbers, bluesky_posts
from retro_speedlab_lakehouse.sources.catalog import (
    BLUESKY_POSTS,
    GAME_PLAYS,
    ITCH_DAILY,
    SOURCE_NUMBERS,
    YOUTUBE_DAILY,
)
from retro_speedlab_lakehouse.sources.gold import bluesky_posts as posts_gold
from retro_speedlab_lakehouse.sources.http import ATTEMPTS, get_json
from retro_speedlab_lakehouse.sources.itch import itch_days
from retro_speedlab_lakehouse.sources.job import SourcesFailed, ingest_sources
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.plays import umami_plays
from retro_speedlab_lakehouse.sources.record import SnapshotSource
from retro_speedlab_lakehouse.sources.registry import DAILY, NUMBERS, PLAYS, POSTS, hourly_sources, nightly_sources
from retro_speedlab_lakehouse.sources.streamelements import streamelements_numbers
from retro_speedlab_lakehouse.sources.uptime import uptime_numbers
from retro_speedlab_lakehouse.sources.youtube import youtube_days, youtube_numbers

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
    "level-cleared": [{"value": "comet_courier", "total": 4}],
}
PATHS = [
    {"x": "/html/123/index.html", "y": 5},
    {"x": "/html/123/comet_courier/index.html", "y": 2},
    {"x": "/html/124/comet_courier/index.html#again", "y": 1},
]
FEED = "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed"
CREATED = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)


def feed_post(key: str, likes: int, created: str) -> dict[str, Any]:
    record = {"text": f"Post {key}", "createdAt": created}
    counts = {"likeCount": likes, "repostCount": 1, "replyCount": 2, "quoteCount": 3}
    return {"post": {"uri": f"at://did:plc:lab/app.bsky.feed.post/{key}", "record": record, **counts}}


def feed(request: httpx.Request) -> httpx.Response:
    if "cursor" not in request.url.params:
        shared = {**feed_post("other", 99, "2026-11-13T09:00:00Z"), "reason": {"$type": "reasonRepost"}}
        return httpx.Response(
            200, json={"feed": [feed_post("3m5a", 4, "2026-11-14T12:00:00.000Z"), shared], "cursor": "c"}
        )
    return httpx.Response(200, json={"feed": [feed_post("3m5b", 0, "2026-11-14T10:30:00Z")]})


COLLECTION = [("comet_courier", 1, "Comet Courier", "1.2.0"), ("blinkwisp", 2, "Blinkwisp", "1.2.1")]


def failing(now: datetime) -> list[Number]:
    raise httpx.ConnectError("unreachable")


def per_game(request: httpx.Request) -> httpx.Response:
    event = request.url.params["event"]
    return httpx.Response(200, json=PER_GAME[event] if event in PER_GAME else [])


@respx.mock
def test_aim_counts_the_training_runs_of_every_experiment(settings: Settings) -> None:
    respx.get("https://aim.test/api/experiments/").respond(json=[{"run_count": 3}, {"run_count": 2}])

    assert aim_numbers(settings.sources.aim) == [Number("policy_runs", "PPO training runs", 5, "", "all time")]


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
    with pytest.raises(SourcesFailed, match=r"these failed: twitch\.numbers \(ConnectError\)$"):
        ingest_sources(spark, settings.lake, gate, [later, SnapshotSource("twitch", NUMBERS, failing)], NOW)
    numbers = read(spark, SOURCE_NUMBERS, settings.lake).collect()
    assert [(row["source"], row["metric"], row["value"]) for row in numbers] == [("discord", "members", 5.0)]
    status = read(spark, SOURCES_STATUS, settings.lake).collect()
    assert [(row["source"], row["last_records"], row["total_records"]) for row in status] == [("discord", 1, 2)]


@respx.mock
def test_a_refused_source_names_its_status_but_never_its_url(spark: SparkSession, settings: Settings) -> None:
    respx.get("https://www.googleapis.com/youtube/v3/channels").respond(503)

    def refused(now: datetime) -> list[Number]:
        with httpx.Client() as session:
            session.get("https://www.googleapis.com/youtube/v3/channels", params={"key": "secret"}).raise_for_status()
        return []

    with pytest.raises(SourcesFailed) as failure:
        ingest_sources(spark, settings.lake, Gate("sources"), [SnapshotSource("youtube", NUMBERS, refused)], NOW)
    assert str(failure.value) == "Kept the other sources; these failed: youtube.numbers (HTTP 503)"


@respx.mock
def test_human_plays_reach_gold_for_every_game_mode_and_window(spark: SparkSession, settings: Settings) -> None:
    respx.post(f"{UMAMI}/auth/login").respond(json={"token": "token"})
    respx.get(f"{UMAMI}/websites/games/event-data/values").mock(side_effect=per_game)
    append_rows(
        GAMES, settings.lake, spark.createDataFrame(COLLECTION, "slug string, number int, title string, version string")
    )
    plays = SnapshotSource("umami", PLAYS, lambda now: umami_plays(settings.sources.umami, "games", now))
    ingest_sources(spark, settings.lake, Gate("sources"), [plays], NOW)
    rows = read(spark, GAME_PLAYS, settings.lake).collect()
    counted = {(row["game"], row["mode"], row["window"]): (row["started"], row["won"]) for row in rows}
    assert len(counted) == len(COLLECTION) * 2 * 3
    assert counted[("comet_courier", "campaign", "all_time")] == (3, 1)
    assert counted[("comet_courier", "endless", "last_day")] == (2, 0)
    assert counted[("blinkwisp", "campaign", "last_week")] == (0, 0)


@respx.mock
def test_itch_days_reach_gold_per_game_and_day(spark: SparkSession, settings: Settings) -> None:
    respx.post(f"{UMAMI}/auth/login").respond(json={"token": "token"})
    respx.get(f"{UMAMI}/websites/itch/stats").respond(json={"pageviews": 8, "visitors": 3})
    respx.get(f"{UMAMI}/websites/itch/metrics").respond(json=PATHS)
    respx.get(f"{UMAMI}/websites/itch/event-data/values").mock(side_effect=per_game)
    append_rows(
        GAMES, settings.lake, spark.createDataFrame(COLLECTION, "slug string, number int, title string, version string")
    )
    days = SnapshotSource("itch", DAILY, lambda now: itch_days(settings.sources.umami, now))
    ingest_sources(spark, settings.lake, Gate("sources"), [days], NOW)
    rows = read(spark, ITCH_DAILY, settings.lake).collect()
    counted = {(row["game"], row["date"].isoformat()): (row["views"], row["campaign"], row["cleared"]) for row in rows}
    assert len(counted) == len(COLLECTION) * 2
    today = datetime.now(UTC).date()
    assert counted[("comet_courier", today.isoformat())] == (3, 3, 4)
    assert counted[("blinkwisp", (today - timedelta(days=1)).isoformat())] == (0, 0, 0)


def rate_limited(retry_after: str) -> httpx.Response:
    return httpx.Response(429, headers={"Retry-After": retry_after})


@pytest.fixture
def waits(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)
    return slept


@respx.mock
def test_a_rate_limit_waits_as_long_as_the_server_asks(waits: list[float]) -> None:
    route = respx.get("https://api.test/numbers").mock(side_effect=[rate_limited("30"), httpx.Response(200, json=[1])])
    with httpx.Client(base_url="https://api.test") as session:
        assert get_json(session, "/numbers", {}) == [1]
    assert (route.call_count, waits) == (2, [30.0])


@respx.mock
def test_a_rate_limit_that_persists_fails(waits: list[float]) -> None:
    route = respx.get("https://api.test/numbers").mock(return_value=rate_limited("1"))
    with httpx.Client(base_url="https://api.test") as session, pytest.raises(httpx.HTTPStatusError):
        get_json(session, "/numbers", {})
    assert (route.call_count, len(waits)) == (ATTEMPTS, ATTEMPTS - 1)


@respx.mock
def test_a_rate_limit_longer_than_a_run_can_wait_fails_at_once(waits: list[float]) -> None:
    route = respx.get("https://api.test/numbers").mock(return_value=rate_limited("3600"))
    with httpx.Client(base_url="https://api.test") as session, pytest.raises(httpx.HTTPStatusError):
        get_json(session, "/numbers", {})
    assert (route.call_count, waits) == (1, [])


@respx.mock
def test_langfuse_is_asked_twice_an_hour_and_ten_times_a_night(settings: Settings) -> None:
    data = {"data": [{"count_count": 1, "sum_totalTokens": 1, "sum_totalCost": 0.1, "sessionId": "lab-run 1"}]}
    route = respx.get("https://langfuse.test/api/public/v2/metrics").respond(json=data)
    for sources, calls in ((hourly_sources, 2), (nightly_sources, 12)):
        for source in sources(settings.sources):
            if source.name == "langfuse":
                source.fetch(None, NOW)
        assert route.call_count == calls


@respx.mock
def test_bluesky_keeps_the_accounts_own_posts_of_every_page(spark: SparkSession, settings: Settings) -> None:
    route = respx.get(FEED).mock(side_effect=feed)
    posts = SnapshotSource("bluesky", POSTS, lambda now: bluesky_posts(settings.sources.bluesky))
    ingest_sources(spark, settings.lake, Gate("sources"), [posts], NOW)
    rows = {row["post"]: row for row in read(spark, BLUESKY_POSTS, settings.lake).collect()}
    assert sorted(rows) == ["3m5a", "3m5b"]
    assert (rows["3m5a"]["text"], rows["3m5a"]["likes"], rows["3m5a"]["engagement"]) == ("Post 3m5a", 4, 10)
    assert route.calls[0].request.url.params["actor"] == "retrospeedlab.bsky.social"
    assert route.calls[1].request.url.params["cursor"] == "c"


@respx.mock
def test_bluesky_counts_the_followers_and_the_likes_of_the_accounts_own_posts(settings: Settings) -> None:
    respx.get("https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile").respond(json={"followersCount": 12})
    respx.get(FEED).mock(side_effect=feed)
    numbers = {number.metric: number.value for number in bluesky_numbers(settings.sources.bluesky)}
    assert numbers == {"followers": 12, "likes": 4}


def test_bluesky_engagement_at_a_day_is_the_first_count_after_24_hours(spark: SparkSession) -> None:
    measured = [(2, 23), (5, 25), (9, 27), (30, 200)]
    history = spark.createDataFrame(
        [
            ("at://lab/post/a", "A", CREATED, likes, 0, 0, 0, CREATED + timedelta(hours=hours))
            for likes, hours in measured
        ]
        + [("at://lab/post/b", "B", CREATED, 6, 0, 0, 0, CREATED + timedelta(hours=40))],
        "uri string, text string, created_at timestamp, likes long, reposts long, replies long, quotes long, "
        "measured_at timestamp",
    )
    rows = {row["post"]: row for row in posts_gold(history, NOW).collect()}
    assert (rows["a"]["likes"], rows["a"]["engagement_24h"]) == (30, 5)
    assert (rows["b"]["likes"], rows["b"]["engagement_24h"]) == (6, None)


@respx.mock
def test_streamelements_counts_the_channel_and_the_human_chat(settings: Settings) -> None:
    api = "https://api.streamelements.com/kappa/v2"
    session = {
        "follower-total": {"count": 39},
        "follower-month": {"count": 3},
        "subscriber-total": {"count": 0},
        "tip-count": {"count": 2},
        "tip-total": {"amount": 7.5},
        "cheer-total": {"amount": 100},
    }
    chat = {
        "chatters": [
            {"name": "StreamElements", "amount": 90},
            {"name": "ada", "amount": 8},
            {"name": "bo", "amount": 2},
        ],
        "commands": [{"command": "!website", "amount": 7}, {"command": "!games", "amount": 1}],
    }
    respx.get(f"{api}/channels/me").respond(json={"_id": "c1"})
    respx.get(f"{api}/sessions/c1").respond(json={"data": session})
    respx.get(f"{api}/chatstats/retrospeedlab/stats").respond(json=chat)
    numbers = {number.metric: number.value for number in streamelements_numbers(settings.sources.streamelements)}
    assert numbers == {
        "followers": 39,
        "followers_month": 3,
        "subscribers": 0,
        "tips": 2,
        "tip_amount": 7.5,
        "cheers": 100,
        "chat_messages": 10,
        "chatters": 2,
        "commands": 8,
    }


def youtube_report(request: httpx.Request) -> httpx.Response:
    metrics = request.url.params["metrics"].split(",")
    if "dimensions" not in request.url.params:
        return httpx.Response(200, json={"columnHeaders": [{"name": name} for name in metrics], "rows": [[47] * 8]})
    headers = [{"name": name} for name in ["day", *metrics]]
    rows = [["2026-11-12", 1, 19, 1174, 0, 0, 0, 0, 0], ["2026-11-13", 7, 32, 0, 1, 0, 1, 0, 1]]
    return httpx.Response(200, json={"columnHeaders": headers, "rows": rows})


@respx.mock
def test_youtube_loads_the_channels_last_28_days_and_each_day(spark: SparkSession, settings: Settings) -> None:
    respx.post("https://oauth2.googleapis.com/token").respond(json={"access_token": "token"})
    route = respx.get("https://youtubeanalytics.googleapis.com/v2/reports").mock(side_effect=youtube_report)
    numbers = {number.metric: number.value for number in youtube_numbers(settings.sources.youtube, NOW)}
    assert (route.calls[0].request.url.params["startDate"], route.calls[0].request.url.params["endDate"]) == (
        "2026-10-19",
        "2026-11-15",
    )
    assert (len(numbers), numbers["views"], numbers["average_view_seconds"]) == (8, 47, 47)
    days = SnapshotSource("youtube", DAILY, lambda now: youtube_days(settings.sources.youtube, now))
    ingest_sources(spark, settings.lake, Gate("sources"), [days], NOW)
    rows = {row["date"].isoformat(): row for row in read(spark, YOUTUBE_DAILY, settings.lake).collect()}
    assert (rows["2026-11-12"]["minutes_watched"], rows["2026-11-13"]["subscribers_gained"]) == (19, 1)
