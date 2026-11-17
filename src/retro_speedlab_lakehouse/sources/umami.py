from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta

import httpx

from retro_speedlab_lakehouse.sources.http import client, get_json, sent
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import UmamiSettings

WEEK = timedelta(days=7)
WINDOW = "last 7 days"
GAMES_WINDOW = "games.retrospeedlab.com, last 7 days"
MILLISECONDS = 1000
STARTED = "-started"
WON = "campaign-won"


def milliseconds(moment: datetime) -> int:
    return int(moment.timestamp() * MILLISECONDS)


@contextmanager
def signed_in(settings: UmamiSettings) -> Iterator[httpx.Client]:
    with client(settings.url, {}) as session:
        login = {"username": settings.username, "password": settings.password}
        token = sent(session, "POST", "/auth/login", {}, login).json()["token"]
        session.headers["Authorization"] = f"Bearer {token}"
        yield session


def week(now: datetime) -> dict[str, int]:
    return {"startAt": milliseconds(now - WEEK), "endAt": milliseconds(now)}


def event_counts(session: httpx.Client, website: str, window: dict[str, int]) -> dict[str, int]:
    events = get_json(session, f"/websites/{website}/metrics", {**window, "type": "event"})
    return {event["x"]: int(event["y"]) for event in events}


def human_games(counts: dict[str, int], window: str) -> list[Number]:
    started = sum(count for name, count in counts.items() if name.endswith(STARTED))
    return [
        Number("game_plays", "Games played by humans", started, "", window),
        Number("game_wins", "Campaigns won by humans", counts[WON] if WON in counts else 0, "", window),
    ]


def umami_numbers(settings: UmamiSettings, now: datetime) -> list[Number]:
    window = week(now)
    with signed_in(settings) as session:
        stats = get_json(session, f"/websites/{settings.website}/stats", window)
        active = get_json(session, f"/websites/{settings.website}/active", {})
        counts = event_counts(session, settings.games_website, window)
    return [
        Number("visitors", "Visitors", stats["visitors"], "", WINDOW),
        Number("visits", "Visits", stats["visits"], "", WINDOW),
        Number("pageviews", "Page views", stats["pageviews"], "", WINDOW),
        Number("active", "Visitors now", active["visitors"], "", "at the last load"),
        *human_games(counts, GAMES_WINDOW),
    ]
