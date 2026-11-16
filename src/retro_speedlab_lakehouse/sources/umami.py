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


def umami_numbers(settings: UmamiSettings, now: datetime) -> list[Number]:
    window = {"startAt": milliseconds(now - WEEK), "endAt": milliseconds(now)}
    with signed_in(settings) as session:
        stats = get_json(session, f"/websites/{settings.website}/stats", window)
        active = get_json(session, f"/websites/{settings.website}/active", {})
        events = get_json(session, f"/websites/{settings.games_website}/metrics", {**window, "type": "event"})
    counts = {event["x"]: int(event["y"]) for event in events}
    started = sum(count for name, count in counts.items() if name.endswith(STARTED))
    return [
        Number("visitors", "Visitors", stats["visitors"], "", WINDOW),
        Number("visits", "Visits", stats["visits"], "", WINDOW),
        Number("pageviews", "Page views", stats["pageviews"], "", WINDOW),
        Number("active", "Visitors now", active["visitors"], "", "at the last load"),
        Number("game_plays", "Games played by humans", started, "", GAMES_WINDOW),
        Number("game_wins", "Campaigns won by humans", counts[WON] if WON in counts else 0, "", GAMES_WINDOW),
    ]
