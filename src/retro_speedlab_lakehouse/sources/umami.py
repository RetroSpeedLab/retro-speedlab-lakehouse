from datetime import datetime, timedelta

from retro_speedlab_lakehouse.sources.http import client, get_json, sent
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import UmamiSettings

WEEK = timedelta(days=7)
WINDOW = "last 7 days"
MILLISECONDS = 1000


def milliseconds(moment: datetime) -> int:
    return int(moment.timestamp() * MILLISECONDS)


def umami_numbers(settings: UmamiSettings, now: datetime) -> list[Number]:
    with client(settings.url, {}) as session:
        login = {"username": settings.username, "password": settings.password}
        token = sent(session, "POST", "/auth/login", {}, login).json()["token"]
        session.headers["Authorization"] = f"Bearer {token}"
        window = {"startAt": milliseconds(now - WEEK), "endAt": milliseconds(now)}
        stats = get_json(session, f"/websites/{settings.website}/stats", window)
        active = get_json(session, f"/websites/{settings.website}/active", {})
    return [
        Number("visitors", "Visitors", stats["visitors"], "", WINDOW),
        Number("visits", "Visits", stats["visits"], "", WINDOW),
        Number("pageviews", "Page views", stats["pageviews"], "", WINDOW),
        Number("active", "Visitors now", active["visitors"], "", "at the last load"),
    ]
