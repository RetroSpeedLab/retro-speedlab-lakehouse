from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import TIMEOUT_SECONDS, client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.record import SourceRecord
from retro_speedlab_lakehouse.sources.settings import YoutubeSettings

TOKEN_URL = "https://oauth2.googleapis.com/token"
ANALYTICS = "https://youtubeanalytics.googleapis.com/v2"
DAYS = 28
WINDOW = f"on YouTube, last {DAYS} days"
METRICS = (
    ("views", "views", "Views", ""),
    ("estimatedMinutesWatched", "minutes_watched", "Minutes watched", "min"),
    ("averageViewDuration", "average_view_seconds", "Average view", "s"),
    ("subscribersGained", "subscribers_gained", "Subscribers gained", ""),
    ("subscribersLost", "subscribers_lost", "Subscribers lost", ""),
    ("likes", "likes", "Likes", ""),
    ("comments", "comments", "Comments", ""),
    ("shares", "shares", "Shares", ""),
)


@dataclass(frozen=True, slots=True)
class YoutubeDay:
    date: str
    counts: dict[str, int]

    @property
    def record(self) -> SourceRecord:
        return SourceRecord(self.date, {"date": self.date, **self.counts})


def access_token(settings: YoutubeSettings) -> str:
    credentials = {
        "client_id": settings.client_id,
        "client_secret": settings.client_secret,
        "refresh_token": settings.refresh_token,
        "grant_type": "refresh_token",
    }
    response = httpx.post(TOKEN_URL, data=credentials, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()["access_token"]


def report(settings: YoutubeSettings, now: datetime, dimensions: dict[str, str]) -> list[dict[str, Any]]:
    params = {
        "ids": "channel==MINE",
        "startDate": (now - timedelta(days=DAYS - 1)).date().isoformat(),
        "endDate": now.date().isoformat(),
        "metrics": ",".join(metric for metric, _, _, _ in METRICS),
        **dimensions,
    }
    with client(ANALYTICS, {"Authorization": f"Bearer {access_token(settings)}"}) as session:
        found = get_json(session, "/reports", params)
    names = [header["name"] for header in found["columnHeaders"]]
    return [dict(zip(names, row, strict=True)) for row in found["rows"]]


def youtube_numbers(settings: YoutubeSettings, now: datetime) -> list[Number]:
    totals = report(settings, now, {})[0]
    return [Number(name, label, totals[metric], unit, WINDOW) for metric, name, label, unit in METRICS]


def youtube_days(settings: YoutubeSettings, now: datetime) -> list[YoutubeDay]:
    rows = report(settings, now, {"dimensions": "day", "sort": "day"})
    return [YoutubeDay(row["day"], {name: row[metric] for metric, name, _, _ in METRICS}) for row in rows]
