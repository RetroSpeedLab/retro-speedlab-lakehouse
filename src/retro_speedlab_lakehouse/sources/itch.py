import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

import httpx

from retro_speedlab_lakehouse.sources.http import get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.plays import per_game
from retro_speedlab_lakehouse.sources.record import SourceRecord
from retro_speedlab_lakehouse.sources.settings import UmamiSettings
from retro_speedlab_lakehouse.sources.umami import event_counts, human_games, milliseconds, signed_in, week

WINDOW = "LAB24 on itch.io, last 7 days"
DAY = timedelta(days=1)
DAYS = 2
GAME_PATH = re.compile(r"([^/?#]+)/(?:index\.html)?(?:[?#].*)?$")
EVENTS = {
    "campaign": "campaign-started",
    "endless": "endless-started",
    "cleared": "level-cleared",
    "won": "campaign-won",
}


@dataclass(frozen=True, slots=True)
class ItchDay:
    date: str
    pageviews: int
    visitors: int
    views: dict[str, int]
    campaign: dict[str, int]
    endless: dict[str, int]
    cleared: dict[str, int]
    won: dict[str, int]

    @property
    def record(self) -> SourceRecord:
        return SourceRecord(self.date, asdict(self))


def itch_numbers(settings: UmamiSettings, now: datetime) -> list[Number]:
    window = week(now)
    with signed_in(settings) as session:
        stats = get_json(session, f"/websites/{settings.itch_website}/stats", window)
        counts = event_counts(session, settings.itch_website, window)
    return [
        Number("visitors", "Visitors", stats["visitors"], "", WINDOW),
        Number("pageviews", "Page views", stats["pageviews"], "", WINDOW),
        *human_games(counts, WINDOW),
    ]


def game_views(session: httpx.Client, website: str, window: dict[str, int]) -> dict[str, int]:
    views: Counter[str] = Counter()
    for path in get_json(session, f"/websites/{website}/metrics", {**window, "type": "path"}):
        found = GAME_PATH.search(path["x"])
        if found:
            views[found[1]] += int(path["y"])
    return dict(views)


def itch_day(session: httpx.Client, website: str, start: datetime) -> ItchDay:
    window = {"startAt": milliseconds(start), "endAt": milliseconds(start + DAY) - 1}
    stats = get_json(session, f"/websites/{website}/stats", window)
    counts = {name: per_game(session, website, event, window) for name, event in EVENTS.items()}
    views = game_views(session, website, window)
    return ItchDay(start.date().isoformat(), stats["pageviews"], stats["visitors"], views, **counts)


def itch_days(settings: UmamiSettings, now: datetime) -> list[ItchDay]:
    today = datetime(now.year, now.month, now.day, tzinfo=UTC)
    with signed_in(settings) as session:
        return [itch_day(session, settings.itch_website, today - DAY * back) for back in range(DAYS)]
