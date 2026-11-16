from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import get_json
from retro_speedlab_lakehouse.sources.plays import per_game
from retro_speedlab_lakehouse.sources.record import SourceRecord
from retro_speedlab_lakehouse.sources.settings import UmamiSettings
from retro_speedlab_lakehouse.sources.umami import milliseconds, signed_in

ENDED = "campaign-ended"
WON = "campaign-won"
LEVEL = "level"
CONTAINS = "c."


@dataclass(frozen=True, slots=True)
class GameDropoff:
    game: str
    ended: dict[str, int]
    won: int
    since: str

    @property
    def record(self) -> SourceRecord:
        return SourceRecord(self.game, asdict(self))


def ended_levels(session: httpx.Client, website: str, game: str, window: dict[str, Any]) -> dict[str, int]:
    query = {**window, "event": ENDED, "propertyName": LEVEL, "path": f"{CONTAINS}/{game}/"}
    values = get_json(session, f"/websites/{website}/event-data/values", query)
    return {value["value"]: int(value["total"]) for value in values}


def umami_dropoff(settings: UmamiSettings, now: datetime) -> list[GameDropoff]:
    window = {"startAt": milliseconds(settings.dropoff_since), "endAt": milliseconds(now)}
    since = settings.dropoff_since.isoformat()
    with signed_in(settings) as session:
        ended = per_game(session, settings.games_website, ENDED, window)
        won = per_game(session, settings.games_website, WON, window)
        levels = {game: ended_levels(session, settings.games_website, game, window) for game in ended}
    return [
        GameDropoff(game, levels[game] if game in levels else {}, won[game] if game in won else 0, since)
        for game in sorted(set(ended) | set(won))
    ]
