from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import get_json
from retro_speedlab_lakehouse.sources.record import SourceRecord
from retro_speedlab_lakehouse.sources.settings import UmamiSettings
from retro_speedlab_lakehouse.sources.umami import milliseconds, signed_in

MODES = ("campaign", "endless")
OUTCOMES = ("started", "won")
WINDOWS = {"last_day": timedelta(days=1), "last_week": timedelta(days=7), "all_time": None}
GAME = "game"
EPOCH = 0


@dataclass(frozen=True, slots=True)
class ModePlays:
    mode: str
    window: str
    started: dict[str, int]
    won: dict[str, int]

    @property
    def record(self) -> SourceRecord:
        return SourceRecord(f"{self.mode}/{self.window}", asdict(self))


def event_name(mode: str, outcome: str) -> str:
    return f"{mode}-{outcome}"


def window_of(span: timedelta | None, now: datetime) -> dict[str, Any]:
    start = EPOCH if span is None else milliseconds(now - span)
    return {"startAt": start, "endAt": milliseconds(now)}


def per_game(session: httpx.Client, website: str, event: str, window: dict[str, Any]) -> dict[str, int]:
    values = get_json(
        session, f"/websites/{website}/event-data/values", {**window, "event": event, "propertyName": GAME}
    )
    return {value["value"]: int(value["total"]) for value in values}


def mode_plays(session: httpx.Client, website: str, mode: str, window: str, now: datetime) -> ModePlays:
    span = window_of(WINDOWS[window], now)
    counts = {outcome: per_game(session, website, event_name(mode, outcome), span) for outcome in OUTCOMES}
    return ModePlays(mode, window, counts["started"], counts["won"])


def umami_plays(settings: UmamiSettings, website: str, now: datetime) -> list[ModePlays]:
    with signed_in(settings) as session:
        return [mode_plays(session, website, mode, window, now) for mode in MODES for window in WINDOWS]
