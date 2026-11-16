from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from retro_speedlab_lakehouse.sources.http import get_json
from retro_speedlab_lakehouse.sources.record import SourcePage, SourceRecord
from retro_speedlab_lakehouse.sources.settings import UmamiSettings
from retro_speedlab_lakehouse.sources.umami import signed_in

UMAMI = "umami"
CELLS = "engagement"
EVENTS = ("rank-view", "rank-click", "variant-view", "variant-goal")
CELL = "cell"
HOUR = timedelta(hours=1)
MILLISECONDS = 1000


def hour_of(moment: datetime) -> datetime:
    return moment.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def hours_to_load(watermark: str | None, now: datetime, backfill_hours: int) -> list[datetime]:
    end = hour_of(now)
    earliest = end - backfill_hours * HOUR
    start = earliest if watermark is None else max(datetime.fromisoformat(watermark), earliest)
    return [start + index * HOUR for index in range(int((end - start) / HOUR))]


def cells_record(session: httpx.Client, site: str, website: str, event: str, hour: datetime) -> SourceRecord:
    window = {
        "startAt": int(hour.timestamp() * MILLISECONDS),
        "endAt": int((hour + HOUR).timestamp() * MILLISECONDS) - 1,
    }
    values = get_json(
        session, f"/websites/{website}/event-data/values", {**window, "event": event, "propertyName": CELL}
    )
    cells = {value["value"]: int(value["total"]) for value in values}
    body = {"site": site, "event": event, "hour": hour.isoformat(), "cells": cells}
    return SourceRecord(f"{site}/{event}/{hour.isoformat()}", body)


@dataclass(frozen=True, slots=True)
class UmamiCells:
    umami: UmamiSettings
    websites: dict[str, str]
    backfill_hours: int

    @property
    def name(self) -> str:
        return UMAMI

    @property
    def stream(self) -> str:
        return CELLS

    def fetch(self, watermark: str | None, now: datetime) -> SourcePage:
        hours = hours_to_load(watermark, now, self.backfill_hours)
        if not hours:
            return SourcePage([], watermark)
        with signed_in(self.umami) as session:
            records = [
                cells_record(session, site, website, event, hour)
                for hour in hours
                for site, website in self.websites.items()
                for event in EVENTS
            ]
        return SourcePage(records, (hours[-1] + HOUR).isoformat())
