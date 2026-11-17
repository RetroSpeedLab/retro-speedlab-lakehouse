import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import TIMEOUT_SECONDS, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.record import SourceRecord
from retro_speedlab_lakehouse.sources.settings import LangfuseSettings

METRICS = "/api/public/v2/metrics"
WEEK = timedelta(days=7)
DAY = timedelta(days=1)
WINDOW = "last 7 days"
MEASURES = [
    {"measure": "count", "aggregation": "count"},
    {"measure": "totalTokens", "aggregation": "sum"},
    {"measure": "totalCost", "aggregation": "sum"},
]
LAB_SESSION = "lab-run "
LAB_EPOCH = datetime(2026, 11, 1, tzinfo=UTC)
ROW_LIMIT = 1000


@dataclass(frozen=True, slots=True)
class LabCost:
    stamp: str
    observations: int
    tokens: int
    cost_usd: float

    @property
    def record(self) -> SourceRecord:
        return SourceRecord(self.stamp, asdict(self))


@dataclass(frozen=True, slots=True)
class DailyCost:
    date: date
    observations: int
    tokens: int
    cost_usd: float

    @property
    def record(self) -> SourceRecord:
        body = {"date": self.date.isoformat(), "observations": self.observations, "tokens": self.tokens}
        return SourceRecord(self.date.isoformat(), {**body, "cost_usd": self.cost_usd})


def stamp(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def total(value: float | str | None) -> float:
    return 0.0 if value is None else float(value)


def measured(settings: LangfuseSettings, query: dict[str, Any]) -> list[dict[str, Any]]:
    auth = (settings.public_key, settings.secret_key)
    with httpx.Client(base_url=settings.url, auth=auth, timeout=TIMEOUT_SECONDS) as session:
        return get_json(session, METRICS, {"query": json.dumps(query)})["data"]


def totals(settings: LangfuseSettings, start: datetime, end: datetime) -> dict[str, Any]:
    query = {
        "view": "observations",
        "metrics": MEASURES,
        "dimensions": [],
        "filters": [],
        "fromTimestamp": stamp(start),
        "toTimestamp": stamp(end),
    }
    return measured(settings, query)[0]


def langfuse_numbers(settings: LangfuseSettings, now: datetime) -> list[Number]:
    week = totals(settings, now - WEEK, now)
    return [
        Number("observations", "Traced steps", week["count_count"], "", WINDOW),
        Number("tokens", "Tokens", int(total(week["sum_totalTokens"])), "", WINDOW),
        Number("cost", "Model cost", round(total(week["sum_totalCost"]), 2), "USD", WINDOW),
    ]


def daily_cost(settings: LangfuseSettings, start: datetime, now: datetime) -> DailyCost:
    day = totals(settings, start, min(start + DAY, now))
    tokens = int(total(day["sum_totalTokens"]))
    return DailyCost(start.date(), int(day["count_count"]), tokens, total(day["sum_totalCost"]))


def langfuse_days(settings: LangfuseSettings, now: datetime, days: int) -> list[DailyCost]:
    midnight = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return [daily_cost(settings, midnight - back * DAY, now) for back in range(days)]


def langfuse_lab_costs(settings: LangfuseSettings, now: datetime) -> list[LabCost]:
    query = {
        "view": "observations",
        "metrics": MEASURES,
        "dimensions": [{"field": "sessionId"}],
        "filters": [],
        "fromTimestamp": stamp(LAB_EPOCH),
        "toTimestamp": stamp(now),
        "orderBy": [{"field": "sum_totalCost", "direction": "desc"}],
        "config": {"row_limit": ROW_LIMIT},
    }
    sessions = [row for row in measured(settings, query) if str(row["sessionId"]).startswith(LAB_SESSION)]
    return [
        LabCost(
            row["sessionId"].removeprefix(LAB_SESSION),
            int(row["count_count"]),
            int(total(row["sum_totalTokens"])),
            total(row["sum_totalCost"]),
        )
        for row in sessions
    ]
