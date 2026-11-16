import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import TIMEOUT_SECONDS, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.record import SourceRecord
from retro_speedlab_lakehouse.sources.settings import LangfuseSettings

METRICS = "/api/public/v2/metrics"
WEEK = timedelta(days=7)
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


def stamp(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def measured(settings: LangfuseSettings, query: dict[str, Any]) -> list[dict[str, Any]]:
    auth = (settings.public_key, settings.secret_key)
    with httpx.Client(base_url=settings.url, auth=auth, timeout=TIMEOUT_SECONDS) as session:
        return get_json(session, METRICS, {"query": json.dumps(query)})["data"]


def langfuse_numbers(settings: LangfuseSettings, now: datetime) -> list[Number]:
    query = {
        "view": "observations",
        "metrics": MEASURES,
        "dimensions": [],
        "filters": [],
        "fromTimestamp": stamp(now - WEEK),
        "toTimestamp": stamp(now),
    }
    totals = measured(settings, query)[0]
    return [
        Number("observations", "Traced steps", totals["count_count"], "", WINDOW),
        Number("tokens", "Tokens", totals["sum_totalTokens"], "", WINDOW),
        Number("cost", "Model cost", round(totals["sum_totalCost"], 2), "USD", WINDOW),
    ]


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
            int(row["sum_totalTokens"]),
            float(row["sum_totalCost"]),
        )
        for row in sessions
    ]
