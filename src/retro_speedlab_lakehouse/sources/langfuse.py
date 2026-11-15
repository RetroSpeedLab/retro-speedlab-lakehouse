import json
from datetime import datetime, timedelta

import httpx

from retro_speedlab_lakehouse.sources.http import TIMEOUT_SECONDS, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import LangfuseSettings

METRICS = "/api/public/v2/metrics"
WEEK = timedelta(days=7)
WINDOW = "last 7 days"
MEASURES = [
    {"measure": "count", "aggregation": "count"},
    {"measure": "totalTokens", "aggregation": "sum"},
    {"measure": "totalCost", "aggregation": "sum"},
]


def stamp(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def langfuse_numbers(settings: LangfuseSettings, now: datetime) -> list[Number]:
    query = {
        "view": "observations",
        "metrics": MEASURES,
        "dimensions": [],
        "filters": [],
        "fromTimestamp": stamp(now - WEEK),
        "toTimestamp": stamp(now),
    }
    auth = (settings.public_key, settings.secret_key)
    with httpx.Client(base_url=settings.url, auth=auth, timeout=TIMEOUT_SECONDS) as session:
        totals = get_json(session, METRICS, {"query": json.dumps(query)})["data"][0]
    return [
        Number("observations", "Traced steps", totals["count_count"], "", WINDOW),
        Number("tokens", "Tokens", totals["sum_totalTokens"], "", WINDOW),
        Number("cost", "Model cost", round(totals["sum_totalCost"], 2), "USD", WINDOW),
    ]
