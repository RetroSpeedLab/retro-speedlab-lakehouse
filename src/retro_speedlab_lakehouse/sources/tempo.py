from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.record import SourceRecord
from retro_speedlab_lakehouse.sources.settings import TempoSettings

SEARCH = "/api/search"
SCORES = '{ name = "lab scores" } | select(span.lab.stamp, span.score.steps, span.score.tokens, span.score.cost_usd)'
WEEK = timedelta(days=7)
DAY = timedelta(days=1)
WINDOW = "last 7 days"
LAB_EPOCH = datetime(2026, 11, 1, tzinfo=UTC)
ROW_LIMIT = 10_000
NANOSECONDS = 1_000_000_000


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


@dataclass(frozen=True, slots=True)
class ScoredRun:
    started_at: datetime
    cost: LabCost


def attribute_value(attribute: dict[str, Any]) -> Any:
    return next(iter(attribute["value"].values()))


def scored_run(span: dict[str, Any]) -> ScoredRun:
    found = {attribute["key"]: attribute_value(attribute) for attribute in span["attributes"]}
    started_at = datetime.fromtimestamp(int(span["startTimeUnixNano"]) / NANOSECONDS, UTC)
    cost = LabCost(
        found["lab.stamp"],
        int(float(found["score.steps"])),
        int(float(found["score.tokens"])),
        float(found["score.cost_usd"]),
    )
    return ScoredRun(started_at, cost)


def scored_runs(settings: TempoSettings, start: datetime, end: datetime) -> list[ScoredRun]:
    params = {"q": SCORES, "start": int(start.timestamp()), "end": int(end.timestamp()), "limit": ROW_LIMIT, "spss": 1}
    with client(settings.url, {}) as session:
        traces = get_json(session, SEARCH, params)["traces"]
    return [scored_run(span) for found in traces for spans in found["spanSets"] for span in spans["spans"]]


def summed(runs: list[ScoredRun]) -> tuple[int, int, float]:
    costs = [run.cost for run in runs]
    return (
        sum(cost.observations for cost in costs),
        sum(cost.tokens for cost in costs),
        sum(cost.cost_usd for cost in costs),
    )


def tempo_numbers(settings: TempoSettings, now: datetime) -> list[Number]:
    observations, tokens, cost = summed(scored_runs(settings, now - WEEK, now))
    return [
        Number("observations", "Traced steps", observations, "", WINDOW),
        Number("tokens", "Tokens", tokens, "", WINDOW),
        Number("cost", "Model cost", round(cost, 2), "USD", WINDOW),
    ]


def tempo_days(settings: TempoSettings, now: datetime, days: int) -> list[DailyCost]:
    midnight = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    starts = [midnight - back * DAY for back in range(days)]
    runs = scored_runs(settings, starts[-1], now)
    return [
        DailyCost(start.date(), *summed([run for run in runs if run.started_at.date() == start.date()]))
        for start in starts
    ]


def tempo_lab_costs(settings: TempoSettings, now: datetime) -> list[LabCost]:
    return [run.cost for run in scored_runs(settings, LAB_EPOCH, now)]
