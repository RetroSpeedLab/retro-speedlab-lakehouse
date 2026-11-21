from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.record import SourcePage, SourceRecord
from retro_speedlab_lakehouse.sources.settings import TempoSettings
from retro_speedlab_lakehouse.sources.tempo import LAB_EPOCH, NANOSECONDS, ROW_LIMIT, SEARCH, attribute_value

TEMPO = "tempo"
ROUNDS = "rounds"
COUNTS = ("turns", "tool_calls", "tool_errors", "subagents", "tokens")
USAGE = {
    "input_tokens": "lab.usage.input",
    "output_tokens": "lab.usage.output",
    "cache_read_tokens": "lab.usage.cache_read_input_tokens",
    "cache_creation_tokens": "lab.usage.cache_creation_input_tokens",
}
MODEL = "gen_ai.response.model"
SELECTED = ["lab.game", "lab.stamp", "lab.round", "lab.outcome", "lab.cost_usd", *(f"lab.{name}" for name in COUNTS)]
SELECTION = ", ".join(f"span.{key}" for key in [*SELECTED, *USAGE.values(), MODEL])
QUERY = f'{{ name =~ "lab round.*" }} | select({SELECTION})'
OVERLAP = timedelta(hours=6)
SECONDS_PER_MINUTE = 60


@dataclass(frozen=True, slots=True)
class LabRound:
    game: str
    stamp: str
    round: int
    outcome: str
    started_at: datetime
    minutes: float
    model: str | None
    cost_usd: float
    turns: int
    tool_calls: int
    tool_errors: int
    subagents: int
    tokens: int
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int

    @property
    def record(self) -> SourceRecord:
        return SourceRecord(f"{self.stamp}:{self.round}", {**asdict(self), "started_at": self.started_at.isoformat()})


def lab_round(span: dict[str, Any]) -> LabRound:
    found = {attribute["key"]: attribute_value(attribute) for attribute in span["attributes"]}
    started_at = datetime.fromtimestamp(int(span["startTimeUnixNano"]) / NANOSECONDS, UTC)
    minutes = int(span["durationNanos"]) / NANOSECONDS / SECONDS_PER_MINUTE
    return LabRound(
        found["lab.game"],
        found["lab.stamp"],
        int(found["lab.round"]),
        found["lab.outcome"],
        started_at,
        minutes,
        found[MODEL] if MODEL in found else None,
        float(found["lab.cost_usd"]),
        *(int(found[f"lab.{name}"]) for name in COUNTS),
        *(int(found[key]) for key in USAGE.values()),
    )


def lab_rounds(settings: TempoSettings, start: datetime, end: datetime) -> list[LabRound]:
    params = {"q": QUERY, "start": int(start.timestamp()), "end": int(end.timestamp()), "limit": ROW_LIMIT, "spss": 1}
    with client(settings.url, {}) as session:
        traces = get_json(session, SEARCH, params)["traces"]
    return [lab_round(span) for found in traces for spans in found["spanSets"] for span in spans["spans"]]


@dataclass(frozen=True, slots=True)
class TempoRounds:
    settings: TempoSettings

    @property
    def name(self) -> str:
        return TEMPO

    @property
    def stream(self) -> str:
        return ROUNDS

    def fetch(self, watermark: str | None, now: datetime) -> SourcePage:
        start = LAB_EPOCH if watermark is None else datetime.fromisoformat(watermark) - OVERLAP
        return SourcePage([found.record for found in lab_rounds(self.settings, start, now)], now.isoformat())
