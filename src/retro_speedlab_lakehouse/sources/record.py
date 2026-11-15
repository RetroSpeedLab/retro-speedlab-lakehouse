from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class SourceRecord:
    key: str
    body: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SourcePage:
    records: list[SourceRecord]
    watermark: str | None


class Source(Protocol):
    name: str
    stream: str

    def fetch(self, watermark: str | None, now: datetime) -> SourcePage: ...
