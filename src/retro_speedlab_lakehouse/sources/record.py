from collections.abc import Callable, Sequence
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
    @property
    def name(self) -> str: ...

    @property
    def stream(self) -> str: ...

    def fetch(self, watermark: str | None, now: datetime) -> SourcePage: ...


class Recorded(Protocol):
    @property
    def record(self) -> SourceRecord: ...


@dataclass(frozen=True, slots=True)
class SnapshotSource:
    name: str
    stream: str
    read: Callable[[datetime], Sequence[Recorded]]

    def fetch(self, watermark: str | None, now: datetime) -> SourcePage:
        return SourcePage([item.record for item in self.read(now)], now.isoformat())
