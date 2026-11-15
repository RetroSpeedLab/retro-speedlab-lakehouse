from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime

from retro_speedlab_lakehouse.sources.record import SourcePage, SourceRecord


@dataclass(frozen=True, slots=True)
class Number:
    metric: str
    label: str
    value: float
    unit: str
    window: str


@dataclass(frozen=True, slots=True)
class NumberSource:
    name: str
    stream: str
    read: Callable[[datetime], list[Number]]

    def fetch(self, watermark: str | None, now: datetime) -> SourcePage:
        records = [SourceRecord(number.metric, asdict(number)) for number in self.read(now)]
        return SourcePage(records, now.isoformat())
