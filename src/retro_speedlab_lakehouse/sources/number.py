from dataclasses import asdict, dataclass

from retro_speedlab_lakehouse.sources.record import SourceRecord


@dataclass(frozen=True, slots=True)
class Number:
    metric: str
    label: str
    value: float
    unit: str
    window: str

    @property
    def record(self) -> SourceRecord:
        return SourceRecord(self.metric, asdict(self))
