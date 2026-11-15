from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from statistics import median

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import STREAM_STATUS
from retro_speedlab_lakehouse.lake.write import merge
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings

WINDOW = timedelta(minutes=1)
STREAM = "live"


@dataclass(frozen=True, slots=True)
class BatchStats:
    at: datetime
    events: int
    lag_seconds: float
    lag_max_seconds: float
    last_event_at: datetime


def batch_stats(events: DataFrame) -> BatchStats:
    lag = F.unix_micros(F.current_timestamp()) - F.unix_micros("event_at")
    row = events.agg(
        F.count("*").alias("events"),
        (F.percentile_approx(lag, 0.5) / 1e6).alias("lag"),
        (F.max(lag) / 1e6).alias("lag_max"),
        F.max("event_at").alias("last"),
    ).first()
    if row is None:
        raise ValueError("A batch without events has no stats")
    return BatchStats(datetime.now(UTC), row["events"], row["lag"], row["lag_max"], row["last"])


class StreamStatus:
    def __init__(self, settings: Settings, gate: Gate) -> None:
        self.settings = settings
        self.gate = gate
        self.batches: deque[BatchStats] = deque()
        self.written_at = datetime.min.replace(tzinfo=UTC)

    def observe(self, events: DataFrame) -> None:
        stats = batch_stats(events)
        self.batches.append(stats)
        while self.batches[0].at < stats.at - WINDOW:
            self.batches.popleft()
        if stats.at - self.written_at >= timedelta(seconds=self.settings.stream.status_seconds):
            self.write(events, stats)

    def write(self, events: DataFrame, stats: BatchStats) -> None:
        row = {
            "stream": STREAM,
            "events_per_minute": sum(batch.events for batch in self.batches),
            "lag_seconds": median(batch.lag_seconds for batch in self.batches),
            "lag_max_seconds": max(batch.lag_max_seconds for batch in self.batches),
            "last_event_at": stats.last_event_at,
            "updated_at": stats.at,
        }
        merge(STREAM_STATUS, self.settings.lake, events.sparkSession.createDataFrame([row]), self.gate)
        self.gate.flush(events.sparkSession, self.settings.lake)
        self.written_at = stats.at
