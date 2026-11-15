from dataclasses import dataclass
from enum import StrEnum

from retro_speedlab_lakehouse.settings import LakeSettings


class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    LIVE = "live"


class Retention(StrEnum):
    LIVE = "live"
    SERVING = "serving"
    RAW = "raw"


RETENTION_PROPERTIES = {
    Retention.LIVE: {
        "delta.deletedFileRetentionDuration": "interval 1 hours",
        "delta.logRetentionDuration": "interval 1 days",
        "delta.checkpointInterval": "20",
    },
    Retention.SERVING: {
        "delta.deletedFileRetentionDuration": "interval 2 days",
        "delta.logRetentionDuration": "interval 7 days",
    },
    Retention.RAW: {
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "delta.logRetentionDuration": "interval 30 days",
    },
}
RETENTION_HOURS = {Retention.LIVE: 1, Retention.SERVING: 48, Retention.RAW: 168}
RAW_DAYS = 365


@dataclass(frozen=True, slots=True)
class Table:
    layer: Layer
    name: str
    keys: tuple[str, ...]
    partitions: tuple[str, ...]
    zorder: tuple[str, ...]

    @property
    def full_name(self) -> str:
        return f"{self.layer}.{self.name}"

    @property
    def retention(self) -> Retention:
        if self.layer is Layer.LIVE:
            return Retention.LIVE
        if self.layer is Layer.BRONZE:
            return Retention.RAW
        return Retention.SERVING

    def location(self, lake: LakeSettings) -> str:
        root = lake.bronze if self.layer is Layer.BRONZE else lake.serving
        return f"{root.rstrip('/')}/{self.layer}/{self.name}"


def table(layer: Layer, name: str, keys: tuple[str, ...]) -> Table:
    return Table(layer=layer, name=name, keys=keys, partitions=(), zorder=())


EVENTS = Table(Layer.BRONZE, "events", ("event_id",), ("date",), ())
API = Table(Layer.BRONZE, "api", (), ("date",), ())
SOURCES = Table(Layer.BRONZE, "sources", (), ("date",), ())
WATERMARKS = table(Layer.BRONZE, "source_watermarks", ("source", "stream"))

LIVE_RUN = table(Layer.LIVE, "live_run", ("slot",))
STREAM_STATUS = table(Layer.LIVE, "stream_status", ("stream",))

GAMES = table(Layer.SILVER, "games", ("slug",))
GAME_RUNS = Table(Layer.SILVER, "game_runs", ("game",), (), ())
RUN_VERSIONS = Table(Layer.SILVER, "run_versions", ("event_id",), (), ("game", "received_at"))
SPLITS = table(Layer.SILVER, "splits", ("game", "level"))
HISTORY = Table(Layer.SILVER, "history", ("game", "at", "seconds"), (), ("game", "at"))
KNOWLEDGE = table(Layer.SILVER, "knowledge", ("game", "kind", "name"))
LAB_RUNS = table(Layer.SILVER, "lab_runs", ("game", "stamp"))
LAB_PROGRESS = table(Layer.SILVER, "lab_run_progress", ("game", "stamp"))
REPORTS = table(Layer.SILVER, "reports", ("game", "name"))
HIGHSCORES = table(Layer.SILVER, "highscores", ("id",))

GAME_SUMMARY = table(Layer.GOLD, "game_summary", ("game",))
LEVEL_COSTS = table(Layer.GOLD, "level_costs", ("game", "level"))
IMPROVEMENT = table(Layer.GOLD, "improvement_curve", ("game", "hour"))
RECORDS_DAILY = table(Layer.GOLD, "records_daily", ("game", "date"))
LAB_OUTCOMES = table(Layer.GOLD, "lab_outcomes", ("game", "outcome"))
PLATFORM_TABLES = table(Layer.GOLD, "platform_tables", ("table",))
PLATFORM_JOBS = table(Layer.GOLD, "platform_jobs", ("run_id", "job"))
SOURCES_STATUS = table(Layer.GOLD, "sources_status", ("source", "stream"))
QUALITY_REPORTS = table(Layer.GOLD, "quality_reports", ())

TRACE_PIECES = table(Layer.BRONZE, "trace_pieces", ("game", "record", "segment"))
FRAME_TRACES = Table(Layer.BRONZE, "frame_traces", ("game", "frame"), ("game",), ())
VERIFIER_REPLAYS = table(Layer.SILVER, "verifier_replays", ("side", "entry"))
VERIFIER_SPLITS = table(Layer.SILVER, "verifier_splits", ("side", "entry", "split"))
LEVEL_SEGMENTS = table(Layer.GOLD, "level_segments", ("game", "segment"))
LEVEL_HISTORY = table(Layer.GOLD, "level_history", ("game", "level", "at"))
POSITION_HEATMAP = table(Layer.GOLD, "position_heatmap", ("game", "level", "cell_x", "cell_y"))
RAM_HEATMAP = table(Layer.GOLD, "ram_heatmap", ("game", "level", "label", "value"))
MINING_SUMMARY = table(Layer.GOLD, "mining_summary", ("game",))
BOARD_HISTORY = table(Layer.GOLD, "board_history", ("game", "side", "at"))
HUMANS_VS_AI = table(Layer.GOLD, "humans_vs_ai", ("game", "id"))
SPLIT_COMPARISON = table(Layer.GOLD, "split_comparison", ("game", "level_index"))
VERIFICATION_STATS = table(Layer.GOLD, "verification_stats", ("game", "side"))

CORE_TABLES = (
    EVENTS,
    API,
    SOURCES,
    WATERMARKS,
    LIVE_RUN,
    STREAM_STATUS,
    GAMES,
    GAME_RUNS,
    RUN_VERSIONS,
    SPLITS,
    HISTORY,
    KNOWLEDGE,
    LAB_RUNS,
    LAB_PROGRESS,
    REPORTS,
    HIGHSCORES,
    GAME_SUMMARY,
    LEVEL_COSTS,
    IMPROVEMENT,
    RECORDS_DAILY,
    LAB_OUTCOMES,
    PLATFORM_TABLES,
    PLATFORM_JOBS,
    SOURCES_STATUS,
    QUALITY_REPORTS,
)
MINING_TABLES = (
    TRACE_PIECES,
    FRAME_TRACES,
    VERIFIER_REPLAYS,
    VERIFIER_SPLITS,
    LEVEL_SEGMENTS,
    LEVEL_HISTORY,
    POSITION_HEATMAP,
    RAM_HEATMAP,
    MINING_SUMMARY,
    BOARD_HISTORY,
    HUMANS_VS_AI,
    SPLIT_COMPARISON,
    VERIFICATION_STATS,
)
