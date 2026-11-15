from collections.abc import Callable
from datetime import UTC, datetime

from loguru import logger
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.batch.api import ingest_api_hourly, ingest_api_nightly
from retro_speedlab_lakehouse.batch.gold import build_gold
from retro_speedlab_lakehouse.batch.maintenance import maintain, vacuum_live
from retro_speedlab_lakehouse.batch.platform import JobRun, build_platform, record_job
from retro_speedlab_lakehouse.batch.silver import build_silver
from retro_speedlab_lakehouse.lake.catalog import CORE_TABLES, MINING_TABLES, Table
from retro_speedlab_lakehouse.mining.build import build_mining_gold
from retro_speedlab_lakehouse.mining.records import mine_records
from retro_speedlab_lakehouse.mining.verify import verify_replays
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.bronze import ingest
from retro_speedlab_lakehouse.sources.catalog import SOURCE_TABLES
from retro_speedlab_lakehouse.sources.gold import build_source_gold
from retro_speedlab_lakehouse.sources.registry import hourly_sources, nightly_sources
from retro_speedlab_lakehouse.sources.silver import build_source_silver

Job = Callable[[SparkSession, Settings, Gate], int]
ALL_TABLES: tuple[Table, ...] = CORE_TABLES + MINING_TABLES + SOURCE_TABLES
SUCCESS = "success"
FAILED = "failed"
ERROR_LENGTH = 500


def now() -> datetime:
    return datetime.now(UTC)


def sources_hourly(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return sum(ingest(spark, settings.lake, gate, source) for source in hourly_sources(settings))


def sources_nightly(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return sum(ingest(spark, settings.lake, gate, source) for source in nightly_sources(settings))


def silver(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_silver(spark, settings.lake, gate) + build_source_silver(spark, settings.lake, gate)


def gold(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_gold(spark, settings.lake, gate, now()) + build_source_gold(spark, settings.lake, gate)


def platform(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_platform(spark, ALL_TABLES, settings.lake, gate, now())


def maintenance(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return maintain(spark, ALL_TABLES, settings.lake, now())


def live_vacuum(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return vacuum_live(spark, ALL_TABLES, settings.lake)


JOBS: dict[str, Job] = {
    "api-hourly": ingest_api_hourly,
    "api-nightly": ingest_api_nightly,
    "sources-hourly": sources_hourly,
    "sources-nightly": sources_nightly,
    "silver": silver,
    "gold": gold,
    "platform": platform,
    "maintenance": maintenance,
    "vacuum-live": live_vacuum,
    "mine-records": mine_records,
    "verify-replays": verify_replays,
    "mining-gold": build_mining_gold,
}


def run_job(spark: SparkSession, settings: Settings, name: str, dag: str, run_id: str) -> None:
    if name not in JOBS:
        raise ValueError(f"No job is called {name}; the jobs are {', '.join(JOBS)}")
    started_at = now()
    gate = Gate(f"{dag}/{run_id}/{name}")
    try:
        rows = JOBS[name](spark, settings, gate)
    except Exception as error:
        gate.flush(spark, settings.lake)
        failure = JobRun(dag, run_id, name, started_at, now(), FAILED, 0, str(error)[:ERROR_LENGTH])
        record_job(spark, settings.lake, gate, failure)
        raise
    gate.flush(spark, settings.lake)
    record_job(spark, settings.lake, gate, JobRun(dag, run_id, name, started_at, now(), SUCCESS, rows, None))
    logger.info(f"{name} wrote {rows} rows in {(now() - started_at).total_seconds():.1f} s")
