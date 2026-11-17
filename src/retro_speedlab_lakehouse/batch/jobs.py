from collections.abc import Callable
from datetime import UTC, datetime

from loguru import logger
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.bandit.allocation import build_allocation
from retro_speedlab_lakehouse.bandit.holdout import build_bandit_comparison
from retro_speedlab_lakehouse.batch.api import ingest_api_hourly, ingest_api_nightly
from retro_speedlab_lakehouse.batch.gold import build_gold
from retro_speedlab_lakehouse.batch.maintenance import maintain
from retro_speedlab_lakehouse.batch.platform import JobRun, build_platform, record_job
from retro_speedlab_lakehouse.batch.silver import build_silver
from retro_speedlab_lakehouse.cockpit.catalog import COCKPIT_TABLES
from retro_speedlab_lakehouse.cockpit.job import build_cockpit
from retro_speedlab_lakehouse.engagement.catalog import ENGAGEMENT_TABLES
from retro_speedlab_lakehouse.engagement.jobs import build_community, build_engagement, build_engagement_experiments
from retro_speedlab_lakehouse.experiments.job import build_experiments
from retro_speedlab_lakehouse.lake.catalog import CORE_TABLES, MINING_TABLES, Table
from retro_speedlab_lakehouse.mining.build import build_mining_gold
from retro_speedlab_lakehouse.mining.records import mine_records
from retro_speedlab_lakehouse.mining.verify import verify_replays
from retro_speedlab_lakehouse.promotion.roi import build_promotion_roi
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.catalog import SOURCE_TABLES
from retro_speedlab_lakehouse.sources.job import ingest_sources
from retro_speedlab_lakehouse.sources.registry import hourly_sources, nightly_sources
from retro_speedlab_lakehouse.videos.mirror import mirror_videos

Job = Callable[[SparkSession, Settings, Gate], int]
ALL_TABLES: tuple[Table, ...] = CORE_TABLES + MINING_TABLES + SOURCE_TABLES + ENGAGEMENT_TABLES + COCKPIT_TABLES
SUCCESS = "success"
FAILED = "failed"
ERROR_LENGTH = 500


def now() -> datetime:
    return datetime.now(UTC)


def sources_hourly(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return ingest_sources(spark, settings.lake, gate, hourly_sources(settings.sources), now())


def sources_nightly(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return ingest_sources(spark, settings.lake, gate, nightly_sources(settings.sources), now())


def silver(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_silver(spark, settings.lake, gate)


def gold(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    moment = now()
    return build_gold(spark, settings.lake, gate, moment) + build_promotion_roi(spark, settings.lake, gate, moment)


def lab_experiments(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_experiments(spark, settings.lake, gate, now())


def search_allocation(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_allocation(spark, settings.lake, settings.bandit, gate, now())


def search_bandit(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_bandit_comparison(spark, settings.lake, gate, now())


def engagement_hourly(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_engagement(spark, settings, gate, now())


def engagement_nightly(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_engagement_experiments(spark, settings, gate, now()) + build_community(spark, settings, gate)


def cockpit(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_cockpit(spark, settings.lake, settings.findings, gate, now())


def platform(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return build_platform(spark, ALL_TABLES, settings.lake, gate, now())


def maintenance(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return maintain(spark, ALL_TABLES, settings.lake, now())


JOBS: dict[str, Job] = {
    "api-hourly": ingest_api_hourly,
    "api-nightly": ingest_api_nightly,
    "sources-hourly": sources_hourly,
    "sources-nightly": sources_nightly,
    "silver": silver,
    "gold": gold,
    "experiments": lab_experiments,
    "search-allocation": search_allocation,
    "search-bandit": search_bandit,
    "videos-nightly": mirror_videos,
    "engagement-hourly": engagement_hourly,
    "engagement-nightly": engagement_nightly,
    "cockpit": cockpit,
    "platform": platform,
    "maintenance": maintenance,
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
