from datetime import datetime, timedelta

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import GPU_METRICS, HOST_METRICS, SEARCH_EVENTS, Table
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.results.postgres import publish
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.tracking.catalog import (
    KIND_MINUTES,
    PPO_ROUNDS,
    SEARCH_TURNS,
    SEARCH_WORKER_KINDS,
    TRAINING_ROUNDS,
    TURN_MINUTES,
)
from retro_speedlab_lakehouse.tracking.gold import build_tracking_gold
from retro_speedlab_lakehouse.tracking.silver import MEASURED_AT, build_tracking_silver

CHARTED = timedelta(days=30)
WHOLE: tuple[Table, ...] = (SEARCH_TURNS, SEARCH_WORKER_KINDS, TRAINING_ROUNDS, PPO_ROUNDS)
RECENT: tuple[Table, ...] = (TURN_MINUTES, KIND_MINUTES, HOST_METRICS, GPU_METRICS)


def charted(spark: SparkSession, settings: Settings, now: datetime) -> dict[str, DataFrame]:
    lake = settings.lake
    recent = F.col(MEASURED_AT) >= F.lit(now - CHARTED)
    tables = {table.name: read(spark, table, lake) for table in WHOLE if exists(spark, table, lake)}
    found = [table for table in RECENT if exists(spark, table, lake)]
    return tables | {table.name: read(spark, table, lake).where(recent) for table in found}


def load_tracking(spark: SparkSession, settings: Settings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SEARCH_EVENTS, settings.lake):
        return 0
    build_tracking_silver(settings.lake, gate, read(spark, SEARCH_EVENTS, settings.lake), now)
    built = build_tracking_gold(spark, settings.lake, gate, now)
    published = publish(settings.experiments_database, charted(spark, settings, now))
    logger.info(f"Tracking: {built} gold rows, {published} rows in Postgres for Grafana")
    return built + published
