from datetime import datetime

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.bandit.comparison import SECONDS_PER_HOUR
from retro_speedlab_lakehouse.bandit.holdout import finished_turns, turn_savings
from retro_speedlab_lakehouse.engagement.catalog import CONTENT_RANKING, ENGAGEMENT_EXPERIMENTS, EXPERIMENT_PLAN
from retro_speedlab_lakehouse.lake.catalog import (
    LAB_EXPERIMENTS,
    PROMOTION_PREFERENCES,
    PROMOTION_ROI,
    SEARCH_BANDIT,
    SEARCH_EVENTS,
    SUPPORTER_COUNTS,
    Table,
)
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.results.postgres import publish
from retro_speedlab_lakehouse.settings import Settings

RESULTS: tuple[Table, ...] = (
    ENGAGEMENT_EXPERIMENTS,
    EXPERIMENT_PLAN,
    LAB_EXPERIMENTS,
    SEARCH_BANDIT,
    PROMOTION_ROI,
    PROMOTION_PREFERENCES,
    CONTENT_RANKING,
    SUPPORTER_COUNTS,
)
SEARCH_BANDIT_TURNS = "search_bandit_turns"


def bandit_turns(events: DataFrame, now: datetime) -> DataFrame:
    turns = turn_savings(events, finished_turns(events, now))
    hours = F.col("seconds") / SECONDS_PER_HOUR
    return turns.withColumn("frames_saved_per_hour", F.col("saved") / hours)


def export_experiments(spark: SparkSession, settings: Settings, now: datetime) -> int:
    tables = {table.name: read(spark, table, settings.lake) for table in RESULTS}
    tables[SEARCH_BANDIT_TURNS] = bandit_turns(read(spark, SEARCH_EVENTS, settings.lake), now)
    rows = publish(settings.experiments_database, tables)
    logger.info(f"Experiments: {rows} rows in {len(tables)} Postgres tables for Grafana")
    return rows
