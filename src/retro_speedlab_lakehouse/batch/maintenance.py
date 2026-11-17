from datetime import datetime, timedelta

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import (
    PLATFORM_JOBS,
    QUALITY_REPORTS,
    RAW_DAYS,
    RETENTION_HOURS,
    Layer,
    Table,
)
from retro_speedlab_lakehouse.lake.tables import exists
from retro_speedlab_lakehouse.settings import LakeSettings

JOB_HISTORY_DAYS = 30
HISTORY_COLUMNS = {PLATFORM_JOBS: "started_at", QUALITY_REPORTS: "checked_at"}


def compact(spark: SparkSession, table: Table, lake: LakeSettings) -> None:
    optimizer = DeltaTable.forPath(spark, table.location(lake)).optimize()
    if table.zorder:
        optimizer.executeZOrderBy(*table.zorder)
        return
    optimizer.executeCompaction()


def vacuum(spark: SparkSession, table: Table, lake: LakeSettings) -> None:
    DeltaTable.forPath(spark, table.location(lake)).vacuum(float(RETENTION_HOURS[table.retention]))


def expire(spark: SparkSession, table: Table, lake: LakeSettings, now: datetime) -> None:
    if table.layer is Layer.BRONZE and "date" in table.partitions:
        cutoff = (now - timedelta(days=RAW_DAYS)).date()
        DeltaTable.forPath(spark, table.location(lake)).delete(F.col("date") < F.lit(cutoff))
    if table in HISTORY_COLUMNS:
        cutoff_at = now - timedelta(days=JOB_HISTORY_DAYS)
        column = HISTORY_COLUMNS[table]
        DeltaTable.forPath(spark, table.location(lake)).delete(F.col(column) < F.lit(cutoff_at))


def maintain(spark: SparkSession, tables: tuple[Table, ...], lake: LakeSettings, now: datetime) -> int:
    kept = [table for table in tables if exists(spark, table, lake)]
    for table in kept:
        expire(spark, table, lake, now)
        compact(spark, table, lake)
        vacuum(spark, table, lake)
        logger.info(f"Maintained {table.full_name}")
    return len(kept)
