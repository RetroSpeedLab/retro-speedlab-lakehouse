import threading
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import (
    PLATFORM_JOBS,
    PLATFORM_TABLES,
    QUALITY_REPORTS,
    RAW_DAYS,
    RETENTION_HOURS,
    Layer,
    Table,
)
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.settings import LakeSettings

JOB_HISTORY_DAYS = 30
BUDGET = timedelta(minutes=30)
CANCEL_EVERY_SECONDS = 5.0
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


def files(spark: SparkSession, table: Table, lake: LakeSettings) -> int:
    detail = DeltaTable.forPath(spark, table.location(lake)).detail().first()
    if detail is None:
        raise ValueError(f"{table.full_name} has no detail")
    return int(detail["numFiles"])


def fewest_files_first(spark: SparkSession, tables: tuple[Table, ...], lake: LakeSettings) -> list[Table]:
    named = {table.full_name: table for table in tables}
    measured = read(spark, PLATFORM_TABLES, lake).select("table", "files").orderBy("files", "table").collect()
    return [named[row["table"]] for row in measured if row["table"] in named]


def maintain_table(spark: SparkSession, table: Table, lake: LakeSettings, now: datetime) -> None:
    expire(spark, table, lake, now)
    compact(spark, table, lake)
    vacuum(spark, table, lake)
    logger.info(f"Maintained {table.full_name}")


def cancel_until(spark: SparkSession, done: threading.Event) -> None:
    while not done.is_set():
        spark.sparkContext.cancelAllJobs()
        done.wait(CANCEL_EVERY_SECONDS)


def maintain(
    spark: SparkSession, tables: tuple[Table, ...], lake: LakeSettings, now: datetime, budget: timedelta
) -> int:
    deadline = datetime.now(UTC) + budget
    done = threading.Event()
    stop = threading.Timer(budget.total_seconds(), cancel_until, (spark, done))
    stop.start()
    maintained = 0
    try:
        for table in fewest_files_first(spark, tables, lake):
            if datetime.now(UTC) >= deadline:
                break
            maintain_table(spark, table, lake, now)
            maintained += 1
    except Exception:
        if datetime.now(UTC) < deadline:
            raise
    finally:
        done.set()
        stop.cancel()
    if datetime.now(UTC) >= deadline:
        logger.warning(f"Maintenance took {budget}; it maintained {maintained} tables, the rest wait for a later night")
    return maintained
