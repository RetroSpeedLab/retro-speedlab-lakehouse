from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.cockpit.catalog import SEARCH_HOURS
from retro_speedlab_lakehouse.cockpit.days import Metric
from retro_speedlab_lakehouse.cockpit.findings import DayMetric, Evidence, GameSearch
from retro_speedlab_lakehouse.lake.catalog import HOST_STATUS, PLATFORM_JOBS, QUALITY_FAILURES, STREAM_STATUS, Table
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import LANGFUSE_DAILY

DAY = timedelta(days=1)
HOUR_SECONDS = 3600
MINUTE_SECONDS = 60
FAILED = "failed"


def rows_of(spark: SparkSession, table: Table, lake: LakeSettings, columns: list[Column]) -> list[Row]:
    return read(spark, table, lake).select(*columns).collect() if exists(spark, table, lake) else []


def seconds_before(now: datetime, column: str) -> Column:
    return F.unix_timestamp(F.lit(now)) - F.unix_timestamp(column)


def last_day(now: datetime, column: str) -> Column:
    return F.col(column) >= F.lit(now - DAY)


def day_metrics(days: DataFrame, metrics: list[Metric]) -> tuple[DayMetric, ...]:
    values: dict[str, dict[int, float | None]] = {metric.name: {} for metric in metrics}
    for row in days.select("metric", "days_ago", "value").collect():
        values[row["metric"]][row["days_ago"]] = row["value"]
    return tuple(DayMetric(metric.name, metric.area, metric.flow, values[metric.name]) for metric in metrics)


def searches(spark: SparkSession, lake: LakeSettings, now: datetime) -> tuple[GameSearch, ...]:
    if not exists(spark, SEARCH_HOURS, lake):
        return ()
    hours = read(spark, SEARCH_HOURS, lake)
    searched = hours.where(last_day(now, "hour") & (F.col("attempts") > 0)).select("game").distinct()
    ended = seconds_before(now, "hour") - HOUR_SECONDS
    waited = hours.where("improvements > 0").groupBy("game").agg((F.min(ended) / HOUR_SECONDS).alias("waited"))
    rows = searched.join(waited, "game", "left").orderBy("game").collect()
    return tuple(GameSearch(row["game"], None if row["waited"] is None else max(row["waited"], 0.0)) for row in rows)


def failed_jobs(spark: SparkSession, lake: LakeSettings, now: datetime) -> tuple[str, ...]:
    failed = (F.col("status") == FAILED) & last_day(now, "finished_at")
    rows = rows_of(spark, PLATFORM_JOBS, lake, [F.when(failed, F.col("job")).alias("job")])
    return tuple(row["job"] for row in rows if row["job"] is not None)


def failed_tables(spark: SparkSession, lake: LakeSettings, now: datetime) -> tuple[str, ...]:
    rows = rows_of(spark, QUALITY_FAILURES, lake, [F.when(last_day(now, "checked_at"), F.col("table")).alias("table")])
    return tuple(row["table"] for row in rows if row["table"] is not None)


def stream_lag(spark: SparkSession, lake: LakeSettings, now: datetime) -> float | None:
    lag = F.greatest(F.col("lag_seconds"), seconds_before(now, "updated_at")) / MINUTE_SECONDS
    rows = rows_of(spark, STREAM_STATUS, lake, [lag.alias("lag")])
    if not rows:
        return None
    return max(row["lag"] for row in rows)


def disks(spark: SparkSession, lake: LakeSettings) -> dict[str, float]:
    rows = rows_of(spark, HOST_STATUS, lake, [F.col("host"), F.col("disk_percent")])
    return {row["host"]: row["disk_percent"] for row in rows}


def claude_cost(spark: SparkSession, lake: LakeSettings, now: datetime) -> dict[int, float]:
    days_ago = F.datediff(F.lit(now.date()), F.col("date"))
    rows = rows_of(spark, LANGFUSE_DAILY, lake, [days_ago.alias("days_ago"), F.col("cost_usd")])
    return {row["days_ago"]: row["cost_usd"] for row in rows}


def gather(spark: SparkSession, lake: LakeSettings, days: DataFrame, metrics: list[Metric], now: datetime) -> Evidence:
    return Evidence(
        metrics=day_metrics(days, metrics),
        searches=searches(spark, lake, now),
        failed_jobs=failed_jobs(spark, lake, now),
        failed_tables=failed_tables(spark, lake, now),
        stream_lag_minutes=stream_lag(spark, lake, now),
        disks=disks(spark, lake),
        claude_cost=claude_cost(spark, lake, now),
    )
