from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from retro_speedlab_lakehouse.cockpit.catalog import AUDIENCE_HOURS, COCKPIT_DAYS, SEARCH_HOURS
from retro_speedlab_lakehouse.cockpit.days import (
    HIGHSCORES_METRIC,
    LAB_RUNS_METRIC,
    SEARCH_METRICS,
    SOURCE_METRICS,
    Metric,
    counted_days,
    dense,
    search_days,
    source_days,
)
from retro_speedlab_lakehouse.cockpit.hours import audience_hours, search_hours
from retro_speedlab_lakehouse.lake.catalog import HIGHSCORES, LAB_RUNS, SEARCH_EVENTS, SOURCES
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace_counted
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings

COUNTED = ((HIGHSCORES, "set_at", HIGHSCORES_METRIC), (LAB_RUNS, "started_at", LAB_RUNS_METRIC))


def build_hours(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    events = read(spark, SEARCH_EVENTS, lake)
    searched, watched = search_hours(events, now), audience_hours(events, now)
    return replace_counted(SEARCH_HOURS, lake, searched, gate) + replace_counted(AUDIENCE_HOURS, lake, watched, gate)


def measured_days(spark: SparkSession, lake: LakeSettings, now: datetime) -> tuple[list[DataFrame], list[Metric]]:
    measured: list[DataFrame] = []
    metrics: list[Metric] = []
    if exists(spark, SEARCH_EVENTS, lake):
        measured.append(search_days(read(spark, SEARCH_EVENTS, lake), now))
        metrics.extend(SEARCH_METRICS)
    for table, column, metric in COUNTED:
        if exists(spark, table, lake):
            measured.append(counted_days(read(spark, table, lake), column, metric, now))
            metrics.append(metric)
    if exists(spark, SOURCES, lake):
        measured.append(source_days(read(spark, SOURCES, lake), now))
        metrics.extend(SOURCE_METRICS)
    return measured, metrics


def build_cockpit(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    measured, metrics = measured_days(spark, lake, now)
    if not measured:
        return 0
    days = dense(spark, measured, metrics, now)
    written = replace_counted(COCKPIT_DAYS, lake, days, gate)
    if exists(spark, SEARCH_EVENTS, lake):
        written += build_hours(spark, lake, gate, now)
    return written
