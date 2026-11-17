from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.batch.major import current_major
from retro_speedlab_lakehouse.lake.catalog import (
    GAME_RUNS,
    GAME_SUMMARY,
    GAMES,
    HISTORY,
    IMPROVEMENT,
    LAB_OUTCOMES,
    LAB_RUNS,
    LEVEL_COSTS,
    RECORDS_DAILY,
    RUN_VERSIONS,
    SPLITS,
    Table,
)
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.time_travel import as_of
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings

DAY = timedelta(days=1)
SECONDS_PER_HOUR = 3600
DEPLOYED = "deployed"


def level_costs(splits: DataFrame) -> DataFrame:
    per_game = Window.partitionBy("game")
    costliest = Window.partitionBy("game").orderBy(F.col("frames").desc(), "position")
    return splits.select(
        "game",
        "level",
        "position",
        "frames",
        "level_seconds",
        "first_seconds",
        (F.col("first_seconds") - F.col("level_seconds")).alias("saved_seconds"),
        (F.col("level_seconds") / F.sum("level_seconds").over(per_game)).alias("share"),
        (F.row_number().over(costliest) == 1).alias("costliest"),
    )


def improvement_curve(history: DataFrame) -> DataFrame:
    hourly = history.withColumn("hour", F.date_trunc("hour", "at"))
    last = Window.partitionBy("game", "hour").orderBy(F.col("at").desc())
    return hourly.withColumn("newest", F.row_number().over(last)).where("newest = 1").select("game", "hour", "seconds")


def records_daily(versions: DataFrame) -> DataFrame:
    won = versions.where("won").withColumn("date", F.to_date("received_at"))
    daily = won.groupBy("game", "date").agg(F.min("seconds").alias("best_seconds"))
    running = Window.partitionBy("game").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ordered = Window.partitionBy("game").orderBy("date")
    records = daily.withColumn("record_seconds", F.min("best_seconds").over(running))
    return records.withColumn(
        "saved_seconds", F.coalesce(F.lag("record_seconds").over(ordered) - F.col("record_seconds"), F.lit(0.0))
    )


def lab_outcomes(lab_runs: DataFrame) -> DataFrame:
    return lab_runs.groupBy("game", "outcome").agg(
        F.count("*").alias("runs"),
        F.sum("minutes").alias("minutes"),
        F.sum("cost_usd").alias("cost_usd"),
        F.sum("commits").alias("commits"),
    )


def lab_totals(lab_runs: DataFrame) -> DataFrame:
    return lab_runs.groupBy("game").agg(
        F.count("*").alias("lab_runs"),
        F.sum((F.col("outcome") == DEPLOYED).cast("int")).alias("lab_deployed"),
        F.sum("minutes").alias("lab_minutes"),
    )


def history_totals(history: DataFrame) -> DataFrame:
    return history.groupBy("game").agg(
        F.min("at").alias("first_at"), F.max("at").alias("last_change_at"), F.count("*").alias("history_points")
    )


def game_summary(runs: DataFrame, costs: DataFrame, history: DataFrame, lab_runs: DataFrame) -> DataFrame:
    levels = costs.groupBy("game").agg(
        F.count("*").alias("levels_cleared"), F.sum("saved_seconds").alias("saved_since_first_clear_seconds")
    )
    costliest = costs.where("costliest").select(
        "game",
        F.col("level").alias("costliest_level"),
        F.col("frames").alias("costliest_frames"),
        F.col("share").alias("costliest_share"),
    )
    summary = (
        runs.select(
            "game",
            "title",
            "won",
            F.col("seconds").alias("run_seconds"),
            F.when(F.col("won"), F.col("seconds")).alias("record_seconds"),
            "frames",
            F.size(F.from_json(F.get_json_object("payload", "$.levels"), "array<struct<name:string>>")).alias("levels"),
            "run_updated_at",
        )
        .join(levels, "game", "left")
        .join(costliest, "game", "left")
        .join(history_totals(history), "game", "left")
        .join(lab_totals(lab_runs), "game", "left")
    )
    return summary.withColumn(
        "hours_searched", (F.unix_timestamp("run_updated_at") - F.unix_timestamp("first_at")) / SECONDS_PER_HOUR
    ).fillna(0, ["lab_runs", "lab_deployed", "lab_minutes", "levels_cleared"])


def with_yesterday(summary: DataFrame, yesterday: DataFrame | None) -> DataFrame:
    if yesterday is None:
        return summary.withColumn("saved_since_yesterday_seconds", F.lit(None).cast("double"))
    before = yesterday.select("game", F.col("record_seconds").alias("yesterday_record_seconds"))
    return (
        summary.join(before, "game", "left")
        .withColumn("saved_since_yesterday_seconds", F.col("yesterday_record_seconds") - F.col("record_seconds"))
        .drop("yesterday_record_seconds")
    )


def build_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    games = read(spark, GAMES, lake)

    def current(table: Table) -> DataFrame:
        return current_major(read(spark, table, lake), "version", games)

    costs = level_costs(current(SPLITS))
    history = current(HISTORY)
    lab_runs = read(spark, LAB_RUNS, lake)
    summary = game_summary(current(GAME_RUNS), costs, history, lab_runs)
    yesterday = as_of(spark, GAME_SUMMARY, lake, now - DAY) if exists(spark, GAME_SUMMARY, lake) else None
    tables = [
        (LEVEL_COSTS, costs),
        (IMPROVEMENT, improvement_curve(history)),
        (RECORDS_DAILY, records_daily(current(RUN_VERSIONS))),
        (LAB_OUTCOMES, lab_outcomes(lab_runs)),
        (GAME_SUMMARY, with_yesterday(summary, yesterday).withColumn("computed_at", F.lit(now))),
    ]
    written = 0
    for table, rows in tables:
        replace(table, lake, rows, gate)
        written += rows.count()
    return written
