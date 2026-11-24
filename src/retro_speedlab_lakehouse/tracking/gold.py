from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace_counted
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.tracking.catalog import (
    KIND_MINUTES,
    PPO_ROUNDS,
    SEARCH_TURNS,
    SEARCH_WORKER_KINDS,
    TRAINING_ROUNDS,
    TURN_MINUTES,
)
from retro_speedlab_lakehouse.tracking.silver import HYPERPARAMETERS, MEASURED_AT

MINUTES_PER_HOUR = 60
TREND_ROUNDS = 10


def turn_attributes() -> list[F.Column]:
    return [
        F.max("arm").alias("arm"),
        F.max_by("game_version", "started_at").alias("game_version"),
        F.min("started_at").alias("started_at"),
        F.max("ended_at").alias("ended_at"),
        F.bool_or(F.col("ended_at").isNull()).alias("running"),
        F.count("*").alias("minutes"),
    ]


def search_turns(minutes: DataFrame, now: datetime) -> DataFrame:
    turns = minutes.groupBy("game", "turn").agg(
        *turn_attributes(),
        F.sum("frames_saved").cast("long").alias("frames_saved"),
        F.avg("attempts_per_second").alias("attempts_per_second"),
        F.min("best_seconds").alias("best_seconds"),
        F.sum("new_records").cast("long").alias("new_records"),
        F.sum("crossings").cast("long").alias("crossings"),
        F.max("stuck_states").cast("long").alias("stuck_states"),
    )
    per_hour = F.col("frames_saved") * MINUTES_PER_HOUR / F.col("minutes")
    return turns.withColumn("frames_saved_per_hour", per_hour).withColumn("computed_at", F.lit(now))


def worker_kinds(minutes: DataFrame, now: datetime) -> DataFrame:
    weighted = F.sum(F.col("frames_saved_per_worker_second") * F.col("worker_share")) / F.sum("worker_share")
    return (
        minutes.groupBy("game", "turn", "kind")
        .agg(
            *turn_attributes(),
            F.max("workers").alias("workers"),
            weighted.alias("frames_saved_per_worker_second"),
            F.avg("worker_share").alias("worker_share"),
        )
        .withColumn("computed_at", F.lit(now))
    )


def training_rounds(rounds: DataFrame, now: datetime) -> DataFrame:
    newest = rounds.groupBy("game").agg(F.max_by("run", "started_at").alias("run"))
    trend = Window.partitionBy("game").orderBy("round").rowsBetween(1 - TREND_ROUNDS, Window.currentRow)
    return (
        rounds.join(newest, ["game", "run"])
        .drop(HYPERPARAMETERS, MEASURED_AT)
        .withColumn("reward_trend", F.avg("reward").over(trend))
        .withColumn("computed_at", F.lit(now))
    )


def build_tracking_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    built = (
        (TURN_MINUTES, SEARCH_TURNS, search_turns),
        (KIND_MINUTES, SEARCH_WORKER_KINDS, worker_kinds),
        (PPO_ROUNDS, TRAINING_ROUNDS, training_rounds),
    )
    return sum(
        replace_counted(gold, lake, build(read(spark, silver, lake), now), gate)
        for silver, gold, build in built
        if exists(spark, silver, lake)
    )
