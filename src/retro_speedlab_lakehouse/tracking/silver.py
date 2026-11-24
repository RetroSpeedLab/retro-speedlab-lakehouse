from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retro_speedlab_lakehouse.lake.write import merge
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.tracking.catalog import KIND_MINUTES, PPO_ROUNDS, TURN_MINUTES

TURN_MINUTES_TOPIC = "runs.turn_minutes"
POLICY_ROUNDS_TOPIC = "policy.rounds"
WINDOW = timedelta(days=2)
STALE = timedelta(hours=1)
TURN_METRICS = ("frames_saved", "best_seconds", "new_records", "crossings", "attempts_per_second", "stuck_states")
KIND_METRICS = ("frames_saved_per_worker_second", "worker_share")
PPO_METRICS = ("reward", "exits", "episodes", "exit_rate", "entropy", "explained_variance", "loss")
HYPERPARAMETERS = "hyperparameters"
MEASURED_AT = "measured_at"
RUN_COLUMNS = ("run", "game", "game_version", "release", "started_at", "ended_at", MEASURED_AT)
TURN_COLUMNS = (*RUN_COLUMNS, "turn", "arm")


def doubles(names: tuple[str, ...]) -> list[StructField]:
    return [StructField(name, DoubleType(), True) for name in names]


RUN = [
    StructField("run", StringType(), True),
    StructField("game", StringType(), True),
    StructField("game_version", StringType(), True),
    StructField("release", StringType(), True),
    StructField("started_at", TimestampType(), True),
]
TURN_MINUTE = StructType(
    [
        *RUN,
        StructField("turn", LongType(), True),
        StructField("arm", StringType(), True),
        StructField("workers", MapType(StringType(), LongType()), True),
        StructField("step", LongType(), True),
        *doubles(TURN_METRICS),
        StructField("kinds", MapType(StringType(), StructType(doubles(KIND_METRICS))), True),
        StructField("final", BooleanType(), True),
    ]
)
PPO_ROUND = StructType(
    [
        *RUN,
        StructField("round", LongType(), True),
        *doubles(PPO_METRICS),
        StructField(HYPERPARAMETERS, StringType(), True),
    ]
)


def payloads(events: DataFrame, topic: str, schema: StructType, key: str, now: datetime) -> DataFrame:
    recent = events.where((F.col("topic") == topic) & (F.col("date") >= F.lit((now - WINDOW).date())))
    parsed = recent.select(F.from_json("payload", schema).alias("payload"), "event_at").select("payload.*", "event_at")
    tracked_in_aim = F.col("started_at").isNull()
    return parsed.where(~tracked_in_aim).dropDuplicates(["run", key])


def ended(rows: DataFrame, done: Column, now: datetime) -> DataFrame:
    run = Window.partitionBy("run")
    last = F.max("event_at").over(run)
    over = F.max(done).over(run) | (last < F.lit(now - STALE))
    return rows.withColumn("ended_at", F.when(over, last)).withColumnRenamed("event_at", MEASURED_AT)


def turn_minutes(minutes: DataFrame) -> DataFrame:
    return minutes.select(*TURN_COLUMNS, "step", *TURN_METRICS)


def kind_minutes(minutes: DataFrame) -> DataFrame:
    kinds = minutes.select(*TURN_COLUMNS, "workers", "step", F.explode("kinds").alias("kind", "metrics"))
    return kinds.select(
        *TURN_COLUMNS,
        "step",
        "kind",
        F.element_at("workers", F.col("kind")).alias("workers"),
        *(F.col(f"metrics.{name}").alias(name) for name in KIND_METRICS),
    )


def ppo_rounds(rounds: DataFrame) -> DataFrame:
    return rounds.select(*RUN_COLUMNS, "round", *PPO_METRICS, HYPERPARAMETERS)


def build_tracking_silver(lake: LakeSettings, gate: Gate, events: DataFrame, now: datetime) -> None:
    minutes = ended(payloads(events, TURN_MINUTES_TOPIC, TURN_MINUTE, "step", now), F.col("final"), now).persist()
    merge(TURN_MINUTES, lake, turn_minutes(minutes), gate)
    merge(KIND_MINUTES, lake, kind_minutes(minutes), gate)
    minutes.unpersist()
    rounds = ended(payloads(events, POLICY_ROUNDS_TOPIC, PPO_ROUND, "round", now), F.lit(False), now)
    merge(PPO_ROUNDS, lake, ppo_rounds(rounds), gate)
