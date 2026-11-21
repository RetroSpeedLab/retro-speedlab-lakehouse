from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.lake.write import merge
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.tracking.catalog import AIM_METRICS, AIM_RUNS, KIND_MINUTES, PPO_ROUNDS, TURN_MINUTES

SEARCH_TURN = "search_turn"
PPO_ROUND = "ppo_round"
NO_CONTEXT = "{}"
POLICY = "policy/"
TURN_METRICS = ("frames_saved", "best_seconds", "new_records", "crossings", "attempts_per_second", "stuck_states")
KIND_METRICS = ("frames_saved_per_worker_second", "worker_share")
PPO_METRICS = ("reward", "exits", "episodes", "exit_rate", "entropy", "explained_variance", "loss")
PARAMS = StructType(
    [
        StructField("kind", StringType(), True),
        StructField("game", StringType(), True),
        StructField("game_version", StringType(), True),
        StructField("release", StringType(), True),
        StructField("turn", LongType(), True),
        StructField("arm", StringType(), True),
        StructField("workers", MapType(StringType(), LongType()), True),
    ]
)
RUN_COLUMNS = ("run", "game", "game_version", "release", "started_at", "ended_at")


def runs_of(spark: SparkSession, lake: LakeSettings, fetched: list[str], kind: str) -> DataFrame:
    runs = read(spark, AIM_RUNS, lake).where(F.col("run").isin(fetched))
    parsed = runs.select("run", "created_at", "ended_at", F.from_json("params", PARAMS).alias("params"))
    typed = parsed.select("run", "params.*", F.col("created_at").alias("started_at"), "ended_at")
    return typed.where(F.col("kind") == kind).drop("kind")


def pivoted(points: DataFrame, keys: list[str], metrics: tuple[str, ...]) -> DataFrame:
    return points.groupBy(*keys).pivot("metric", list(metrics)).agg(F.first("value"))


def turn_minutes(points: DataFrame, turns: DataFrame) -> DataFrame:
    own = pivoted(points.where(F.col("context") == NO_CONTEXT), ["run", "step"], TURN_METRICS)
    return turns.drop("workers").join(own, "run")


def kind_minutes(points: DataFrame, turns: DataFrame) -> DataFrame:
    per_kind = points.where(F.col("context") != NO_CONTEXT).withColumn("kind", F.get_json_object("context", "$.kind"))
    own = pivoted(per_kind, ["run", "kind", "step"], KIND_METRICS)
    attributes = turns.select(*RUN_COLUMNS, "turn", "arm", "workers")
    return attributes.join(own, "run").withColumn("workers", F.element_at("workers", F.col("kind")))


def ppo_rounds(points: DataFrame, runs: DataFrame) -> DataFrame:
    named = points.where(F.col("metric").startswith(POLICY)).withColumn(
        "metric", F.expr(f"substr(metric, {len(POLICY) + 1})")
    )
    own = pivoted(named, ["run", "step"], PPO_METRICS).withColumnRenamed("step", "round")
    return runs.select(*RUN_COLUMNS).join(own, "run")


def build_tracking_silver(spark: SparkSession, lake: LakeSettings, gate: Gate, fetched: list[str]) -> None:
    points = read(spark, AIM_METRICS, lake).where(F.col("run").isin(fetched)).persist()
    turns = runs_of(spark, lake, fetched, SEARCH_TURN)
    merge(TURN_MINUTES, lake, turn_minutes(points, turns), gate)
    merge(KIND_MINUTES, lake, kind_minutes(points, turns), gate)
    merge(PPO_ROUNDS, lake, ppo_rounds(points, runs_of(spark, lake, fetched, PPO_ROUND)), gate)
    points.unpersist()
