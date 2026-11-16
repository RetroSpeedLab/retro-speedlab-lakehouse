from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.bandit.comparison import BANDIT, HOLDOUT, METRIC, Comparison, Turn, compared
from retro_speedlab_lakehouse.experiments.analysis import HIGHER
from retro_speedlab_lakehouse.lake.catalog import SEARCH_BANDIT, SEARCH_EVENTS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.stream.search_rows import ALLOCATIONS, IMPROVED, STEPS, field, newest

RESULT = StructType(
    [
        StructField("metric", StringType(), False),
        StructField("better", StringType(), False),
        StructField("control_n", LongType(), False),
        StructField("treatment_n", LongType(), False),
        StructField("enough", BooleanType(), False),
        StructField("control_estimate", DoubleType(), True),
        StructField("treatment_estimate", DoubleType(), True),
        StructField("effect", DoubleType(), True),
        StructField("ci_lower", DoubleType(), True),
        StructField("ci_upper", DoubleType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("p_value", DoubleType(), True),
        StructField("verdict", StringType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


def finished_turns(events: DataFrame, now: datetime) -> DataFrame:
    allocated = events.where(F.col("topic") == ALLOCATIONS).withColumn("turn", field("turn").cast("long"))
    latest = newest(allocated, [F.col("key"), F.col("turn")])
    return latest.select(
        F.col("key").alias("game"),
        "turn",
        field("arm").alias("arm"),
        F.to_timestamp(field("turn_started_at")).alias("started_at"),
        F.to_timestamp(field("turn_ends_at")).alias("ends_at"),
    ).where(F.col("arm").isin(BANDIT, HOLDOUT) & (F.col("ends_at") <= F.lit(now)))


def turn_savings(events: DataFrame, turns: DataFrame) -> DataFrame:
    improved = events.where((F.col("topic") == STEPS) & (field("kind") == IMPROVED)).select(
        F.col("key").alias("step_game"), "event_at", field("saved").cast("long").alias("saved")
    )
    within = (
        (improved.step_game == turns.game)
        & (improved.event_at >= turns.started_at)
        & (improved.event_at < turns.ends_at)
    )
    saved = improved.join(F.broadcast(turns), within).groupBy("game", "turn").agg(F.sum("saved").alias("saved"))
    return (
        turns.join(saved, ["game", "turn"], "left")
        .withColumn("saved", F.coalesce(F.col("saved"), F.lit(0)))
        .withColumn("seconds", F.col("ends_at").cast("double") - F.col("started_at").cast("double"))
    )


def result_row(comparison: Comparison, now: datetime) -> tuple[object, ...]:
    sizes = (METRIC, HIGHER, comparison.holdout, comparison.bandit, comparison.result is not None)
    result = comparison.result
    if result is None:
        return (*sizes, None, None, None, None, None, None, None, comparison.verdict, now)
    interval = result.confidence_interval
    estimates = (
        result.control_estimate,
        result.treatment_estimate,
        result.absolute_effect,
        interval.lower,
        interval.upper,
        interval.confidence_level,
        result.p_value,
    )
    return (*sizes, *estimates, comparison.verdict, now)


def build_bandit_comparison(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SEARCH_EVENTS, lake):
        return 0
    events = read(spark, SEARCH_EVENTS, lake)
    rows = turn_savings(events, finished_turns(events, now)).collect()
    turns = [Turn(row["game"], row["turn"], row["arm"], row["saved"], row["seconds"]) for row in rows]
    replace(SEARCH_BANDIT, lake, spark.createDataFrame([result_row(compared(turns), now)], RESULT), gate)
    return len(turns)
