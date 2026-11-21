from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.lake.catalog import LAB_ROI, LAB_RUNS, Table
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace_counted
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import LAB_RUN_ROUNDS, LAB_RUN_TRACES, LAB_TRACE_DAILY, SILVER_LAB_ROUNDS

COUNTS = (
    "cost_usd",
    "minutes",
    "turns",
    "tool_calls",
    "tool_errors",
    "subagents",
    "tokens",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
)
LAB_ROUND = StructType(
    [
        StructField("game", StringType(), False),
        StructField("stamp", StringType(), False),
        StructField("round", IntegerType(), False),
        StructField("outcome", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("minutes", DoubleType(), False),
        StructField("model", StringType(), True),
        StructField("cost_usd", DoubleType(), False),
        *(StructField(name, LongType(), False) for name in COUNTS[2:]),
    ]
)
NO_RUNS = "game string, stamp string, commits int"
NO_ROI = "game string, stamp string, frames_saved double, frames_per_dollar double"


def totals() -> list[F.Column]:
    return [F.sum(name).alias(name) for name in COUNTS]


def run_traces(rounds: DataFrame, runs: DataFrame, roi: DataFrame) -> DataFrame:
    first = F.min(F.struct("round", "model")).alias("first")
    traced = rounds.groupBy("game", "stamp", "outcome").agg(
        F.count("*").alias("rounds"), F.min("started_at").alias("started_at"), first, *totals()
    )
    return (
        traced.select("*", F.col("first.model").alias("model"))
        .drop("first")
        .join(runs.select("game", "stamp", "commits"), ["game", "stamp"], "left")
        .join(roi.select("game", "stamp", "frames_saved", "frames_per_dollar"), ["game", "stamp"], "left")
    )


def trace_days(runs: DataFrame) -> DataFrame:
    return runs.groupBy(F.to_date("started_at").alias("date"), "game").agg(
        F.count("*").alias("runs"), F.sum("rounds").alias("rounds"), *totals()
    )


def optional(spark: SparkSession, lake: LakeSettings, table: Table, empty: str) -> DataFrame:
    return read(spark, table, lake) if exists(spark, table, lake) else spark.createDataFrame([], empty)


def build_lab_traces_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SILVER_LAB_ROUNDS, lake):
        return 0
    stamped = F.lit(now).alias("computed_at")
    rounds = read(spark, SILVER_LAB_ROUNDS, lake).drop("measured_at")
    runs = run_traces(rounds, optional(spark, lake, LAB_RUNS, NO_RUNS), optional(spark, lake, LAB_ROI, NO_ROI))
    tables = ((LAB_RUN_ROUNDS, rounds), (LAB_RUN_TRACES, runs), (LAB_TRACE_DAILY, trace_days(runs)))
    return sum(replace_counted(table, lake, rows.select("*", stamped), gate) for table, rows in tables)
