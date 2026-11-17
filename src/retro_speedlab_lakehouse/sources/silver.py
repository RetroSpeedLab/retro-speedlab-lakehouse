from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retro_speedlab_lakehouse.lake.catalog import SOURCES
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.bronze import latest
from retro_speedlab_lakehouse.sources.catalog import (
    LANGFUSE_DAILY,
    SILVER_DROPOFF,
    SILVER_ITCH_DAILY,
    SILVER_ITCH_PLAYS,
    SILVER_LAB_COSTS,
    SILVER_NUMBERS,
    SILVER_PLAYS,
)
from retro_speedlab_lakehouse.sources.registry import DAILY, DROPOFF, LAB_COSTS, NUMBERS, PLAYS

NUMBER = StructType(
    [
        StructField("metric", StringType(), False),
        StructField("label", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("window", StringType(), False),
    ]
)
DAILY_COST = StructType(
    [
        StructField("date", DateType(), False),
        StructField("observations", LongType(), False),
        StructField("tokens", LongType(), False),
        StructField("cost_usd", DoubleType(), False),
    ]
)
COUNTS = MapType(StringType(), LongType(), False)
MODE_PLAYS = StructType(
    [
        StructField("mode", StringType(), False),
        StructField("window", StringType(), False),
        StructField("started", COUNTS, False),
        StructField("won", COUNTS, False),
    ]
)

DROPOFF_COUNTS = StructType(
    [
        StructField("game", StringType(), False),
        StructField("ended", COUNTS, False),
        StructField("won", LongType(), False),
        StructField("since", TimestampType(), False),
    ]
)
ITCH_DAY = StructType(
    [
        StructField("date", DateType(), False),
        StructField("pageviews", LongType(), False),
        StructField("visitors", LongType(), False),
        StructField("views", COUNTS, False),
        StructField("campaign", COUNTS, False),
        StructField("endless", COUNTS, False),
        StructField("cleared", COUNTS, False),
        StructField("won", COUNTS, False),
    ]
)
LAB_COST = StructType(
    [
        StructField("stamp", StringType(), False),
        StructField("observations", LongType(), False),
        StructField("tokens", LongType(), False),
        StructField("cost_usd", DoubleType(), False),
    ]
)


def newest(records: DataFrame, stream: str, schema: StructType) -> DataFrame:
    rows = records.where(F.col("stream") == stream)
    latest = rows.groupBy("source").agg(F.max("fetched_at").alias("fetched_at"))
    return (
        rows.join(latest, ["source", "fetched_at"])
        .select("source", F.from_json("body", schema).alias("item"), F.col("fetched_at").alias("measured_at"))
        .select("source", "item.*", "measured_at")
    )


def plays_of(records: DataFrame, source: str) -> DataFrame:
    return newest(records.where(F.col("source") == source), PLAYS, MODE_PLAYS).drop("source")


def daily(days: DataFrame, schema: StructType) -> DataFrame:
    return days.select(F.from_json("body", schema).alias("day"), F.col("fetched_at").alias("measured_at")).select(
        "day.*", "measured_at"
    )


def build_source_silver(spark: SparkSession, lake: LakeSettings, gate: Gate) -> int:
    if not exists(spark, SOURCES, lake):
        return 0
    records = read(spark, SOURCES, lake)
    streams = [
        (SILVER_NUMBERS, newest(records, NUMBERS, NUMBER)),
        (SILVER_PLAYS, plays_of(records, "umami")),
        (SILVER_ITCH_PLAYS, plays_of(records, "itch")),
        (SILVER_DROPOFF, newest(records, DROPOFF, DROPOFF_COUNTS).drop("source")),
        (SILVER_LAB_COSTS, newest(records, LAB_COSTS, LAB_COST).drop("source")),
        (LANGFUSE_DAILY, daily(latest(spark, lake, "langfuse", DAILY), DAILY_COST)),
        (SILVER_ITCH_DAILY, daily(latest(spark, lake, "itch", DAILY), ITCH_DAY)),
    ]
    for table, rows in streams:
        replace(table, lake, rows, gate)
    return sum(rows.count() for _, rows in streams)
