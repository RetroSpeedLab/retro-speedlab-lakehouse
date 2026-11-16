from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, MapType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.lake.catalog import SOURCES
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import SILVER_DROPOFF, SILVER_LAB_COSTS, SILVER_NUMBERS, SILVER_PLAYS
from retro_speedlab_lakehouse.sources.registry import DROPOFF, LAB_COSTS, NUMBERS, PLAYS

NUMBER = StructType(
    [
        StructField("metric", StringType(), False),
        StructField("label", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("window", StringType(), False),
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


def build_source_silver(spark: SparkSession, lake: LakeSettings, gate: Gate) -> int:
    if not exists(spark, SOURCES, lake):
        return 0
    records = read(spark, SOURCES, lake)
    streams = [
        (SILVER_NUMBERS, newest(records, NUMBERS, NUMBER)),
        (SILVER_PLAYS, newest(records, PLAYS, MODE_PLAYS).drop("source")),
        (SILVER_DROPOFF, newest(records, DROPOFF, DROPOFF_COUNTS).drop("source")),
        (SILVER_LAB_COSTS, newest(records, LAB_COSTS, LAB_COST).drop("source")),
    ]
    for table, rows in streams:
        replace(table, lake, rows, gate)
    return sum(rows.count() for _, rows in streams)
