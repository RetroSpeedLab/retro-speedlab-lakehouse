from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, MapType, StringType, StructField, StructType

from retro_speedlab_lakehouse.lake.catalog import SOURCES
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import SILVER_NUMBERS, SILVER_PLAYS
from retro_speedlab_lakehouse.sources.registry import NUMBERS, PLAYS

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
    numbers = newest(records, NUMBERS, NUMBER)
    plays = newest(records, PLAYS, MODE_PLAYS).drop("source")
    replace(SILVER_NUMBERS, lake, numbers, gate)
    replace(SILVER_PLAYS, lake, plays, gate)
    return numbers.count() + plays.count()
