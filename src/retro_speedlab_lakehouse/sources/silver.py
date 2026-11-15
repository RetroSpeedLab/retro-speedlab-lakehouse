from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from retro_speedlab_lakehouse.lake.catalog import SOURCES
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import SILVER_NUMBERS
from retro_speedlab_lakehouse.sources.registry import NUMBERS

NUMBER = StructType(
    [
        StructField("metric", StringType(), False),
        StructField("label", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("window", StringType(), False),
    ]
)


def newest_numbers(records: DataFrame) -> DataFrame:
    numbers = records.where(F.col("stream") == NUMBERS)
    newest = numbers.groupBy("source").agg(F.max("fetched_at").alias("fetched_at"))
    return (
        numbers.join(newest, ["source", "fetched_at"])
        .select("source", F.from_json("body", NUMBER).alias("number"), F.col("fetched_at").alias("measured_at"))
        .select("source", "number.*", "measured_at")
    )


def build_source_silver(spark: SparkSession, lake: LakeSettings, gate: Gate) -> int:
    if not exists(spark, SOURCES, lake):
        return 0
    numbers = newest_numbers(read(spark, SOURCES, lake))
    replace(SILVER_NUMBERS, lake, numbers, gate)
    return numbers.count()
