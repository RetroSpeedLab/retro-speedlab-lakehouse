from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import SILVER_NUMBERS, SOURCE_NUMBERS

FRESH = timedelta(days=2)


def build_source_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SILVER_NUMBERS, lake):
        return 0
    fresh = read(spark, SILVER_NUMBERS, lake).where(F.col("measured_at") >= F.lit(now - FRESH))
    numbers = fresh.withColumn("computed_at", F.lit(now))
    replace(SOURCE_NUMBERS, lake, numbers, gate)
    return numbers.count()
