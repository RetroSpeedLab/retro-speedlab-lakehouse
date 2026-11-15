from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings


def build_source_silver(spark: SparkSession, lake: LakeSettings, gate: Gate) -> int:
    return 0
