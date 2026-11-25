from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import SOURCES, SOURCES_STATUS, WATERMARKS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace_counted
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.record import Source


def sources_status(spark: SparkSession, lake: LakeSettings, configured: list[Source]) -> DataFrame:
    totals = read(spark, SOURCES, lake).groupBy("source", "stream").agg(F.count("*").alias("total_records"))
    state = read(spark, WATERMARKS, lake).select(
        "source",
        "stream",
        F.col("records").alias("last_records"),
        F.col("updated_at").alias("last_ingest_at"),
        "watermark",
    )
    active = spark.createDataFrame(
        list({(source.name, source.stream) for source in configured}), "source string, stream string"
    )
    return (
        state.join(active, ["source", "stream"]).join(totals, ["source", "stream"], "left").fillna(0, ["total_records"])
    )


def build_sources_status(spark: SparkSession, lake: LakeSettings, gate: Gate, configured: list[Source]) -> int:
    if not (exists(spark, WATERMARKS, lake) and exists(spark, SOURCES, lake)):
        return 0
    return replace_counted(SOURCES_STATUS, lake, sources_status(spark, lake, configured), gate)
