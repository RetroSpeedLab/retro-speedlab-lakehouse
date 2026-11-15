from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.database.register import register
from retro_speedlab_lakehouse.lake.catalog import CORE_TABLES, GAME_SUMMARY
from retro_speedlab_lakehouse.settings import Settings

RECORD_SECONDS = 61.5


def test_register_names_every_present_table_once(spark: SparkSession, settings: Settings) -> None:
    record = spark.createDataFrame([("comet_courier", RECORD_SECONDS)], "game string, record_seconds double")
    record.write.format("delta").save(GAME_SUMMARY.location(settings.lake))
    assert register(spark, CORE_TABLES, settings.lake) == 1
    assert register(spark, CORE_TABLES, settings.lake) == 1
    row = spark.table(GAME_SUMMARY.full_name).first()
    assert row is not None and row["record_seconds"] == RECORD_SECONDS
