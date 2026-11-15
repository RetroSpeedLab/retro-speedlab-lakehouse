from loguru import logger
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.lake.catalog import Table
from retro_speedlab_lakehouse.lake.tables import exists
from retro_speedlab_lakehouse.settings import LakeSettings


def register(spark: SparkSession, tables: tuple[Table, ...], lake: LakeSettings) -> int:
    for layer in sorted({table.layer for table in tables}):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
    present = [table for table in tables if exists(spark, table, lake)]
    for table in present:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table.full_name} USING DELTA LOCATION '{table.location(lake)}'")
    logger.info(f"{len(present)} of {len(tables)} tables are registered")
    return len(present)
