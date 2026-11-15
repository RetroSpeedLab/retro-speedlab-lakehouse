from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from retro_speedlab_lakehouse.lake.catalog import RETENTION_PROPERTIES, Table
from retro_speedlab_lakehouse.settings import LakeSettings


def exists(spark: SparkSession, table: Table, lake: LakeSettings) -> bool:
    return DeltaTable.isDeltaTable(spark, table.location(lake))


def create(table: Table, lake: LakeSettings, rows: DataFrame) -> None:
    empty = rows.sparkSession.createDataFrame([], rows.schema)
    empty.write.format("delta").mode("errorifexists").partitionBy(*table.partitions).save(table.location(lake))
    properties = ", ".join(f"'{key}' = '{value}'" for key, value in RETENTION_PROPERTIES[table.retention].items())
    rows.sparkSession.sql(f"ALTER TABLE delta.`{table.location(lake)}` SET TBLPROPERTIES ({properties})")


def ensure(table: Table, lake: LakeSettings, rows: DataFrame) -> None:
    if not exists(rows.sparkSession, table, lake):
        create(table, lake, rows)


def append_rows(table: Table, lake: LakeSettings, rows: DataFrame) -> None:
    ensure(table, lake, rows)
    rows.write.format("delta").mode("append").option("mergeSchema", "true").save(table.location(lake))


def read(spark: SparkSession, table: Table, lake: LakeSettings) -> DataFrame:
    return spark.read.format("delta").load(table.location(lake))
