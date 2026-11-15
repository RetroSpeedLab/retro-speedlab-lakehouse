from datetime import datetime

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from retro_speedlab_lakehouse.lake.catalog import Table
from retro_speedlab_lakehouse.settings import LakeSettings

BEFORE_HISTORY = "DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION"


def as_of(spark: SparkSession, table: Table, lake: LakeSettings, moment: datetime) -> DataFrame | None:
    try:
        rows = spark.read.format("delta").option("timestampAsOf", moment.isoformat()).load(table.location(lake))
        rows.schema
    except AnalysisException as error:
        if error.getCondition() == BEFORE_HISTORY:
            return None
        raise
    return rows
