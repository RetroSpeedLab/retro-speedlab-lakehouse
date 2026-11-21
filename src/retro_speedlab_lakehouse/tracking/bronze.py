from dataclasses import astuple
from datetime import datetime, timedelta

import httpx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import merge
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.tracking.aim import AimPoint, AimRun, listed_runs, moment
from retro_speedlab_lakehouse.tracking.catalog import AIM_METRICS, AIM_RUNS

OVERLAP = timedelta(hours=1)
RUN = StructType(
    [
        StructField("run", StringType(), False),
        StructField("experiment", StringType(), False),
        StructField("name", StringType(), False),
        StructField("params", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), True),
    ]
)
POINT = StructType(
    [
        StructField("run", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("context", StringType(), False),
        StructField("step", LongType(), False),
        StructField("value", DoubleType(), False),
    ]
)


def newest_created(runs: DataFrame) -> datetime | None:
    newest = runs.select(F.max(F.col("created_at").cast("double"))).first()
    return None if newest is None or newest[0] is None else moment(newest[0])


def hashes(rows: DataFrame) -> set[str]:
    return {row["run"] for row in rows.select("run").collect()}


def due_runs(
    spark: SparkSession, lake: LakeSettings, session: httpx.Client, now: datetime, open_for: timedelta
) -> list[str]:
    if not exists(spark, AIM_RUNS, lake):
        return listed_runs(session, None)
    runs = read(spark, AIM_RUNS, lake)
    newest = newest_created(runs)
    listed = listed_runs(session, None if newest is None else newest - OVERLAP)
    ended = hashes(runs.where(F.col("run").isin(listed) & F.col("ended_at").isNotNull()))
    still_open = hashes(runs.where(F.col("ended_at").isNull() & (F.col("created_at") >= F.lit(now - open_for))))
    return sorted((set(listed) | still_open) - ended)


def write_bronze(
    spark: SparkSession, lake: LakeSettings, gate: Gate, runs: list[AimRun], points: list[AimPoint], now: datetime
) -> None:
    created = spark.createDataFrame([astuple(run) for run in runs], RUN).withColumn("date", F.to_date("created_at"))
    merge(AIM_RUNS, lake, created.withColumn("fetched_at", F.lit(now)), gate)
    tracked = spark.createDataFrame([astuple(point) for point in points], POINT)
    dated = tracked.join(created.select("run", "date"), "run").withColumn("fetched_at", F.lit(now))
    merge(AIM_METRICS, lake, dated, gate)
