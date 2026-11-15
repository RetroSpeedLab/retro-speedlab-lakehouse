from dataclasses import dataclass
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.lake.catalog import (
    PLATFORM_JOBS,
    PLATFORM_TABLES,
    SOURCES,
    SOURCES_STATUS,
    WATERMARKS,
    Layer,
    Table,
)
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import merge, replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings

TABLE_STATS = StructType(
    [
        StructField("table", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("store", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("bytes", LongType(), False),
        StructField("files", LongType(), False),
        StructField("version", LongType(), False),
        StructField("modified_at", TimestampType(), False),
        StructField("measured_at", TimestampType(), False),
    ]
)
JOB_RUN = StructType(
    [
        StructField("dag", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("job", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("error", StringType(), True),
    ]
)
BRONZE_STORE = "black-omen"
SERVING_STORE = "lake"


@dataclass(frozen=True, slots=True)
class JobRun:
    dag: str
    run_id: str
    job: str
    started_at: datetime
    finished_at: datetime
    status: str
    rows: int
    error: str | None


def table_stats(spark: SparkSession, table: Table, lake: LakeSettings, now: datetime) -> tuple[object, ...]:
    location = table.location(lake)
    detail = DeltaTable.forPath(spark, location).detail().first()
    latest = DeltaTable.forPath(spark, location).history(1).first()
    if detail is None or latest is None:
        raise ValueError(f"{table.full_name} has no detail or history")
    store = BRONZE_STORE if table.layer is Layer.BRONZE else SERVING_STORE
    rows = read(spark, table, lake).count()
    return (
        table.full_name,
        str(table.layer),
        store,
        rows,
        detail["sizeInBytes"],
        detail["numFiles"],
        latest["version"],
        latest["timestamp"],
        now,
    )


def platform_tables(spark: SparkSession, tables: tuple[Table, ...], lake: LakeSettings, now: datetime) -> DataFrame:
    stats = [table_stats(spark, table, lake, now) for table in tables if exists(spark, table, lake)]
    return spark.createDataFrame(stats, TABLE_STATS)


def sources_status(spark: SparkSession, lake: LakeSettings) -> DataFrame:
    totals = read(spark, SOURCES, lake).groupBy("source", "stream").agg(F.count("*").alias("total_records"))
    state = read(spark, WATERMARKS, lake).select(
        "source",
        "stream",
        F.col("records").alias("last_records"),
        F.col("updated_at").alias("last_ingest_at"),
        "watermark",
    )
    return state.join(totals, ["source", "stream"], "left").fillna(0, ["total_records"])


def build_platform(
    spark: SparkSession, tables: tuple[Table, ...], lake: LakeSettings, gate: Gate, now: datetime
) -> int:
    stats = platform_tables(spark, tables, lake, now)
    replace(PLATFORM_TABLES, lake, stats, gate)
    written = stats.count()
    if exists(spark, WATERMARKS, lake) and exists(spark, SOURCES, lake):
        status = sources_status(spark, lake)
        replace(SOURCES_STATUS, lake, status, gate)
        written += status.count()
    return written


def record_job(spark: SparkSession, lake: LakeSettings, gate: Gate, run: JobRun) -> None:
    row = (run.dag, run.run_id, run.job, run.started_at, run.finished_at, run.status, run.rows, run.error)
    merge(PLATFORM_JOBS, lake, spark.createDataFrame([row], JOB_RUN), gate)
