import json
from datetime import UTC, datetime

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.lake.catalog import SOURCES, WATERMARKS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import append, merge
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.record import Source, SourcePage

RECORD = StructType(
    [
        StructField("source", StringType(), False),
        StructField("stream", StringType(), False),
        StructField("record_key", StringType(), False),
        StructField("body", StringType(), False),
        StructField("fetched_at", TimestampType(), False),
    ]
)
WATERMARK = StructType(
    [
        StructField("source", StringType(), False),
        StructField("stream", StringType(), False),
        StructField("watermark", StringType(), True),
        StructField("records", LongType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)


def watermark_of(spark: SparkSession, lake: LakeSettings, source: Source) -> str | None:
    if not exists(spark, WATERMARKS, lake):
        return None
    rows = read(spark, WATERMARKS, lake).where((F.col("source") == source.name) & (F.col("stream") == source.stream))
    first = rows.select("watermark").first()
    return None if first is None else first["watermark"]


def page_rows(spark: SparkSession, source: Source, page: SourcePage, fetched_at: datetime) -> DataFrame:
    records = [
        (
            source.name,
            source.stream,
            record.key,
            json.dumps(record.body, separators=(",", ":"), default=str),
            fetched_at,
        )
        for record in page.records
    ]
    return spark.createDataFrame(records, RECORD).withColumn("date", F.to_date("fetched_at"))


def ingest(spark: SparkSession, lake: LakeSettings, gate: Gate, source: Source) -> int:
    now = datetime.now(UTC)
    page = source.fetch(watermark_of(spark, lake, source), now)
    append(SOURCES, lake, page_rows(spark, source, page, now), gate)
    state = [(source.name, source.stream, page.watermark, len(page.records), now)]
    merge(WATERMARKS, lake, spark.createDataFrame(state, WATERMARK), gate)
    logger.info(f"{source.name}.{source.stream}: {len(page.records)} records, watermark {page.watermark}")
    return len(page.records)


def latest(spark: SparkSession, lake: LakeSettings, source: str, stream: str) -> DataFrame:
    rows = read(spark, SOURCES, lake).where((F.col("source") == source) & (F.col("stream") == stream))
    newest = rows.groupBy("record_key").agg(F.max("fetched_at").alias("fetched_at"))
    return rows.join(newest, ["record_key", "fetched_at"])
