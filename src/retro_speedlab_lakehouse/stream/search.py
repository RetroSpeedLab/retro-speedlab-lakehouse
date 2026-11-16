from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import CHAT_PULSE, LAB_STATUS, POLICY_PULSE, SEARCH_EVENTS, SEARCH_PULSE
from retro_speedlab_lakehouse.lake.tables import ensure, read
from retro_speedlab_lakehouse.lake.write import overwrite
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.stream import search_rows

SEARCH_APP = "retro-speedlab-lakehouse-search"
PULSES = (
    (SEARCH_PULSE, search_rows.search_pulse),
    (POLICY_PULSE, search_rows.policy_pulse),
    (LAB_STATUS, search_rows.lab_status),
    (CHAT_PULSE, search_rows.chat_pulse),
)


def bronze_append(settings: Settings, events: DataFrame, batch_id: int) -> None:
    bronze = search_rows.bronze_rows(events)
    ensure(SEARCH_EVENTS, settings.lake, bronze)
    (
        bronze.write.format("delta")
        .mode("append")
        .option("txnAppId", SEARCH_APP)
        .option("txnVersion", str(batch_id))
        .save(SEARCH_EVENTS.location(settings.lake))
    )


def recent(spark: SparkSession, settings: Settings, now: datetime) -> DataFrame:
    start = now - search_rows.HOUR
    events = read(spark, SEARCH_EVENTS, settings.lake)
    return events.where((F.col("date") >= F.lit(start.date())) & (F.col("event_at") >= F.lit(start)))


def search_batch(settings: Settings, gate: Gate, events: DataFrame, batch_id: int) -> None:
    if events.isEmpty():
        return
    spark = events.sparkSession
    bronze_append(settings, events, batch_id)
    now = datetime.now(UTC)
    window = recent(spark, settings, now).persist()
    for table, pulse in PULSES:
        counted = pulse(window, now)
        overwrite(table, settings.lake, spark.createDataFrame(counted.collect(), counted.schema), gate)
    window.unpersist()
    gate.flush(spark, settings.lake)
