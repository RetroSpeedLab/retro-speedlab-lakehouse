from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from retro_speedlab_lakehouse.lake.catalog import (
    EVENTS,
    GAME_RUNS,
    HIGHSCORES,
    LAB_RUNS,
    LIVE_RUN,
    REPORTS,
    RUN_VERSIONS,
    Table,
)
from retro_speedlab_lakehouse.lake.tables import ensure
from retro_speedlab_lakehouse.lake.write import merge_newer
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.stream import rows
from retro_speedlab_lakehouse.stream.status import StreamStatus

LIVE_QUERY = "live"
ENTITIES_QUERY = "entities"
BRONZE_APP = "retro-speedlab-lakehouse-bronze"


def kafka_events(spark: SparkSession, settings: Settings, topics: list[str]) -> DataFrame:
    return rows.parsed(
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )


def write_if_any(table: Table, settings: Settings, gate: Gate, batch: DataFrame) -> None:
    if not batch.isEmpty():
        merge_newer(table, settings.lake, batch, gate)


def live_batch(settings: Settings, status: StreamStatus, events: DataFrame) -> None:
    if events.isEmpty():
        return
    run = rows.live_run(events)
    write_if_any(LIVE_RUN, settings, status.gate, run)
    status.observe(events)


def bronze_append(settings: Settings, events: DataFrame, batch_id: int) -> None:
    bronze = rows.bronze_events(events)
    ensure(EVENTS, settings.lake, bronze)
    (
        bronze.write.format("delta")
        .mode("append")
        .option("txnAppId", BRONZE_APP)
        .option("txnVersion", str(batch_id))
        .save(EVENTS.location(settings.lake))
    )


def entities_batch(settings: Settings, gate: Gate, events: DataFrame, batch_id: int) -> None:
    if events.isEmpty():
        return
    events.persist()
    bronze_append(settings, events, batch_id)
    for table, transform in SERVED[1:]:
        write_if_any(table, settings, gate, transform(events))
    gate.flush(events.sparkSession, settings.lake)
    events.unpersist()


SERVED = (
    (LIVE_RUN, rows.live_run),
    (RUN_VERSIONS, rows.run_versions),
    (GAME_RUNS, rows.game_runs),
    (REPORTS, rows.reports),
    (LAB_RUNS, rows.lab_runs),
    (HIGHSCORES, rows.highscores),
)


def ensure_served(spark: SparkSession, settings: Settings) -> None:
    nothing = rows.parsed(spark.createDataFrame([], rows.KAFKA_ROW))
    for table, transform in SERVED:
        ensure(table, settings.lake, transform(nothing))


def start(spark: SparkSession, settings: Settings) -> list[StreamingQuery]:
    ensure_served(spark, settings)
    status = StreamStatus(settings, Gate(LIVE_QUERY))
    entities_gate = Gate(ENTITIES_QUERY)
    live = (
        kafka_events(spark, settings, [rows.LIVE_TOPIC])
        .writeStream.queryName(LIVE_QUERY)
        .option("checkpointLocation", str(settings.lake.checkpoints / LIVE_QUERY))
        .trigger(processingTime=f"{settings.stream.live_trigger_seconds} seconds")
        .foreachBatch(lambda events, _: live_batch(settings, status, events))
        .start()
    )
    entities = (
        kafka_events(spark, settings, settings.kafka.backend_topics)
        .writeStream.queryName(ENTITIES_QUERY)
        .option("checkpointLocation", str(settings.lake.checkpoints / ENTITIES_QUERY))
        .trigger(processingTime=f"{settings.stream.silver_trigger_seconds} seconds")
        .foreachBatch(lambda events, batch_id: entities_batch(settings, entities_gate, events, batch_id))
        .start()
    )
    return [live, entities]
