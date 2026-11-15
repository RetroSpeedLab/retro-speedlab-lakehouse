from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ENVELOPE = StructType(
    [
        StructField("id", LongType(), False),
        StructField("topic", StringType(), False),
        StructField("key", StringType(), False),
        StructField("at", TimestampType(), False),
    ]
)
REPORT = StructType(
    [
        StructField("id", LongType(), False),
        StructField("headline", StringType(), False),
        StructField("lines", ArrayType(StringType()), False),
        StructField("created", TimestampType(), False),
        StructField("updated", TimestampType(), False),
    ]
)
LAB_RUN = StructType(
    [
        StructField("outcome", StringType(), False),
        StructField("cost_usd", DoubleType(), False),
        StructField("minutes", DoubleType(), False),
        StructField("turns", IntegerType(), False),
        StructField("subagents", IntegerType(), False),
        StructField("commits", IntegerType(), False),
        StructField("tool_errors", IntegerType(), False),
    ]
)
HIGHSCORE = StructType(
    [
        StructField("id", LongType(), False),
        StructField("game", StringType(), False),
        StructField("version", StringType(), False),
        StructField("name", StringType(), False),
        StructField("frames", LongType(), False),
        StructField("set_at", TimestampType(), False),
    ]
)
KAFKA_ROW = StructType(
    [
        StructField("value", BinaryType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("timestamp", TimestampType(), False),
    ]
)
RUN_TOPICS = ("runs.live", "runs.snapshots")
LIVE_TOPIC = "runs.live"
REPORTS_TOPIC = "lab.reports"
LAB_RUNS_TOPIC = "lab.runs"
HIGHSCORES_TOPIC = "highscores.entries"
LIVE_SLOT = "live"
STAMP_FORMAT = "yyyy-MM-dd'T'HHmm"


def parsed(kafka_rows: DataFrame) -> DataFrame:
    value = F.col("value").cast("string")
    return kafka_rows.select(
        F.from_json(value, ENVELOPE).alias("event"),
        F.get_json_object(value, "$.payload").alias("payload"),
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        F.col("event.id").alias("event_id"),
        F.col("event.topic").alias("topic"),
        F.col("event.key").alias("key"),
        F.col("event.at").alias("event_at"),
        "payload",
        "partition",
        "offset",
        "kafka_timestamp",
    )


def bronze_events(events: DataFrame) -> DataFrame:
    return events.withColumn("ingested_at", F.current_timestamp()).withColumn("date", F.to_date("event_at"))


def newest(events: DataFrame, keys: list[str]) -> DataFrame:
    order = Window.partitionBy(*keys).orderBy(F.col("event_id").desc())
    return events.withColumn("newest", F.row_number().over(order)).where("newest = 1").drop("newest")


def field(path: str) -> Column:
    return F.get_json_object("payload", f"$.{path}")


def run_columns(events: DataFrame) -> DataFrame:
    return events.select(
        F.col("key").alias("game"),
        "event_id",
        "event_at",
        field("game.title").alias("title"),
        field("won").cast("boolean").alias("won"),
        field("seconds").cast("double").alias("seconds"),
        field("frames").cast("long").alias("frames"),
        field("frame_rate").cast("double").alias("frame_rate"),
        field("reached").alias("reached"),
        field("searching").alias("searching"),
        F.to_timestamp(field("updated_at")).alias("run_updated_at"),
        "payload",
    )


def live_run(events: DataFrame) -> DataFrame:
    live = newest(events.where(F.col("topic") == LIVE_TOPIC), [])
    return live.select(
        F.lit(LIVE_SLOT).alias("slot"),
        F.col("key").alias("game"),
        "event_id",
        "event_at",
        F.to_timestamp(field("updated_at")).alias("run_updated_at"),
        "payload",
        F.current_timestamp().alias("committed_at"),
    )


def runs(events: DataFrame) -> DataFrame:
    return run_columns(events.where(F.col("topic").isin(*RUN_TOPICS)))


def run_versions(events: DataFrame) -> DataFrame:
    return runs(events).drop("payload").withColumnRenamed("event_at", "received_at")


def game_runs(events: DataFrame) -> DataFrame:
    return newest(runs(events), ["game"])


def reports(events: DataFrame) -> DataFrame:
    rows = newest(events.where(F.col("topic") == REPORTS_TOPIC), ["key"])
    return rows.select(
        field("game").alias("game"),
        field("name").alias("name"),
        F.from_json("payload", REPORT).alias("report"),
        "event_id",
    ).select("game", "name", "report.*", "event_id")


def lab_runs(events: DataFrame) -> DataFrame:
    rows = newest(events.where(F.col("topic") == LAB_RUNS_TOPIC), ["key"])
    return rows.select(
        field("game").alias("game"),
        field("stamp").alias("stamp"),
        F.to_timestamp(field("stamp"), STAMP_FORMAT).alias("started_at"),
        F.from_json("payload", LAB_RUN).alias("run"),
        "event_id",
    ).select("game", "stamp", "started_at", "run.*", "event_id")


def highscores(events: DataFrame) -> DataFrame:
    rows = newest(events.where(F.col("topic") == HIGHSCORES_TOPIC), ["key"])
    return rows.select(F.from_json("payload", HIGHSCORE).alias("entry"), "event_id").select("entry.*", "event_id")
