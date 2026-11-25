from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
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
DONATION = StructType(
    [
        StructField("message_id", StringType(), False),
        StructField("at", TimestampType(), False),
        StructField("kind", StringType(), False),
        StructField("public", BooleanType(), False),
        StructField("name", StringType(), True),
        StructField("message", StringType(), True),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("subscription_payment", BooleanType(), False),
        StructField("first_payment", BooleanType(), False),
    ]
)
ARTICLE_METRIC = StructType(
    [
        StructField("slug", StringType(), False),
        StructField("title", StringType(), False),
        StructField("tags", ArrayType(StringType()), False),
        StructField("channel", StringType(), False),
        StructField("url", StringType(), False),
        StructField("published", TimestampType(), False),
        StructField("views", LongType(), False),
        StructField("reactions", LongType(), False),
        StructField("comments", LongType(), False),
        StructField("measured", TimestampType(), False),
    ]
)
GPU = StructType(
    [
        StructField("index", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("utilization_percent", DoubleType(), False),
        StructField("memory_used_bytes", LongType(), False),
        StructField("memory_total_bytes", LongType(), False),
        StructField("temperature_celsius", DoubleType(), False),
        StructField("power_watts", DoubleType(), False),
    ]
)
HOST = StructType(
    [
        StructField("host", StringType(), False),
        StructField("measured_at", TimestampType(), False),
        StructField("disk_percent", DoubleType(), False),
        StructField("fullest_disk", StringType(), False),
        StructField("memory_percent", DoubleType(), False),
        StructField("memory_used_bytes", LongType(), False),
        StructField("memory_total_bytes", LongType(), False),
        StructField("load_1", DoubleType(), False),
        StructField("load_5", DoubleType(), False),
        StructField("load_15", DoubleType(), False),
        StructField("cpus", IntegerType(), False),
        StructField("cpu_percent", DoubleType(), False),
        StructField("gpus", ArrayType(GPU), False),
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
HOSTS_TOPIC = "hosts.metrics"
DONATIONS_TOPIC = "donations.events"
ARTICLE_METRICS_TOPIC = "articles.metrics"
LIVE_SLOT = "live"
STAMP_FORMAT = "yyyy-MM-dd'T'HHmm"
NON_FINITE = r":\s*-?(NaN|Infinity)\b"


def strict_json(value: Column) -> Column:
    return F.regexp_replace(value, NON_FINITE, ": null")


def parsed(kafka_rows: DataFrame) -> DataFrame:
    value = strict_json(F.col("value").cast("string"))
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
        field("game.version").alias("version"),
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


def donations(events: DataFrame) -> DataFrame:
    rows = newest(events.where(F.col("topic") == DONATIONS_TOPIC), ["key"])
    return rows.select(F.from_json("payload", DONATION).alias("donation"), "event_id").select("donation.*", "event_id")


def article_metrics(events: DataFrame) -> DataFrame:
    rows = events.where(F.col("topic") == ARTICLE_METRICS_TOPIC)
    parsed = rows.select(F.from_json("payload", ARTICLE_METRIC).alias("metric"), "event_id").select(
        "metric.*", "event_id"
    )
    return newest(parsed, ["slug", "channel", "measured"])


def host_rows(events: DataFrame) -> DataFrame:
    rows = events.where(F.col("topic") == HOSTS_TOPIC)
    parsed = rows.select(F.from_json("payload", HOST).alias("host"), "event_id", F.col("event_at").alias("received_at"))
    return parsed.select("host.*", "event_id", "received_at")


def host_status(events: DataFrame) -> DataFrame:
    return newest(host_rows(events).drop("gpus"), ["host"])


def host_history(events: DataFrame) -> DataFrame:
    return newest(host_rows(events).drop("gpus"), ["host", "measured_at"])


def gpu_history(events: DataFrame) -> DataFrame:
    gpus = host_rows(events).select("host", "measured_at", F.explode("gpus").alias("gpu"), "event_id", "received_at")
    flat = gpus.select("host", "measured_at", "gpu.*", "event_id", "received_at").withColumnRenamed("index", "gpu")
    return newest(flat, ["host", "gpu", "measured_at"])
