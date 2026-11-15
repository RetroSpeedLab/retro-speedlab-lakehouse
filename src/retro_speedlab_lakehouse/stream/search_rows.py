from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

ATTEMPTS = "runs.attempts"
STEPS = "runs.steps"
RECORDS = "runs.records"
POLICY_ROUNDS = "policy.rounds"
LAB_EVENTS = "lab.events"
CHAT_EVENTS = "chat.events"
IMPROVED = "improved"
CROSSING = "crossing"
LAB_SLOT = "lab"
HOUR = timedelta(hours=1)
TEN_MINUTES = timedelta(minutes=10)
MINUTES = 10.0
ENVELOPE = StructType(
    [
        StructField("topic", StringType(), False),
        StructField("key", StringType(), False),
        StructField("at", TimestampType(), False),
    ]
)


def parsed(kafka_rows: DataFrame) -> DataFrame:
    value = F.col("value").cast("string")
    return kafka_rows.select(
        F.from_json(value, ENVELOPE).alias("event"),
        F.get_json_object(value, "$.payload").alias("payload"),
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        F.col("event.topic").alias("topic"),
        F.col("event.key").alias("key"),
        F.col("event.at").alias("event_at"),
        "payload",
        "partition",
        "offset",
        "kafka_timestamp",
    )


def bronze_rows(events: DataFrame) -> DataFrame:
    return events.withColumn("ingested_at", F.current_timestamp()).withColumn("date", F.to_date("event_at"))


def field(path: str) -> Column:
    return F.get_json_object("payload", f"$.{path}")


def since(moment: datetime) -> Column:
    return F.col("event_at") >= F.lit(moment)


def counted(condition: Column) -> Column:
    return F.sum(condition.cast("long"))


def search_pulse(recent: DataFrame, now: datetime) -> DataFrame:
    attempt = F.col("topic") == ATTEMPTS
    improved = (F.col("topic") == STEPS) & (field("kind") == IMPROVED)
    searched = recent.where(F.col("topic").isin(ATTEMPTS, STEPS, RECORDS))
    return searched.groupBy(F.col("key").alias("game")).agg(
        counted(attempt).alias("attempts_last_hour"),
        (counted(attempt & since(now - TEN_MINUTES)) / MINUTES).alias("attempts_per_minute"),
        counted(attempt & (field("outcome") == CROSSING)).alias("crossings_last_hour"),
        F.coalesce(F.sum(F.when(improved, field("saved").cast("long"))), F.lit(0)).alias("frames_saved_last_hour"),
        counted(improved).alias("improvements_last_hour"),
        counted(F.col("topic") == RECORDS).alias("records_last_hour"),
        F.max("event_at").alias("last_event_at"),
        F.lit(now).alias("updated_at"),
    )


def newest(events: DataFrame, keys: list[Column]) -> DataFrame:
    order = Window.partitionBy(*keys).orderBy(F.col("event_at").desc(), F.col("offset").desc())
    return events.withColumn("newest", F.row_number().over(order)).where("newest = 1").drop("newest")


def policy_pulse(recent: DataFrame, now: datetime) -> DataFrame:
    rounds = recent.where(F.col("topic") == POLICY_ROUNDS)
    totals = rounds.groupBy(F.col("key").alias("game")).agg(
        F.count("*").alias("rounds_last_hour"), F.avg(field("reward").cast("double")).alias("reward_mean_last_hour")
    )
    latest = newest(rounds, [F.col("key")]).select(
        F.col("key").alias("game"),
        field("run").alias("run"),
        field("round").cast("long").alias("round"),
        field("reward").cast("double").alias("reward"),
        field("loss").cast("double").alias("loss"),
        field("entropy").cast("double").alias("entropy"),
        field("exit_rate").cast("double").alias("exit_rate"),
        F.col("event_at").alias("last_event_at"),
    )
    return latest.join(totals, "game").withColumn("updated_at", F.lit(now))


def lab_status(recent: DataFrame, now: datetime) -> DataFrame:
    latest = newest(recent.where(F.col("topic") == LAB_EVENTS), [])
    return latest.select(
        F.lit(LAB_SLOT).alias("slot"),
        field("game").alias("game"),
        field("stamp").alias("stamp"),
        field("kind").alias("kind"),
        field("outcome").alias("outcome"),
        field("commits").cast("long").alias("commits"),
        F.col("event_at").alias("last_event_at"),
        F.lit(now).alias("updated_at"),
    )


def chat_pulse(recent: DataFrame, now: datetime) -> DataFrame:
    minutes = recent.where(F.col("topic") == CHAT_EVENTS)
    totals = minutes.groupBy(F.col("key").alias("channel")).agg(
        F.sum(field("messages").cast("long")).alias("messages_last_hour"),
        F.max(field("chatters").cast("long")).alias("most_chatters_in_a_minute"),
    )
    latest = newest(minutes, [F.col("key")]).select(
        F.col("key").alias("channel"),
        field("messages").cast("long").alias("messages_last_minute"),
        field("chatters").cast("long").alias("chatters_last_minute"),
        F.col("event_at").alias("last_event_at"),
    )
    return latest.join(totals, "channel").withColumn("updated_at", F.lit(now))
