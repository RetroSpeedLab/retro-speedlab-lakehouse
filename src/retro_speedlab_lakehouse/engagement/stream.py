from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.stream.search_rows import CHAT_EVENTS, field

STREAM_EXPOSURES = "stream.exposures"
TWITCH_MINUTES = "twitch.minutes"
STREAM_SCENES = "stream_scenes"
SECONDS_PER_MINUTE = 60.0
VARIANTS = "map<string,string>"


def exposure_rows(events: DataFrame) -> DataFrame:
    return (
        events.where(F.col("topic") == STREAM_EXPOSURES)
        .dropDuplicates(["event_id"])
        .select(
            field("surface").alias("surface"),
            field("item").alias("item"),
            field("game").alias("game"),
            field("arm").alias("arm"),
            F.to_timestamp(field("started_at")).alias("started_at"),
            F.to_timestamp(field("ended_at")).alias("ended_at"),
            F.from_json(F.get_json_object("payload", "$.variants"), VARIANTS).alias("variants"),
        )
        .where(F.col("ended_at") > F.col("started_at"))
    )


def seconds_between(start: Column, end: Column) -> Column:
    return end.cast("double") - start.cast("double")


def exposure_minutes(exposures: DataFrame) -> DataFrame:
    minutes = F.sequence(
        F.date_trunc("minute", "started_at"),
        F.date_trunc("minute", F.col("ended_at") - F.expr("INTERVAL 1 MICROSECOND")),
        F.expr("INTERVAL 1 MINUTE"),
    )
    spread = exposures.withColumn("minute", F.explode(minutes))
    overlap = seconds_between(
        F.greatest("started_at", "minute"), F.least("ended_at", F.col("minute") + F.expr("INTERVAL 1 MINUTE"))
    )
    return spread.withColumn("weight", overlap / SECONDS_PER_MINUTE)


def chat_minutes(search_events: DataFrame) -> DataFrame:
    return (
        search_events.where(F.col("topic") == CHAT_EVENTS)
        .select(F.to_timestamp(field("minute")).alias("minute"), field("messages").cast("double").alias("messages"))
        .groupBy("minute")
        .agg(F.sum("messages").alias("messages"))
    )


def twitch_minutes(search_events: DataFrame) -> DataFrame:
    return (
        search_events.where(F.col("topic") == TWITCH_MINUTES)
        .select(
            F.to_timestamp(field("minute")).alias("minute"),
            field("viewers").cast("double").alias("viewers"),
            field("followers").cast("double").alias("followers"),
        )
        .groupBy("minute")
        .agg(F.max("viewers").alias("viewers"), F.max("followers").alias("followers"))
    )


def exposure_outcomes(exposures: DataFrame, chat: DataFrame) -> DataFrame:
    heard = exposure_minutes(exposures).join(chat, "minute", "left")
    return heard.withColumn("messages", F.coalesce(F.col("messages"), F.lit(0.0)) * F.col("weight"))
