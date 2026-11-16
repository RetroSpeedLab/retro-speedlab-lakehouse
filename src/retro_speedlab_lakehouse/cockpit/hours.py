from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.stream.search_rows import (
    ATTEMPTS,
    CHAT_EVENTS,
    IMPROVED,
    POLICY_ROUNDS,
    RECORDS,
    STEPS,
    field,
)

TWITCH_MINUTES = "twitch.minutes"
HOURS = timedelta(days=7)


def since(events: DataFrame, start: datetime) -> DataFrame:
    return events.where((F.col("date") >= F.lit(start.date())) & (F.col("event_at") >= F.lit(start)))


def number(path: str) -> Column:
    return field(path).cast("double")


def when_topic(topic: str, value: Column) -> Column:
    return F.when(F.col("topic") == topic, value)


def total(value: Column) -> Column:
    return F.coalesce(F.sum(value), F.lit(0)).cast("double")


def search_hours(events: DataFrame, now: datetime) -> DataFrame:
    searched = since(events, now - HOURS).where(F.col("topic").isin(ATTEMPTS, STEPS, RECORDS, POLICY_ROUNDS))
    improved = (F.col("topic") == STEPS) & (field("kind") == IMPROVED)
    return searched.groupBy(F.date_trunc("HOUR", "event_at").alias("hour"), F.col("key").alias("game")).agg(
        F.count(when_topic(ATTEMPTS, F.lit(1))).alias("attempts"),
        F.count(F.when(improved, 1)).alias("improvements"),
        total(F.when(improved, number("saved"))).alias("frames_saved"),
        F.count(when_topic(RECORDS, F.lit(1))).alias("records"),
        F.count(when_topic(POLICY_ROUNDS, F.lit(1))).alias("ppo_rounds"),
        total(when_topic(POLICY_ROUNDS, number("reward"))).alias("ppo_reward_sum"),
        total(when_topic(POLICY_ROUNDS, number("loss"))).alias("ppo_loss_sum"),
        F.lit(now).alias("computed_at"),
    )


def audience_hours(events: DataFrame, now: datetime) -> DataFrame:
    watched = since(events, now - HOURS).where(F.col("topic").isin(TWITCH_MINUTES, CHAT_EVENTS))
    live = (F.col("topic") == TWITCH_MINUTES) & (field("live") == "true")
    return watched.groupBy(F.date_trunc("HOUR", "event_at").alias("hour")).agg(
        F.max(F.when(live, number("viewers"))).alias("viewers_peak"),
        F.avg(F.when(live, number("viewers"))).alias("viewers_mean"),
        F.count(F.when(live, 1)).alias("live_minutes"),
        F.max(when_topic(TWITCH_MINUTES, number("followers"))).alias("followers"),
        total(when_topic(CHAT_EVENTS, number("messages"))).alias("chat_messages"),
        F.max(when_topic(CHAT_EVENTS, number("chatters"))).alias("chatters_peak"),
        F.lit(now).alias("computed_at"),
    )
