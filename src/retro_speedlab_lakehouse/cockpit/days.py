from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.cockpit.hours import TWITCH_MINUTES, number, since, total, when_topic
from retro_speedlab_lakehouse.stream.search_rows import CHAT_EVENTS, IMPROVED, RECORDS, STEPS, field

WINDOWS = 8
DAY = timedelta(days=1)
DAY_SECONDS = 86400
STARTED = "started map<string, bigint>"


@dataclass(frozen=True, slots=True)
class Metric:
    name: str
    flow: bool
    area: str


RECORDS_METRIC = Metric("Records", True, "records")
FRAMES_SAVED = Metric("Frames saved", True, "search")
CHAT_MESSAGES = Metric("Chat messages", True, "audience")
PEAK_VIEWERS = Metric("Peak viewers", False, "audience")
FOLLOWERS = Metric("Twitch followers", False, "audience")
HIGHSCORES_METRIC = Metric("Highscores", True, "audience")
LAB_RUNS_METRIC = Metric("Lab runs", True, "lab")
HUMAN_PLAYS = Metric("Human game starts", False, "audience")
DISCORD_MEMBERS = Metric("Discord members", False, "audience")
SEARCH_METRICS = (RECORDS_METRIC, FRAMES_SAVED, CHAT_MESSAGES, PEAK_VIEWERS, FOLLOWERS)
SOURCE_METRICS = (HUMAN_PLAYS, DISCORD_MEMBERS)


def windowed(frame: DataFrame, column: str, now: datetime) -> DataFrame:
    elapsed = F.unix_timestamp(F.lit(now)) - F.unix_timestamp(column)
    recent = frame.where((F.col(column) > F.lit(now - WINDOWS * DAY)) & (F.col(column) <= F.lit(now)))
    return recent.withColumn("days_ago", F.floor(elapsed / DAY_SECONDS).cast("int"))


def search_days(events: DataFrame, now: datetime) -> DataFrame:
    improved = (F.col("topic") == STEPS) & (field("kind") == IMPROVED)
    totals = (
        windowed(since(events, now - WINDOWS * DAY), "event_at", now)
        .groupBy("days_ago")
        .agg(
            F.count(when_topic(RECORDS, F.lit(1))).cast("double").alias(RECORDS_METRIC.name),
            total(F.when(improved, number("saved"))).alias(FRAMES_SAVED.name),
            total(when_topic(CHAT_EVENTS, number("messages"))).alias(CHAT_MESSAGES.name),
            F.max(when_topic(TWITCH_MINUTES, number("viewers"))).alias(PEAK_VIEWERS.name),
            F.max(when_topic(TWITCH_MINUTES, number("followers"))).alias(FOLLOWERS.name),
        )
    )
    names = tuple(metric.name for metric in SEARCH_METRICS)
    return totals.unpivot("days_ago", names, "metric", "value")


def counted_days(rows: DataFrame, column: str, metric: Metric, now: datetime) -> DataFrame:
    counted = windowed(rows, column, now).groupBy("days_ago").agg(F.count("*").cast("double").alias("value"))
    return counted.withColumn("metric", F.lit(metric.name)).select("days_ago", "metric", "value")


def snapshot_days(records: DataFrame, value: Column, metric: Metric, now: datetime) -> DataFrame:
    order = Window.partitionBy("days_ago", "record_key").orderBy(F.col("fetched_at").desc())
    newest = windowed(records, "fetched_at", now).withColumn("newest", F.row_number().over(order))
    summed = newest.where("newest = 1").groupBy("days_ago").agg(F.sum(value).cast("double").alias("value"))
    return summed.withColumn("metric", F.lit(metric.name)).select("days_ago", "metric", "value")


def started_total() -> Column:
    started = F.map_values(F.from_json("body", STARTED)["started"])
    return F.aggregate(started, F.lit(0).cast("long"), lambda counted, count: counted + count)


def source_days(sources: DataFrame, now: datetime) -> DataFrame:
    plays = sources.where(
        (F.col("source") == "umami") & (F.col("stream") == "plays") & F.col("record_key").endswith("/last_day")
    )
    members = sources.where(
        (F.col("source") == "discord") & (F.col("stream") == "numbers") & (F.col("record_key") == "members")
    )
    member_count = F.get_json_object("body", "$.value").cast("double")
    played = snapshot_days(plays, started_total(), HUMAN_PLAYS, now)
    return played.unionByName(snapshot_days(members, member_count, DISCORD_MEMBERS, now))


def dense(spark: SparkSession, measured: list[DataFrame], metrics: list[Metric], now: datetime) -> DataFrame:
    catalog = spark.createDataFrame([(metric.name, metric.flow) for metric in metrics], "metric string, flow boolean")
    windows = spark.range(WINDOWS).select(F.col("id").cast("int").alias("days_ago"))
    values = reduce(DataFrame.unionByName, measured)
    filled = windows.crossJoin(catalog).join(values, ["days_ago", "metric"], "left")
    return filled.select(
        F.date_sub(F.lit(now.date()), F.col("days_ago")).alias("day"),
        "days_ago",
        "metric",
        F.when(F.col("flow"), F.coalesce("value", F.lit(0.0))).otherwise(F.col("value")).alias("value"),
        F.lit(now).alias("computed_at"),
    )
