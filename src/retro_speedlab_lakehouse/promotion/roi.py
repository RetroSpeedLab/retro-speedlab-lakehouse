from dataclasses import dataclass
from datetime import datetime
from math import sqrt

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from scipy import stats

from retro_speedlab_lakehouse.lake.catalog import EVENTS, PROMOTION_ROI
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.stream.rows import field, newest

DRAFTS = "promotion.drafts"
METRICS = "promotion.metrics"
USAGE = "promotion.usage"
PUBLISHED = "published"
CONFIDENCE = 0.975
CHANNELS = ArrayType(StringType())
PROMOTION = StructType(
    [
        StructField("channel", StringType(), False),
        StructField("variant", IntegerType(), False),
        StructField("drafts", LongType(), False),
        StructField("cost_usd", DoubleType(), False),
        StructField("views", LongType(), True),
        StructField("likes", LongType(), False),
        StructField("follows", LongType(), True),
        StructField("views_per_dollar", DoubleType(), True),
        StructField("views_lower", DoubleType(), True),
        StructField("views_upper", DoubleType(), True),
        StructField("likes_per_dollar", DoubleType(), True),
        StructField("likes_lower", DoubleType(), True),
        StructField("likes_upper", DoubleType(), True),
        StructField("computed_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class PerDollar:
    value: float | None
    lower: float | None
    upper: float | None


def draft_costs(events: DataFrame) -> DataFrame:
    usage = events.where(F.col("topic") == USAGE).select(
        field("event").alias("event"),
        F.from_json(field("channels"), CHANNELS).alias("channels"),
        field("cost_usd").cast("double").alias("cost_usd"),
    )
    shared = usage.where(F.size("channels") > 0).select(
        "event", F.explode("channels").alias("channel"), (F.col("cost_usd") / F.size("channels")).alias("cost_usd")
    )
    return shared.groupBy("event", "channel").agg(F.sum("cost_usd").alias("cost_usd"))


def published_drafts(events: DataFrame) -> DataFrame:
    drafts = newest(events.where(F.col("topic") == DRAFTS), ["key"])
    return drafts.where(field("status") == PUBLISHED).select(
        F.col("key").alias("draft"),
        field("event").alias("event"),
        field("channel").alias("channel"),
        field("variant").cast("int").alias("variant"),
    )


def latest_metrics(events: DataFrame) -> DataFrame:
    metrics = newest(events.where(F.col("topic") == METRICS), ["key"])
    return metrics.select(
        F.col("key").alias("draft"),
        field("views").cast("long").alias("views"),
        field("likes").cast("long").alias("likes"),
        field("subscribers_gained").cast("long").alias("follows"),
    )


def measured_variants(events: DataFrame) -> DataFrame:
    measured = (
        published_drafts(events).join(latest_metrics(events), "draft").join(draft_costs(events), ["event", "channel"])
    )
    return measured.groupBy("channel", "variant").agg(
        F.count("*").alias("drafts"),
        F.sum("cost_usd").alias("cost_usd"),
        F.sum("views").alias("views"),
        F.count("views").alias("viewed"),
        F.stddev("views").alias("views_sd"),
        F.sum("likes").alias("likes"),
        F.stddev("likes").alias("likes_sd"),
        F.sum("follows").alias("follows"),
    )


def per_dollar(total: int | None, count: int, spread: float | None, mean_cost: float) -> PerDollar:
    if total is None or count == 0 or mean_cost <= 0:
        return PerDollar(None, None, None)
    value = total / count / mean_cost
    if count < 2 or spread is None:
        return PerDollar(value, None, None)
    half = float(stats.t.ppf(CONFIDENCE, count - 1)) * spread / sqrt(count) / mean_cost
    return PerDollar(value, value - half, value + half)


def promotion_row(row: Row, now: datetime) -> tuple[object, ...]:
    mean_cost = row["cost_usd"] / row["drafts"]
    views = per_dollar(row["views"], row["viewed"], row["views_sd"], mean_cost)
    likes = per_dollar(row["likes"], row["drafts"], row["likes_sd"], mean_cost)
    return (
        row["channel"],
        row["variant"],
        row["drafts"],
        row["cost_usd"],
        row["views"],
        row["likes"],
        row["follows"],
        views.value,
        views.lower,
        views.upper,
        likes.value,
        likes.lower,
        likes.upper,
        now,
    )


def build_promotion_roi(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, EVENTS, lake):
        return 0
    rows = [promotion_row(row, now) for row in measured_variants(read(spark, EVENTS, lake)).collect()]
    replace(PROMOTION_ROI, lake, spark.createDataFrame(rows, PROMOTION), gate)
    return len(rows)
