from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

from retro_speedlab_lakehouse.engagement.channel import TWITCH_CHANNEL
from retro_speedlab_lakehouse.engagement.stream import STREAM_SCENES, exposure_outcomes, exposure_rows
from retro_speedlab_lakehouse.stream.rows import field, newest

ARMS = ("fixed", "adaptive", "holdout")
RANK_VIEW = "rank-view"
RANK_CLICK = "rank-click"
STREAM_GAMES = "stream_games"
PROMOTION = "promotion"
PROMOTION_DRAFTS = "promotion.drafts"
PROMOTION_METRICS = "promotion.metrics"
FIXED = "fixed"
SEPARATOR = r"\|"
CELLS = StructType(
    [
        StructField("site", StringType(), False),
        StructField("event", StringType(), False),
        StructField("hour", StringType(), False),
        StructField("cells", MapType(StringType(), LongType()), False),
    ]
)
COLUMNS = ("surface", "item", "arm", "hour", "exposures", "outcomes")


def umami_cells(records: DataFrame) -> DataFrame:
    parsed = records.select(F.from_json("body", CELLS).alias("body"))
    return parsed.select(
        "body.event",
        F.to_timestamp("body.hour").alias("hour"),
        F.explode("body.cells").alias("cell", "total"),
    ).withColumn("parts", F.split("cell", SEPARATOR))


def umami_events(records: DataFrame) -> DataFrame:
    cells = umami_cells(records).where(F.col("event").isin(RANK_VIEW, RANK_CLICK) & (F.size("parts") == 3))
    tallied = cells.select(
        F.col("parts")[0].alias("surface"),
        F.col("parts")[1].alias("arm"),
        F.col("parts")[2].alias("item"),
        "hour",
        F.when(F.col("event") == RANK_VIEW, F.col("total")).otherwise(0).alias("views"),
        F.when(F.col("event") == RANK_CLICK, F.col("total")).otherwise(0).alias("clicks"),
    ).where(F.col("arm").isin(*ARMS))
    totals = tallied.groupBy("surface", "item", "arm", "hour").agg(
        F.sum("views").cast("double").alias("views"), F.sum("clicks").cast("double").alias("outcomes")
    )
    return totals.withColumn("exposures", F.greatest("views", "outcomes")).select(*COLUMNS)


def stream_events(exposures: DataFrame) -> DataFrame:
    games = exposures.where((F.col("surface") == STREAM_SCENES) & F.col("game").isNotNull()).withColumns(
        {"surface": F.lit(STREAM_GAMES), "item": F.col("game")}
    )
    return (
        exposures.unionByName(games)
        .withColumn("hour", F.date_trunc("hour", "minute"))
        .groupBy("surface", "item", "arm", "hour")
        .agg(F.sum("weight").alias("exposures"), F.sum("messages").alias("outcomes"))
        .select(*COLUMNS)
    )


def counted(name: str) -> Column:
    return F.coalesce(field(name).cast("double"), F.lit(0.0))


def promotion_events(events: DataFrame) -> DataFrame:
    keyed = events.withColumn("draft", F.col("key"))
    drafts = newest(keyed.where(F.col("topic") == PROMOTION_DRAFTS), ["draft"]).select(
        "draft", F.to_timestamp(field("published")).alias("published")
    )
    metrics = newest(keyed.where(F.col("topic") == PROMOTION_METRICS), ["draft"]).select(
        "draft",
        field("kind").alias("item"),
        (counted("likes") + counted("reposts") + counted("replies")).alias("outcomes"),
    )
    posts = metrics.join(drafts.where(F.col("published").isNotNull()), "draft")
    return (
        posts.withColumns({"surface": F.lit(PROMOTION), "arm": F.lit(FIXED), "hour": F.date_trunc("hour", "published")})
        .groupBy("surface", "item", "arm", "hour")
        .agg(F.count("*").cast("double").alias("exposures"), F.sum("outcomes").alias("outcomes"))
        .select(*COLUMNS)
    )


def channel_events(outcomes: DataFrame) -> DataFrame:
    return (
        outcomes.withColumns({"surface": F.lit(TWITCH_CHANNEL), "hour": F.date_trunc("hour", "started_at")})
        .groupBy("surface", "item", "arm", "hour")
        .agg(F.sum("hours").alias("exposures"), F.sum(F.greatest("new_followers", F.lit(0.0))).alias("outcomes"))
        .select(*COLUMNS)
    )


def engagement_events(
    records: DataFrame, events: DataFrame, chat: DataFrame, channel: DataFrame, now: datetime, days: int
) -> DataFrame:
    since = now - timedelta(days=days)
    recent = events.where(F.col("event_at") >= F.lit(since))
    watched = exposure_outcomes(exposure_rows(recent).where(F.col("surface") != TWITCH_CHANNEL), chat)
    found = (
        umami_events(records)
        .unionByName(stream_events(watched))
        .unionByName(promotion_events(recent))
        .unionByName(channel_events(channel))
    )
    return found.where((F.col("hour") >= F.lit(since)) & (F.col("exposures") > 0))
