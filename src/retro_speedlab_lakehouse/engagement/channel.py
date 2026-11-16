from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.engagement.stream import exposure_minutes, seconds_between

TWITCH_CHANNEL = "twitch_channel"
SECONDS_PER_HOUR = 3600.0
MINUTES_PER_HOUR = 60.0
EXPOSURE = ("item", "arm", "game", "started_at", "ended_at")
OUTCOMES = (
    *EXPOSURE,
    "hours",
    "average_viewers",
    "peak_viewers",
    "viewer_hours",
    "new_followers",
    "followers_per_hour",
    "chat_messages",
    "chat_per_viewer_hour",
)


def channel_outcomes(exposures: DataFrame, twitch: DataFrame, chat: DataFrame) -> DataFrame:
    shown = exposures.where(F.col("surface") == TWITCH_CHANNEL).dropDuplicates(["item", "started_at"])
    minutes = exposure_minutes(shown).join(twitch, "minute", "left").join(chat, "minute", "left")
    measured = F.when(F.col("viewers").isNotNull(), F.col("minute"))
    totals = minutes.groupBy(*EXPOSURE).agg(
        F.sum(F.col("viewers") * F.col("weight")).alias("viewer_minutes"),
        F.sum(F.when(F.col("viewers").isNotNull(), F.col("weight"))).alias("measured_minutes"),
        F.max("viewers").alias("peak_viewers"),
        (F.max_by("followers", measured) - F.min_by("followers", measured)).alias("new_followers"),
        F.sum(F.coalesce(F.col("messages"), F.lit(0.0)) * F.col("weight")).alias("chat_messages"),
    )
    hours = seconds_between(F.col("started_at"), F.col("ended_at")) / SECONDS_PER_HOUR
    viewer_hours = F.col("viewer_minutes") / MINUTES_PER_HOUR
    return (
        totals.where(F.col("measured_minutes").isNotNull())
        .withColumns(
            {
                "hours": hours,
                "average_viewers": F.col("viewer_minutes") / F.col("measured_minutes"),
                "viewer_hours": viewer_hours,
                "followers_per_hour": F.col("new_followers") / hours,
                "chat_per_viewer_hour": F.when(viewer_hours > 0, F.col("chat_messages") / viewer_hours),
            }
        )
        .select(*OUTCOMES)
    )
