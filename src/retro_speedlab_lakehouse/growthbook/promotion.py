from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.growthbook.units import CONVERTED, EVEN, Experiment, Family, measured

APPROVED = "approved"
INTERACTIONS = "interactions_24h"
METRICS = (APPROVED, INTERACTIONS)
VARIANTS = ("0", "1")


def promotion_experiment(channel: str) -> Experiment:
    hypothesis = f"The maintainer approves {channel} drafts of variant 1 more often than of variant 0"
    return Experiment(
        f"promotion_variants_{channel}", f"Promotion: {channel} variants", hypothesis, VARIANTS, EVEN, METRICS
    )


def promotion_variants(decisions: DataFrame) -> Family:
    drafts = decisions.select(
        F.concat(F.lit("promotion_variants_"), "channel").alias("experiment"),
        F.col("variant").cast("string").alias("variation"),
        F.concat_ws(":", F.lit("promotion"), "draft").alias("unit"),
        F.col("decided_at").alias("assigned_at"),
        F.when(F.col("approved"), F.lit(CONVERTED)).alias(APPROVED),
        F.col("engagement_24h").alias(INTERACTIONS),
    ).where(F.col("variation").isin(*VARIANTS))
    channels = sorted(row["channel"] for row in decisions.select("channel").distinct().collect())
    return Family([promotion_experiment(channel) for channel in channels], measured(drafts, METRICS))
