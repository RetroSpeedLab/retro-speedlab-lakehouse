from datetime import datetime, timedelta
from itertools import groupby

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.bandit.posterior import Allocation, Effort, allocations
from retro_speedlab_lakehouse.lake.catalog import SEARCH_ALLOCATION, SEARCH_EVENTS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import BanditSettings, LakeSettings
from retro_speedlab_lakehouse.stream.search_rows import ATTEMPTS, IMPROVED, STEPS, field

ALLOCATION = StructType(
    [
        StructField("game", StringType(), False),
        StructField("role", StringType(), False),
        StructField("share", DoubleType(), False),
        StructField("best_probability", DoubleType(), False),
        StructField("saved", LongType(), False),
        StructField("improvements", LongType(), False),
        StructField("worker_seconds", DoubleType(), False),
        StructField("rate_mean", DoubleType(), False),
        StructField("rate_lower", DoubleType(), False),
        StructField("rate_upper", DoubleType(), False),
        StructField("days", LongType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


def efforts(events: DataFrame, since: datetime) -> DataFrame:
    topic = F.col("topic")
    improved = (topic == STEPS) & (field("kind") == IMPROVED)
    attributed = (
        events.where((F.col("date") >= F.lit(since.date())) & (F.col("event_at") >= F.lit(since)))
        .where(topic.isin(ATTEMPTS, STEPS) & field("role").isNotNull())
        .select(
            F.col("key").alias("game"),
            field("role").alias("role"),
            F.when(topic == ATTEMPTS, field("worker_seconds").cast("double")).alias("worker_seconds"),
            F.when(improved, field("saved").cast("long")).alias("saved"),
            improved.alias("improved"),
        )
    )
    totals = attributed.groupBy("game", "role").agg(
        F.coalesce(F.sum("worker_seconds"), F.lit(0.0)).alias("worker_seconds"),
        F.coalesce(F.sum("saved"), F.lit(0)).alias("saved"),
        F.sum(F.col("improved").cast("long")).alias("improvements"),
    )
    return totals.where(F.col("worker_seconds") > 0)


def game_allocations(rows: list[tuple[str, Effort]], bandit: BanditSettings) -> list[tuple[str, Allocation]]:
    rng = np.random.default_rng()
    ordered = sorted(rows, key=lambda row: (row[0], row[1].role))
    return [
        (game, allocation)
        for game, kinds in groupby(ordered, key=lambda row: row[0])
        for allocation in allocations([effort for _, effort in kinds], bandit.min_share, bandit.draws, rng)
    ]


def allocation_row(game: str, allocation: Allocation, days: int, now: datetime) -> tuple[object, ...]:
    effort = allocation.effort
    return (
        game,
        effort.role,
        allocation.share,
        allocation.best,
        effort.saved,
        effort.improvements,
        effort.worker_seconds,
        allocation.rate_mean,
        allocation.rate_lower,
        allocation.rate_upper,
        days,
        now,
    )


def build_allocation(spark: SparkSession, lake: LakeSettings, bandit: BanditSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SEARCH_EVENTS, lake):
        return 0
    totals = efforts(read(spark, SEARCH_EVENTS, lake), now - timedelta(days=bandit.days)).collect()
    found = [
        (row["game"], Effort(row["role"], row["saved"], row["improvements"], row["worker_seconds"])) for row in totals
    ]
    rows = [allocation_row(game, allocation, bandit.days, now) for game, allocation in game_allocations(found, bandit)]
    replace(SEARCH_ALLOCATION, lake, spark.createDataFrame(rows, ALLOCATION), gate)
    return len(rows)
