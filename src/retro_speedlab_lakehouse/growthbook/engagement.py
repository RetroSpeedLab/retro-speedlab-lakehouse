from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.engagement.analysis import HOLDOUT
from retro_speedlab_lakehouse.engagement.events import umami_cells
from retro_speedlab_lakehouse.engagement.findings import (
    ADAPTIVE,
    BETA,
    STREAM_METRICS,
    VARIANT_GOAL,
    VARIANT_VIEW,
    stream_blocks,
)
from retro_speedlab_lakehouse.growthbook.units import EVEN, Experiment, Family, counted, measured
from retro_speedlab_lakehouse.settings import EngagementSettings

CLICK = "click"
CHAT_PER_MINUTE = "chat_per_minute"
SECONDS_PER_MINUTE = 60
CELL = ["experiment", "variation", "hour"]


def site_tests(records: DataFrame, settings: EngagementSettings) -> Family:
    chosen = {name: test for name, test in settings.experiments.items() if test.scope == "visit"}
    cells = umami_cells(records).where(F.col("event").isin(VARIANT_VIEW, VARIANT_GOAL))
    named = cells.select(
        "event",
        "hour",
        "total",
        F.col("parts")[0].alias("experiment"),
        F.col("parts")[1].alias("variation"),
        F.when(F.size("parts") == 3, F.col("parts")[2]).alias("metric"),
    ).where(F.col("experiment").isin(*chosen))
    views = named.where(F.col("event") == VARIANT_VIEW).groupBy(*CELL).agg(F.sum("total").alias("views"))
    goals = named.where(F.col("event") == VARIANT_GOAL).groupBy(*CELL, "metric").agg(F.sum("total").alias("hits"))
    both = views.join(goals, CELL, "full")
    most = F.max(F.coalesce("hits", F.lit(0))).over(Window.partitionBy(*CELL))
    units = counted(both.withColumn("units", F.greatest(F.coalesce("views", F.lit(0)), most)))
    experiments = [
        Experiment(
            name,
            f"Site: {name}",
            f"{test.variants[1]} gets more {', '.join(test.goals)} per view than {test.variants[0]}",
            test.variants,
            EVEN,
            test.goals,
        )
        for name, test in chosen.items()
    ]
    arms = [f"{name}|{variant}" for name, test in chosen.items() for variant in test.variants]
    return Family(experiments, units.where(F.concat_ws("|", "experiment", "variation").isin(*arms)))


def holdout_experiment(surface: str, metric: str, settings: EngagementSettings) -> Experiment:
    share = settings.holdout_share
    hypothesis = f"The learned order of {surface} gets more {metric} than a random order"
    return Experiment(
        f"holdout_{surface}", f"Holdout: {surface}", hypothesis, (HOLDOUT, ADAPTIVE), (share, 1 - share), (metric,)
    )


def holdouts(events: DataFrame, settings: EngagementSettings) -> Family:
    held = {surface: chosen for surface, chosen in settings.surfaces.items() if chosen.holdout}
    beta = [surface for surface, chosen in held.items() if chosen.family == BETA]
    gamma = [surface for surface in held if surface not in beta]
    hourly = (
        events.where(F.col("arm").isin(HOLDOUT, ADAPTIVE) & F.col("surface").isin(*held))
        .groupBy("surface", "arm", "hour")
        .agg(F.sum("exposures").alias("exposures"), F.sum("outcomes").alias("outcomes"))
        .select(F.concat(F.lit("holdout_"), "surface").alias("experiment"), F.col("arm").alias("variation"), "*")
    )
    views = hourly.where(F.col("surface").isin(*beta)).select(
        "experiment",
        "variation",
        "hour",
        F.round("outcomes").alias("hits"),
        F.greatest(F.round("exposures"), F.round("outcomes")).alias("units"),
        F.lit(CLICK).alias("metric"),
    )
    hours = hourly.where(F.col("surface").isin(*gamma)).select(
        "experiment",
        "variation",
        F.concat_ws(":", "experiment", "variation", F.date_format("hour", "yyyyMMddHH")).alias("unit"),
        F.col("hour").alias("assigned_at"),
        (F.col("outcomes") / F.col("exposures")).alias(CHAT_PER_MINUTE),
    )
    experiments = [holdout_experiment(surface, CLICK, settings) for surface in beta] + [
        holdout_experiment(surface, CHAT_PER_MINUTE, settings) for surface in gamma
    ]
    return Family(experiments, counted(views).unionByName(measured(hours, (CHAT_PER_MINUTE,))))


def stream_tests(minutes: DataFrame, settings: EngagementSettings) -> Family:
    chosen = {name: test for name, test in settings.experiments.items() if test.scope == "time"}
    seconds = settings.block_minutes * SECONDS_PER_MINUTE
    blocks = [
        stream_blocks(minutes, name, settings.block_minutes)
        .where(F.col("variant").isin(*test.variants) & F.col("viewers").isNotNull())
        .select(
            F.lit(name).alias("experiment"),
            F.col("variant").alias("variation"),
            F.concat_ws(":", F.lit(name), "variant", "block").alias("unit"),
            F.timestamp_seconds(F.col("block") * seconds).alias("assigned_at"),
            *STREAM_METRICS,
        )
        for name, test in chosen.items()
    ]
    experiments = [
        Experiment(
            name,
            f"Stream: {name}",
            f"{test.variants[1]} keeps more viewers than {test.variants[0]}",
            test.variants,
            EVEN,
            STREAM_METRICS,
        )
        for name, test in chosen.items()
    ]
    return Family(experiments, measured(reduce(DataFrame.unionByName, blocks), STREAM_METRICS))
