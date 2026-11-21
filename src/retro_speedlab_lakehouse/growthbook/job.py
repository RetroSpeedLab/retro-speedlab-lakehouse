from datetime import UTC, datetime, timedelta
from functools import reduce

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.engagement.catalog import ENGAGEMENT_EVENTS
from retro_speedlab_lakehouse.engagement.findings import stream_minutes
from retro_speedlab_lakehouse.engagement.stream import chat_minutes, exposure_rows, twitch_minutes
from retro_speedlab_lakehouse.engagement.umami import CELLS, UMAMI
from retro_speedlab_lakehouse.growthbook.api import sync_experiments
from retro_speedlab_lakehouse.growthbook.engagement import holdouts, site_tests, stream_tests
from retro_speedlab_lakehouse.growthbook.postgres import Event, Unit, publish
from retro_speedlab_lakehouse.growthbook.promotion import promotion_variants
from retro_speedlab_lakehouse.growthbook.search import search_bandit
from retro_speedlab_lakehouse.growthbook.units import Family
from retro_speedlab_lakehouse.lake.catalog import EVENTS, PROMOTION_DECISIONS, SEARCH_EVENTS
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.promotion.preferences import engaged
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.bronze import latest
from retro_speedlab_lakehouse.sources.catalog import BLUESKY_POSTS


def families(spark: SparkSession, settings: Settings, now: datetime) -> list[Family]:
    engagement, lake = settings.engagement, settings.lake
    since = F.lit(now - timedelta(days=engagement.window_days))
    searched = read(spark, SEARCH_EVENTS, lake)
    recent = searched.where(F.col("event_at") >= since)
    exposures = exposure_rows(read(spark, EVENTS, lake).where(F.col("event_at") >= since))
    minutes = stream_minutes(exposures, twitch_minutes(recent), chat_minutes(recent))
    decisions = engaged(read(spark, PROMOTION_DECISIONS, lake), read(spark, BLUESKY_POSTS, lake))
    return [
        site_tests(latest(spark, lake, UMAMI, CELLS).where(F.col("fetched_at") >= since), engagement),
        holdouts(read(spark, ENGAGEMENT_EVENTS, lake), engagement),
        stream_tests(minutes, engagement),
        search_bandit(searched, now),
        promotion_variants(decisions),
    ]


def moment(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, UTC)


def first_units(units: list[Unit]) -> dict[str, datetime]:
    starts: dict[str, datetime] = {}
    for _, experiment, _, assigned_at in units:
        starts[experiment] = min(starts[experiment], assigned_at) if experiment in starts else assigned_at
    return starts


def export_growthbook(spark: SparkSession, settings: Settings, gate: Gate, now: datetime) -> int:
    found = families(spark, settings, now)
    rows = reduce(DataFrame.unionByName, [family.units for family in found]).select(
        "experiment", "variation", "unit", F.col("assigned_at").cast("double").alias("assigned_at"), "metric", "value"
    )
    collected = rows.collect()
    units: list[Unit] = sorted(
        {(row["unit"], row["experiment"], row["variation"], moment(row["assigned_at"])) for row in collected}
    )
    events: list[Event] = [
        (row["unit"], row["metric"], row["value"], moment(row["assigned_at"]))
        for row in collected
        if row["value"] is not None
    ]
    publish(settings.growthbook.database, units, events)
    starts = first_units(units)
    experiments = [experiment for family in found for experiment in family.experiments]
    synced = sync_experiments(settings.growthbook, experiments, starts)
    logger.info(f"GrowthBook: {len(units)} units and {len(events)} metric events in {synced} experiments")
    return len(units) + len(events)
