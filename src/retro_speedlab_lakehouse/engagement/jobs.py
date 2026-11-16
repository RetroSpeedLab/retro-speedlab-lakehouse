from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.engagement.analysis import Judged, judged_all
from retro_speedlab_lakehouse.engagement.catalog import (
    COMMUNITY_DAILY,
    CONTENT_RANKING,
    ENGAGEMENT_EVENTS,
    ENGAGEMENT_EXPERIMENTS,
    TWITCH_CHANNEL_OUTCOMES,
)
from retro_speedlab_lakehouse.engagement.channel import channel_outcomes
from retro_speedlab_lakehouse.engagement.discord import ACTIVITY, DISCORD, DiscordActivity
from retro_speedlab_lakehouse.engagement.events import engagement_events
from retro_speedlab_lakehouse.engagement.findings import holdout_findings, site_findings, stream_findings
from retro_speedlab_lakehouse.engagement.ranking import RANKING, ranking_rows, surface_totals
from retro_speedlab_lakehouse.engagement.stream import chat_minutes, exposure_rows, twitch_minutes
from retro_speedlab_lakehouse.engagement.umami import CELLS, UMAMI, UmamiCells
from retro_speedlab_lakehouse.lake.catalog import EVENTS, GAMES, SEARCH_EVENTS
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.bronze import ingest, latest

WWW = "www"
GAMES_SITE = "games"
RESULT = StructType(
    [
        StructField("experiment", StringType(), False),
        StructField("kind", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("primary", BooleanType(), False),
        StructField("control", StringType(), False),
        StructField("treatment", StringType(), False),
        StructField("control_n", LongType(), False),
        StructField("treatment_n", LongType(), False),
        StructField("enough", BooleanType(), False),
        StructField("control_estimate", DoubleType(), True),
        StructField("treatment_estimate", DoubleType(), True),
        StructField("effect", DoubleType(), True),
        StructField("ci_lower", DoubleType(), True),
        StructField("ci_upper", DoubleType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("p_value", DoubleType(), True),
        StructField("holm_p", DoubleType(), True),
        StructField("bh_p", DoubleType(), True),
        StructField("srm_p", DoubleType(), True),
        StructField("srm_ok", BooleanType(), False),
        StructField("verdict", StringType(), False),
        StructField("winner", StringType(), True),
        StructField("computed_at", TimestampType(), False),
    ]
)


def build_engagement(spark: SparkSession, settings: Settings, gate: Gate, now: datetime) -> int:
    engagement = settings.engagement
    websites = {WWW: settings.sources.umami.website, GAMES_SITE: settings.sources.umami.games_website}
    ingest(spark, settings.lake, gate, UmamiCells(settings.sources.umami, websites, engagement.backfill_hours))
    records = latest(spark, settings.lake, UMAMI, CELLS)
    searched = read(spark, SEARCH_EVENTS, settings.lake)
    chat = chat_minutes(searched)
    backend = read(spark, EVENTS, settings.lake)
    outcomes = channel_outcomes(exposure_rows(backend), twitch_minutes(searched), chat)
    replace(TWITCH_CHANNEL_OUTCOMES, settings.lake, outcomes, gate)
    channel = read(spark, TWITCH_CHANNEL_OUTCOMES, settings.lake)
    events = engagement_events(records, backend, chat, channel, now, engagement.window_days)
    replace(ENGAGEMENT_EVENTS, settings.lake, events, gate)
    catalog = [row["slug"] for row in read(spark, GAMES, settings.lake).select("slug").collect()]
    totals = surface_totals(read(spark, ENGAGEMENT_EVENTS, settings.lake), catalog, engagement)
    rows = ranking_rows(totals, engagement, now)
    replace(CONTENT_RANKING, settings.lake, spark.createDataFrame(rows, RANKING), gate)
    return len(rows)


def result_row(judged: Judged, now: datetime) -> tuple[object, ...]:
    finding, result = judged.finding, judged.finding.result
    arms = finding.arms
    head = (arms.experiment, arms.kind, finding.metric, finding.primary, arms.control, arms.treatment)
    sizes = (finding.control_n, finding.treatment_n, result is not None)
    tail = (judged.holm_p, judged.bh_p, finding.srm_p, finding.srm_ok, judged.verdict, judged.winner, now)
    if result is None:
        return (*head, *sizes, None, None, None, None, None, None, None, *tail)
    interval = result.confidence_interval
    estimates = (
        result.control_estimate,
        result.treatment_estimate,
        result.absolute_effect,
        interval.lower,
        interval.upper,
        interval.confidence_level,
        result.p_value,
    )
    return (*head, *sizes, *estimates, *tail)


def build_engagement_experiments(spark: SparkSession, settings: Settings, gate: Gate, now: datetime) -> int:
    engagement = settings.engagement
    since = now - timedelta(days=engagement.window_days)
    searched = read(spark, SEARCH_EVENTS, settings.lake).where(F.col("event_at") >= F.lit(since))
    exposures = exposure_rows(read(spark, EVENTS, settings.lake).where(F.col("event_at") >= F.lit(since)))
    records = latest(spark, settings.lake, UMAMI, CELLS).where(F.col("fetched_at") >= F.lit(since))
    findings = [
        *holdout_findings(read(spark, ENGAGEMENT_EVENTS, settings.lake), engagement),
        *site_findings(records, engagement),
        *stream_findings(exposures, twitch_minutes(searched), chat_minutes(searched), engagement),
    ]
    rows = [result_row(judged, now) for judged in judged_all(findings)]
    replace(ENGAGEMENT_EXPERIMENTS, settings.lake, spark.createDataFrame(rows, RESULT), gate)
    return len(rows)


def build_community(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    ingest(spark, settings.lake, gate, DiscordActivity(settings.sources.discord))
    days = latest(spark, settings.lake, DISCORD, ACTIVITY).select(
        F.to_date(F.get_json_object("body", "$.date")).alias("date"),
        F.get_json_object("body", "$.messages").cast("long").alias("discord_messages"),
        F.get_json_object("body", "$.joins").cast("long").alias("discord_joins"),
        F.col("fetched_at").alias("measured_at"),
    )
    replace(COMMUNITY_DAILY, settings.lake, days, gate)
    return days.count()
