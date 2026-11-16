from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.engagement.analysis import AB, HOLDOUT, Arms, Finding, mean_finding, rate_finding
from retro_speedlab_lakehouse.engagement.events import umami_cells
from retro_speedlab_lakehouse.engagement.stream import STREAM_SCENES, exposure_minutes
from retro_speedlab_lakehouse.settings import EngagementSettings

BETA = "beta"
ADAPTIVE = "adaptive"
VARIANT_VIEW = "variant-view"
VARIANT_GOAL = "variant-goal"
EVEN = 0.5
MINUTES_PER_HOUR = 60.0
STREAM_METRICS = ("viewers", "chat_per_minute", "follows_per_hour")


def holdout_findings(events: DataFrame, settings: EngagementSettings) -> list[Finding]:
    compared = events.where(F.col("arm").isin(HOLDOUT, ADAPTIVE))
    totals = {
        (row["surface"], row["arm"]): (round(row["outcomes"]), round(row["exposures"]))
        for row in compared.groupBy("surface", "arm")
        .agg(F.sum("outcomes").alias("outcomes"), F.sum("exposures").alias("exposures"))
        .collect()
    }
    hourly = (
        compared.groupBy("surface", "arm", "hour").agg((F.sum("outcomes") / F.sum("exposures")).alias("rate")).collect()
    )
    findings: list[Finding] = []
    for surface, configured in settings.surfaces.items():
        if not configured.holdout:
            continue
        arms = Arms(f"holdout_{surface}", HOLDOUT, HOLDOUT, ADAPTIVE, settings.holdout_share)
        if configured.family == BETA:
            held = totals[(surface, HOLDOUT)] if (surface, HOLDOUT) in totals else (0, 0)
            learned = totals[(surface, ADAPTIVE)] if (surface, ADAPTIVE) in totals else (0, 0)
            findings.append(rate_finding(arms, "click_rate", True, held, learned, settings.min_exposures))
            continue
        rates = {
            arm: [row["rate"] for row in hourly if row["surface"] == surface and row["arm"] == arm]
            for arm in (HOLDOUT, ADAPTIVE)
        }
        findings.append(
            mean_finding(arms, "chat_per_minute", True, rates[HOLDOUT], rates[ADAPTIVE], settings.min_hours)
        )
    return findings


def site_findings(records: DataFrame, settings: EngagementSettings) -> list[Finding]:
    cells = umami_cells(records).where(F.col("event").isin(VARIANT_VIEW, VARIANT_GOAL))
    counted = {
        (row["event"], row["cell"]): row["total"]
        for row in cells.groupBy("event", "cell").agg(F.sum("total").alias("total")).collect()
    }

    def count(event: str, cell: str) -> int:
        return int(counted[(event, cell)]) if (event, cell) in counted else 0

    findings: list[Finding] = []
    for name, experiment in settings.experiments.items():
        if experiment.scope != "visit":
            continue
        arms = Arms(name, AB, *experiment.variants, EVEN)
        for index, goal in enumerate(experiment.goals):
            sides = [
                (count(VARIANT_GOAL, f"{name}|{variant}|{goal}"), count(VARIANT_VIEW, f"{name}|{variant}"))
                for variant in experiment.variants
            ]
            control, treatment = [(hits, max(views, hits)) for hits, views in sides]
            findings.append(rate_finding(arms, f"{goal}_rate", index == 0, control, treatment, settings.min_exposures))
    return findings


def stream_blocks(minutes: DataFrame, experiment: str, block_minutes: int) -> DataFrame:
    block = F.floor(F.unix_timestamp("minute") / (block_minutes * 60))
    chosen = minutes.where(F.col("variants")[experiment].isNotNull())
    return chosen.groupBy(F.col("variants")[experiment].alias("variant"), block.alias("block")).agg(
        F.avg("viewers").alias("viewers"),
        (F.sum(F.coalesce("messages", F.lit(0.0)) * F.col("weight")) / F.sum("weight")).alias("chat_per_minute"),
        ((F.max_by("followers", "minute") - F.min_by("followers", "minute")) * MINUTES_PER_HOUR / block_minutes).alias(
            "follows_per_hour"
        ),
    )


def stream_findings(
    exposures: DataFrame, twitch: DataFrame, chat: DataFrame, settings: EngagementSettings
) -> list[Finding]:
    scenes = exposure_minutes(exposures.where(F.col("surface") == STREAM_SCENES))
    minutes = scenes.join(twitch, "minute", "left").join(chat, "minute", "left").cache()
    findings: list[Finding] = []
    for name, experiment in settings.experiments.items():
        if experiment.scope != "time":
            continue
        arms = Arms(name, AB, *experiment.variants, EVEN)
        blocks = stream_blocks(minutes, name, settings.block_minutes).collect()
        for metric in STREAM_METRICS:
            control, treatment = (
                [float(row[metric]) for row in blocks if row["variant"] == variant and row[metric] is not None]
                for variant in experiment.variants
            )
            primary = metric == STREAM_METRICS[0]
            findings.append(mean_finding(arms, metric, primary, control, treatment, settings.min_blocks))
    minutes.unpersist()
    return findings
