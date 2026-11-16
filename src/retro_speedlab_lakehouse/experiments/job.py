from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retro_speedlab_lakehouse.experiments.analysis import Outcome, analyzed, primary
from retro_speedlab_lakehouse.experiments.arms import Change, SearchHour, experiments
from retro_speedlab_lakehouse.experiments.roi import DEPLOYED, ROI, Effect, LabRun, Spend, effect_of, lab_roi_rows
from retro_speedlab_lakehouse.experiments.sequential import harm
from retro_speedlab_lakehouse.lake.catalog import LAB_EXPERIMENTS, LAB_ROI, LAB_RUNS, SEARCH_EVENTS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import SILVER_LAB_COSTS
from retro_speedlab_lakehouse.stream.search_rows import ATTEMPTS, CROSSING, RECORDS, STEPS, field

IMPROVED = "improved"
UNVERIFIED = "unverified"
RESULT = StructType(
    [
        StructField("game", StringType(), False),
        StructField("stamp", StringType(), False),
        StructField("changed_at", TimestampType(), False),
        StructField("metric", StringType(), False),
        StructField("better", StringType(), False),
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
        StructField("verdict", StringType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


def search_hours(events: DataFrame) -> list[SearchHour]:
    topic = F.col("topic")
    hourly = (
        events.where(topic.isin(ATTEMPTS, STEPS, RECORDS))
        .groupBy(F.col("key").alias("game"), F.date_trunc("hour", "event_at").alias("hour"))
        .agg(
            F.sum((topic == ATTEMPTS).cast("long")).alias("attempts"),
            F.sum(((topic == ATTEMPTS) & (field("outcome") == CROSSING)).cast("long")).alias("crossings"),
            F.coalesce(
                F.sum(F.when((topic == STEPS) & (field("kind") == IMPROVED), field("saved").cast("long"))), F.lit(0)
            ).alias("saved"),
            F.sum((topic == RECORDS).cast("long")).alias("records"),
            F.sum(((topic == STEPS) & (field("kind") == UNVERIFIED)).cast("long")).alias("unverified"),
        )
        .where("attempts > 0")
    )
    return [
        SearchHour(
            row["game"],
            row["hour"].replace(tzinfo=UTC),
            row["attempts"],
            row["crossings"],
            row["saved"],
            row["records"],
            row["unverified"],
        )
        for row in hourly.collect()
    ]


def finished_runs(lab_runs: DataFrame) -> list[LabRun]:
    found = lab_runs.select("game", "stamp", "outcome", "started_at", "minutes", "cost_usd").collect()
    return [
        LabRun(
            row["game"],
            row["stamp"],
            row["outcome"],
            row["started_at"].replace(tzinfo=UTC) + timedelta(minutes=row["minutes"]),
            row["cost_usd"],
        )
        for row in found
    ]


def deployed_changes(runs: list[LabRun]) -> list[Change]:
    return [Change(run.game, run.stamp, run.finished_at) for run in runs if run.outcome == DEPLOYED]


def spends(spark: SparkSession, lake: LakeSettings) -> dict[str, Spend]:
    if not exists(spark, SILVER_LAB_COSTS, lake):
        return {}
    found = read(spark, SILVER_LAB_COSTS, lake).collect()
    return {row["stamp"]: Spend(row["observations"], row["tokens"], row["cost_usd"]) for row in found}


def result_row(change: Change, outcome: Outcome, now: datetime) -> tuple[object, ...]:
    result = outcome.result
    sizes = (
        change.game,
        change.stamp,
        change.at,
        outcome.metric.name,
        outcome.metric.better,
        outcome.control_n,
        outcome.treatment_n,
        result is not None,
    )
    if result is None:
        return (*sizes, None, None, None, None, None, None, None, None, None, outcome.verdict, now)
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
    return (*sizes, *estimates, outcome.holm_p, outcome.bh_p, outcome.verdict, now)


def build_experiments(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SEARCH_EVENTS, lake) or not exists(spark, LAB_RUNS, lake):
        return 0
    hours = search_hours(read(spark, SEARCH_EVENTS, lake))
    runs = finished_runs(read(spark, LAB_RUNS, lake))
    rows: list[tuple[object, ...]] = []
    effects: dict[tuple[str, str], Effect] = {}
    for change, before, after in experiments(deployed_changes(runs), hours, now):
        outcomes = analyzed(before, after)
        rows += [result_row(change, outcome, now) for outcome in outcomes]
        effects[(change.game, change.stamp)] = effect_of(primary(outcomes), harm(before, after))
    replace(LAB_EXPERIMENTS, lake, spark.createDataFrame(rows, RESULT), gate)
    roi = lab_roi_rows(runs, spends(spark, lake), effects, now)
    replace(LAB_ROI, lake, spark.createDataFrame(roi, ROI), gate)
    return len(rows) + len(roi)
