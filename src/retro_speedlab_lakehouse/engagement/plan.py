from dataclasses import dataclass
from datetime import datetime
from math import ceil, inf

from experimentation_toolkit import MDEType, proportion_sample_size
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType
from scipy import stats

from retro_speedlab_lakehouse.engagement.analysis import AB, Baseline, Judged
from retro_speedlab_lakehouse.experiments.analysis import ALPHA
from retro_speedlab_lakehouse.settings import DAYS_PER_WEEK, EngagementSettings, ExperimentPlanSettings

RUNNING = "running"
QUEUED = "queued"
DECIDED = "decided"
VISIT = "visit"
SURFACES = {VISIT: "site", "time": "stream"}
ARMS = 2
PLAN = StructType(
    [
        StructField("experiment", StringType(), False),
        StructField("surface", StringType(), False),
        StructField("status", StringType(), False),
        StructField("exposures_per_week", DoubleType(), False),
        StructField("gathered_per_arm", LongType(), False),
        StructField("needed_per_arm", LongType(), True),
        StructField("weeks_to_decision", DoubleType(), True),
        StructField("alpha", DoubleType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class Candidate:
    experiment: str
    scope: str
    per_arm_week: float
    gathered: int
    minimum: int
    baseline: Baseline | None
    winner: str | None


def candidates(judged: list[Judged], settings: EngagementSettings) -> list[Candidate]:
    weeks = settings.window_days / DAYS_PER_WEEK
    minimums = {VISIT: settings.min_exposures, "time": settings.min_blocks}
    primary = {
        item.finding.arms.experiment: item for item in judged if item.finding.arms.kind == AB and item.finding.primary
    }
    found = [(name, experiment.scope, primary[name]) for name, experiment in settings.experiments.items()]
    return [
        Candidate(
            name,
            scope,
            (item.finding.control_n + item.finding.treatment_n) / ARMS / weeks,
            min(item.finding.control_n, item.finding.treatment_n),
            minimums[scope],
            item.finding.baseline,
            item.winner,
        )
        for name, scope, item in found
    ]


def rate_needed(baseline: Baseline, alpha: float, plan: ExperimentPlanSettings) -> int | None:
    if not 0 < baseline.mean < baseline.mean * (1 + plan.minimum_effect) < 1:
        return None
    return proportion_sample_size(
        baseline_rate=baseline.mean,
        minimum_detectable_effect=plan.minimum_effect,
        mde_type=MDEType.RELATIVE,
        alpha=alpha,
        power=plan.power,
    ).control_sample_size


def mean_needed(baseline: Baseline, alpha: float, plan: ExperimentPlanSettings) -> int | None:
    effect = plan.minimum_effect * abs(baseline.mean)
    if effect == 0:
        return None
    quantiles = stats.norm.ppf(1 - alpha / 2) + stats.norm.ppf(plan.power)
    return ceil(ARMS * (quantiles * baseline.deviation / effect) ** 2)


def needed(candidate: Candidate, alpha: float, plan: ExperimentPlanSettings) -> int | None:
    if candidate.baseline is None:
        return None
    sized = rate_needed if candidate.scope == VISIT else mean_needed
    found = sized(candidate.baseline, alpha, plan)
    return None if found is None else max(found, candidate.minimum)


def weeks_of_data(candidate: Candidate, alpha: float, plan: ExperimentPlanSettings) -> float:
    found = needed(candidate, alpha, plan)
    return inf if found is None or candidate.per_arm_week == 0 else found / candidate.per_arm_week


def running(group: list[Candidate], plan: ExperimentPlanSettings) -> list[Candidate]:
    undecided = sorted((item for item in group if item.winner is None), key=lambda item: -item.per_arm_week)
    chosen: list[Candidate] = []
    for candidate in undecided:
        trial = [*chosen, candidate]
        if all(weeks_of_data(item, ALPHA / len(trial), plan) <= plan.horizon_weeks for item in trial):
            chosen = trial
    if chosen or not undecided:
        return chosen
    return [min(undecided, key=lambda item: weeks_of_data(item, ALPHA, plan))]


def weeks_to_decision(candidate: Candidate, alpha: float, settings: EngagementSettings) -> float | None:
    found = needed(candidate, alpha, settings.plan)
    window_weeks = settings.window_days / DAYS_PER_WEEK
    if found is None or found > candidate.per_arm_week * window_weeks:
        return None
    return max(found - candidate.gathered, 0) / candidate.per_arm_week


def plan_row(
    candidate: Candidate, status: str, alpha: float, settings: EngagementSettings, now: datetime
) -> tuple[object, ...]:
    return (
        candidate.experiment,
        SURFACES[candidate.scope],
        status,
        candidate.per_arm_week * ARMS,
        candidate.gathered,
        needed(candidate, alpha, settings.plan),
        weeks_to_decision(candidate, alpha, settings),
        alpha,
        now,
    )


def plan_rows(judged: list[Judged], settings: EngagementSettings, now: datetime) -> list[tuple[object, ...]]:
    every = candidates(judged, settings)
    rows: list[tuple[object, ...]] = []
    for scope in SURFACES:
        group = [item for item in every if item.scope == scope]
        chosen = {item.experiment for item in running(group, settings.plan)}
        for candidate in group:
            if candidate.experiment in chosen:
                rows.append(plan_row(candidate, RUNNING, ALPHA / len(chosen), settings, now))
                continue
            status = QUEUED if candidate.winner is None else DECIDED
            rows.append(plan_row(candidate, status, ALPHA / (len(chosen) + 1), settings, now))
    return rows
