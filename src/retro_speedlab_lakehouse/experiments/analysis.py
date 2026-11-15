from collections.abc import Callable
from dataclasses import dataclass

from experimentation_toolkit import (
    AnalysisResult,
    CorrectionMethod,
    adjust_p_values,
    analyze_continuous,
    analyze_proportion,
)

from retro_speedlab_lakehouse.experiments.arms import Arm

ALPHA = 0.05
MIN_HOURS = 12
MIN_TRIALS = 30
HIGHER = "higher"
LOWER = "lower"
BETTER = "better"
WORSE = "worse"
UNCLEAR = "no clear change"
TOO_FEW = "not enough data yet"


@dataclass(frozen=True, slots=True)
class Metric:
    name: str
    better: str
    samples: Callable[[Arm], int]
    minimum: int
    analyze: Callable[[Arm, Arm], AnalysisResult]


@dataclass(frozen=True, slots=True)
class Outcome:
    metric: Metric
    control_n: int
    treatment_n: int
    result: AnalysisResult | None
    holm_p: float | None
    bh_p: float | None
    verdict: str


def frames_saved(before: Arm, after: Arm) -> AnalysisResult:
    return analyze_continuous(control=before.saved, treatment=after.saved, metric_name="frames_saved_per_hour")


def crossing_rate(before: Arm, after: Arm) -> AnalysisResult:
    return analyze_proportion(
        control_successes=before.crossings,
        control_total=before.attempts,
        treatment_successes=after.crossings,
        treatment_total=after.attempts,
        metric_name="crossing_rate",
    )


def record_rate(before: Arm, after: Arm) -> AnalysisResult:
    return analyze_proportion(
        control_successes=before.record_hours,
        control_total=len(before.hours),
        treatment_successes=after.record_hours,
        treatment_total=len(after.hours),
        metric_name="record_hour_rate",
    )


def verify_failures(before: Arm, after: Arm) -> AnalysisResult:
    return analyze_proportion(
        control_successes=before.unverified,
        control_total=before.verified,
        treatment_successes=after.unverified,
        treatment_total=after.verified,
        metric_name="verify_failure_rate",
    )


METRICS = (
    Metric("frames_saved_per_hour", HIGHER, lambda arm: len(arm.hours), MIN_HOURS, frames_saved),
    Metric("crossing_rate", HIGHER, lambda arm: arm.attempts, MIN_TRIALS, crossing_rate),
    Metric("record_hour_rate", HIGHER, lambda arm: len(arm.hours), MIN_HOURS, record_rate),
    Metric("verify_failure_rate", LOWER, lambda arm: arm.verified, MIN_TRIALS, verify_failures),
)


def enough(metric: Metric, before: Arm, after: Arm) -> bool:
    searched = min(len(before.hours), len(after.hours)) >= MIN_HOURS
    return searched and metric.samples(before) >= metric.minimum and metric.samples(after) >= metric.minimum


def verdict(metric: Metric, result: AnalysisResult, adjusted: float) -> str:
    lower, upper = result.confidence_interval.lower, result.confidence_interval.upper
    if adjusted > ALPHA or lower is None or upper is None:
        return UNCLEAR
    improved = lower > 0 if metric.better == HIGHER else upper < 0
    worsened = upper < 0 if metric.better == HIGHER else lower > 0
    return BETTER if improved else WORSE if worsened else UNCLEAR


def analyzed(before: Arm, after: Arm) -> list[Outcome]:
    results = {metric.name: metric.analyze(before, after) for metric in METRICS if enough(metric, before, after)}
    tested = {name: result.p_value for name, result in results.items() if result.p_value is not None}
    holm = adjusted(tested, CorrectionMethod.HOLM)
    bh = adjusted(tested, CorrectionMethod.BENJAMINI_HOCHBERG)
    outcomes: list[Outcome] = []
    for metric in METRICS:
        sizes = (metric.samples(before), metric.samples(after))
        if metric.name not in results:
            outcomes.append(Outcome(metric, *sizes, None, None, None, TOO_FEW))
            continue
        result = results[metric.name]
        if metric.name not in holm:
            outcomes.append(Outcome(metric, *sizes, result, None, None, UNCLEAR))
            continue
        judged = verdict(metric, result, holm[metric.name])
        outcomes.append(Outcome(metric, *sizes, result, holm[metric.name], bh[metric.name], judged))
    return outcomes


def adjusted(p_values: dict[str, float], method: CorrectionMethod) -> dict[str, float]:
    if not p_values:
        return {}
    corrected = adjust_p_values(list(p_values.values()), method=method, alpha=ALPHA, hypothesis_labels=list(p_values))
    return dict(zip(p_values, corrected.adjusted_p_values, strict=True))
