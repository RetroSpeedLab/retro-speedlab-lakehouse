from dataclasses import dataclass
from itertools import groupby

from experimentation_toolkit import (
    AnalysisResult,
    CorrectionMethod,
    DiagnosticStatus,
    analyze_continuous,
    analyze_proportion,
    check_sample_ratio_mismatch,
)

from retro_speedlab_lakehouse.experiments.analysis import BETTER, HIGHER, TOO_FEW, UNCLEAR, WORSE, adjusted, verdict

MISMATCH = "sample ratio mismatch"
AB = "ab"
HOLDOUT = "holdout"


@dataclass(frozen=True, slots=True)
class Arms:
    experiment: str
    kind: str
    control: str
    treatment: str
    control_share: float


@dataclass(frozen=True, slots=True)
class Finding:
    arms: Arms
    metric: str
    primary: bool
    control_n: int
    treatment_n: int
    result: AnalysisResult | None
    srm_p: float | None
    srm_ok: bool


@dataclass(frozen=True, slots=True)
class Judged:
    finding: Finding
    holm_p: float | None
    bh_p: float | None
    verdict: str
    winner: str | None


def sample_ratio(arms: Arms, control_n: int, treatment_n: int) -> tuple[float | None, bool]:
    if control_n + treatment_n == 0:
        return None, True
    checked = check_sample_ratio_mismatch(
        observed_counts={arms.control: control_n, arms.treatment: treatment_n},
        expected_proportions={arms.control: arms.control_share, arms.treatment: 1.0 - arms.control_share},
    )
    return checked.p_value, checked.status is DiagnosticStatus.PASS


def rate_finding(
    arms: Arms, metric: str, primary: bool, control: tuple[int, int], treatment: tuple[int, int], minimum: int
) -> Finding:
    (control_hits, control_n), (treatment_hits, treatment_n) = control, treatment
    srm_p, srm_ok = sample_ratio(arms, control_n, treatment_n)
    if min(control_n, treatment_n) < minimum:
        return Finding(arms, metric, primary, control_n, treatment_n, None, srm_p, srm_ok)
    result = analyze_proportion(
        control_successes=control_hits,
        control_total=control_n,
        treatment_successes=treatment_hits,
        treatment_total=treatment_n,
        metric_name=metric,
    )
    return Finding(arms, metric, primary, control_n, treatment_n, result, srm_p, srm_ok)


def mean_finding(
    arms: Arms, metric: str, primary: bool, control: list[float], treatment: list[float], minimum: int
) -> Finding:
    srm_p, srm_ok = sample_ratio(arms, len(control), len(treatment))
    if min(len(control), len(treatment)) < minimum:
        return Finding(arms, metric, primary, len(control), len(treatment), None, srm_p, srm_ok)
    result = analyze_continuous(control=control, treatment=treatment, metric_name=metric)
    return Finding(arms, metric, primary, len(control), len(treatment), result, srm_p, srm_ok)


def winner_of(finding: Finding, judged: str) -> str | None:
    if finding.arms.kind != AB or not finding.primary:
        return None
    if judged == BETTER:
        return finding.arms.treatment
    if judged == WORSE:
        return finding.arms.control
    return None


def judged_experiment(findings: list[Finding]) -> list[Judged]:
    tested = {
        finding.metric: finding.result.p_value
        for finding in findings
        if finding.result is not None and finding.result.p_value is not None
    }
    holm = adjusted(tested, CorrectionMethod.HOLM)
    bh = adjusted(tested, CorrectionMethod.BENJAMINI_HOCHBERG)
    judged: list[Judged] = []
    for finding in findings:
        if finding.result is None or finding.metric not in holm:
            outcome = TOO_FEW if finding.result is None else UNCLEAR
            judged.append(Judged(finding, None, None, outcome, None))
            continue
        outcome = verdict(HIGHER, finding.result, holm[finding.metric]) if finding.srm_ok else MISMATCH
        judged.append(Judged(finding, holm[finding.metric], bh[finding.metric], outcome, winner_of(finding, outcome)))
    return judged


def judged_all(findings: list[Finding]) -> list[Judged]:
    ordered = sorted(findings, key=lambda finding: finding.arms.experiment)
    return [
        judged
        for _, group in groupby(ordered, key=lambda finding: finding.arms.experiment)
        for judged in judged_experiment(list(group))
    ]
