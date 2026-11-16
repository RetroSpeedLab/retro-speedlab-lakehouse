from dataclasses import dataclass

from experimentation_toolkit import AnalysisResult, analyze_continuous

from retro_speedlab_lakehouse.experiments.analysis import HIGHER, TOO_FEW, UNCLEAR, verdict

METRIC = "frames_saved_per_hour"
BANDIT = "bandit"
HOLDOUT = "holdout"
MIN_TURNS = 12
SECONDS_PER_HOUR = 3600.0


@dataclass(frozen=True, slots=True)
class Turn:
    game: str
    turn: int
    arm: str
    saved: int
    seconds: float

    @property
    def saved_per_hour(self) -> float:
        return self.saved / (self.seconds / SECONDS_PER_HOUR)


@dataclass(frozen=True, slots=True)
class Comparison:
    holdout: int
    bandit: int
    result: AnalysisResult | None
    verdict: str


def per_hour(turns: list[Turn], arm: str) -> list[float]:
    return [turn.saved_per_hour for turn in turns if turn.arm == arm]


def compared(turns: list[Turn]) -> Comparison:
    holdout, bandit = per_hour(turns, HOLDOUT), per_hour(turns, BANDIT)
    if min(len(holdout), len(bandit)) < MIN_TURNS:
        return Comparison(len(holdout), len(bandit), None, TOO_FEW)
    result = analyze_continuous(control=holdout, treatment=bandit, metric_name=METRIC)
    judged = UNCLEAR if result.p_value is None else verdict(HIGHER, result, result.p_value)
    return Comparison(len(holdout), len(bandit), result, judged)
