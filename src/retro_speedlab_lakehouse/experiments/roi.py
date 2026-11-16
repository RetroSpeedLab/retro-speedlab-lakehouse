from dataclasses import dataclass
from datetime import datetime

from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.experiments.analysis import Outcome
from retro_speedlab_lakehouse.experiments.sequential import Harm

DEPLOYED = "deployed"
ROI = StructType(
    [
        StructField("game", StringType(), False),
        StructField("stamp", StringType(), False),
        StructField("outcome", StringType(), False),
        StructField("finished_at", TimestampType(), False),
        StructField("observations", LongType(), True),
        StructField("tokens", LongType(), True),
        StructField("cost_usd", DoubleType(), True),
        StructField("reported_cost_usd", DoubleType(), False),
        StructField("searched_hours", LongType(), False),
        StructField("frames_saved", DoubleType(), True),
        StructField("frames_saved_lower", DoubleType(), True),
        StructField("frames_saved_upper", DoubleType(), True),
        StructField("frames_per_dollar", DoubleType(), True),
        StructField("counted", BooleanType(), False),
        StructField("total_cost_usd", DoubleType(), False),
        StructField("total_frames_saved", DoubleType(), False),
        StructField("total_frames_per_dollar", DoubleType(), True),
        StructField("harm_flagged", BooleanType(), True),
        StructField("harm_after_hours", LongType(), True),
        StructField("computed_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class LabRun:
    game: str
    stamp: str
    outcome: str
    finished_at: datetime
    reported_cost_usd: float


@dataclass(frozen=True, slots=True)
class Spend:
    observations: int
    tokens: int
    cost_usd: float


@dataclass(frozen=True, slots=True)
class Effect:
    hours: int
    frames: float | None
    lower: float | None
    upper: float | None
    harm: Harm | None


NO_CHANGE = Effect(0, 0.0, 0.0, 0.0, None)
PENDING = Effect(0, None, None, None, None)


def effect_of(outcome: Outcome, harm: Harm | None) -> Effect:
    hours, result = outcome.treatment_n, outcome.result
    if result is None:
        return Effect(hours, None, None, None, harm)
    lower, upper = result.confidence_interval.lower, result.confidence_interval.upper
    if lower is None or upper is None:
        return Effect(hours, None, None, None, harm)
    return Effect(hours, result.absolute_effect * hours, lower * hours, upper * hours, harm)


def run_effect(run: LabRun, effects: dict[tuple[str, str], Effect]) -> Effect:
    if run.outcome != DEPLOYED:
        return NO_CHANGE
    key = (run.game, run.stamp)
    return effects[key] if key in effects else PENDING


def per_dollar(frames: float | None, cost: float | None) -> float | None:
    if frames is None or cost is None or cost <= 0:
        return None
    return frames / cost


def lab_roi_rows(
    runs: list[LabRun], spends: dict[str, Spend], effects: dict[tuple[str, str], Effect], now: datetime
) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    total_cost, total_frames = 0.0, 0.0
    for run in sorted(runs, key=lambda run: run.finished_at):
        spend = spends[run.stamp] if run.stamp in spends else None
        cost = None if spend is None else spend.cost_usd
        effect = run_effect(run, effects)
        counted = per_dollar(effect.frames, cost) is not None
        if counted and cost is not None and effect.frames is not None:
            total_cost, total_frames = total_cost + cost, total_frames + effect.frames
        harm = effect.harm
        rows.append(
            (
                run.game,
                run.stamp,
                run.outcome,
                run.finished_at,
                None if spend is None else spend.observations,
                None if spend is None else spend.tokens,
                cost,
                run.reported_cost_usd,
                effect.hours,
                effect.frames,
                effect.lower,
                effect.upper,
                per_dollar(effect.frames, cost),
                counted,
                total_cost,
                total_frames,
                per_dollar(total_frames, total_cost),
                None if harm is None else harm.flagged,
                None if harm is None else harm.after_hours,
                now,
            )
        )
    return rows
