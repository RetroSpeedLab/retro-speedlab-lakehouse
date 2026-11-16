from dataclasses import dataclass

from experimentation_toolkit import AlternativeHypothesis, SpendingFunction, analyze_sequential_continuous

from retro_speedlab_lakehouse.experiments.analysis import ALPHA
from retro_speedlab_lakehouse.experiments.arms import MAX_HOURS, Arm

FIRST_LOOK = 2


@dataclass(frozen=True, slots=True)
class Harm:
    flagged: bool
    after_hours: int | None


def harm(before: Arm, after: Arm) -> Harm | None:
    control, treatment = before.saved, after.saved
    if len(control) < FIRST_LOOK or len(treatment) < FIRST_LOOK:
        return None
    looks = list(range(FIRST_LOOK, len(treatment) + 1))
    try:
        result = analyze_sequential_continuous(
            control=control,
            treatment=treatment,
            control_looks=[len(control)] * len(looks),
            treatment_looks=looks,
            maximum_control_size=len(control),
            maximum_treatment_size=MAX_HOURS,
            alpha=ALPHA,
            alternative=AlternativeHypothesis.LESS,
            spending=SpendingFunction.O_BRIEN_FLEMING,
            metric_name="frames_saved_per_hour",
        )
    except ValueError:
        return None
    if result.crossed_at_look is None:
        return Harm(False, None)
    return Harm(True, looks[result.crossed_at_look])
