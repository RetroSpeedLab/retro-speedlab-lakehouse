from datetime import UTC, datetime, timedelta

from retro_speedlab_lakehouse.batch.night import outside_the_night
from retro_speedlab_lakehouse.experiments.analysis import BETTER, TOO_FEW, analyzed
from retro_speedlab_lakehouse.experiments.arms import Change, SearchHour, experiments

CHANGED = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)


def hour(offset: int, saved: int, crossings: int) -> SearchHour:
    return SearchHour("comet_courier", CHANGED + timedelta(hours=offset), 40, crossings, saved, 0, 0)


def test_a_change_with_enough_hours_is_judged_by_its_interval_and_holm() -> None:
    before = [hour(-index - 1, 10 + index % 3, 10) for index in range(24)]
    after = [hour(index + 1, 40 + index % 3, 30) for index in range(24)]
    ((_, control, treatment),) = experiments(
        [Change("comet_courier", "2026-11-15T1130", CHANGED)], before + after, CHANGED + timedelta(days=2)
    )
    assert (len(control.hours), len(treatment.hours)) == (24, 24)
    outcomes = {outcome.metric.name: outcome for outcome in analyzed(control, treatment)}
    assert outcomes["frames_saved_per_hour"].verdict == BETTER
    assert outcomes["crossing_rate"].verdict == BETTER
    assert outcomes["frames_saved_per_hour"].holm_p is not None
    assert outcomes["verify_failure_rate"].verdict == TOO_FEW
    assert outcomes["verify_failure_rate"].result is None


def test_a_change_without_enough_hours_claims_nothing() -> None:
    hours = [hour(-1, 10, 10), hour(2, 90, 40)]
    ((_, control, treatment),) = experiments(
        [Change("comet_courier", "2026-11-15T1130", CHANGED)], hours, CHANGED + timedelta(days=1)
    )
    assert {outcome.verdict for outcome in analyzed(control, treatment)} == {TOO_FEW}


def test_nightly_jobs_run_only_between_two_and_six_utc() -> None:
    assert outside_the_night("maintenance", datetime(2026, 11, 15, 20, 0, tzinfo=UTC))
    assert not outside_the_night("maintenance", datetime(2026, 11, 16, 2, 30, tzinfo=UTC))
    assert not outside_the_night("gold", datetime(2026, 11, 15, 20, 0, tzinfo=UTC))
