import json
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pytest
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.bandit.allocation import build_allocation
from retro_speedlab_lakehouse.bandit.comparison import BANDIT, HOLDOUT, MIN_TURNS, Turn, compared
from retro_speedlab_lakehouse.bandit.holdout import build_bandit_comparison
from retro_speedlab_lakehouse.bandit.posterior import Effort, allocations
from retro_speedlab_lakehouse.experiments.analysis import BETTER, TOO_FEW
from retro_speedlab_lakehouse.lake.catalog import SEARCH_ALLOCATION, SEARCH_BANDIT, SEARCH_EVENTS
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.lake.write import append
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.stream import search_rows
from retro_speedlab_lakehouse.stream.rows import KAFKA_ROW

GAME = "comet_courier"
HOUR = 3600.0
FLOOR = 0.1
TURN = timedelta(hours=2)


def efforts() -> list[Effort]:
    return [
        Effort("scout", 120, 4, 4 * HOUR),
        Effort("polisher", 4000, 40, 4 * HOUR),
        Effort("beam-scout", 0, 0, 4 * HOUR),
        Effort("backtracker", 0, 0, 4 * HOUR),
    ]


def test_the_kind_that_saves_the_most_frames_per_worker_second_gets_the_most_workers() -> None:
    found = {found.effort.role: found for found in allocations(efforts(), FLOOR, 4000, np.random.default_rng(1))}

    assert sum(found.share for found in found.values()) == pytest.approx(1.0)
    assert max(found.values(), key=lambda found: found.share).effort.role == "polisher"
    assert found["polisher"].rate_lower < 4000 / (4 * HOUR) < found["polisher"].rate_upper


def test_every_kind_keeps_the_floor_so_the_search_goes_on_exploring() -> None:
    found = allocations(efforts(), FLOOR, 4000, np.random.default_rng(2))

    assert all(found.share >= FLOOR for found in found)
    assert {round(found.share, 6) for found in found if found.effort.role in ("beam-scout", "backtracker")} == {FLOOR}


def test_a_floor_larger_than_all_workers_fails_fast() -> None:
    with pytest.raises(ValueError, match="exceeds all workers"):
        allocations(efforts(), 0.3, 100, np.random.default_rng(3))


def turns(arm: str, saved: int) -> list[Turn]:
    return [Turn(GAME, index, arm, saved + index % 3, TURN.total_seconds()) for index in range(MIN_TURNS)]


def test_the_bandit_is_judged_against_the_holdout_by_its_interval() -> None:
    comparison = compared(turns(HOLDOUT, 100) + turns(BANDIT, 400))

    assert (comparison.holdout, comparison.bandit, comparison.verdict) == (MIN_TURNS, MIN_TURNS, BETTER)
    assert comparison.result is not None and comparison.result.confidence_interval.lower is not None


def test_without_enough_turns_on_both_sides_nothing_is_claimed() -> None:
    comparison = compared(turns(HOLDOUT, 100) + turns(BANDIT, 400)[:-1])

    assert (comparison.result, comparison.verdict) == (None, TOO_FEW)


def runner_event(topic: str, payload: dict[str, Any], at: datetime) -> dict[str, Any]:
    return {
        "topic": topic,
        "key": GAME,
        "at": at.isoformat(),
        "payload": {"game": GAME, **payload, "at": at.isoformat()},
    }


def attempt(role: str, seconds: float, at: datetime) -> dict[str, Any]:
    return runner_event("runs.attempts", {"role": role, "worker_seconds": seconds, "outcome": "nothing"}, at)


def improved(role: str | None, saved: int, at: datetime) -> dict[str, Any]:
    return runner_event("runs.steps", {"kind": "improved", "saved": saved, "role": role, "worker_seconds": 5.0}, at)


def allocated(turn: int, arm: str, started: datetime) -> dict[str, Any]:
    bounds = {"turn_started_at": started.isoformat(), "turn_ends_at": (started + TURN).isoformat()}
    return runner_event("runs.allocations", {"turn": turn, "arm": arm, **bounds}, started)


def stored(spark: SparkSession, settings: Settings, found: list[dict[str, Any]]) -> None:
    now = datetime.now(UTC)
    rows = [(json.dumps(item).encode("utf-8"), 0, offset, now) for offset, item in enumerate(found)]
    parsed = search_rows.parsed(spark.createDataFrame(rows, KAFKA_ROW))
    append(SEARCH_EVENTS, settings.lake, search_rows.bronze_rows(parsed), Gate("bandit"))


def test_the_allocation_learns_from_attributed_attempts_and_improvements(
    spark: SparkSession, settings: Settings
) -> None:
    now = datetime.now(UTC)
    stored(
        spark,
        settings,
        [
            attempt("scout", HOUR, now - timedelta(hours=1)),
            attempt("polisher", HOUR, now - timedelta(hours=1)),
            attempt("polisher", HOUR, now - timedelta(days=30)),
            improved("polisher", 900, now - timedelta(minutes=30)),
            improved(None, 5000, now - timedelta(minutes=30)),
        ],
    )

    assert build_allocation(spark, settings.lake, settings.bandit, Gate("bandit"), now) == 2
    found = {row["role"]: row for row in read(spark, SEARCH_ALLOCATION, settings.lake).collect()}
    assert (found["polisher"]["saved"], found["polisher"]["improvements"], found["polisher"]["worker_seconds"]) == (
        900,
        1,
        HOUR,
    )
    assert found["scout"]["saved"] == 0 and found["polisher"]["share"] > found["scout"]["share"] >= FLOOR
    assert sum(row["share"] for row in found.values()) == pytest.approx(1.0)


def test_finished_turns_of_both_arms_are_compared_on_the_frames_they_saved(
    spark: SparkSession, settings: Settings
) -> None:
    now = datetime.now(UTC)
    first = now - TURN * 3
    stored(
        spark,
        settings,
        [
            allocated(1, HOLDOUT, first),
            allocated(2, BANDIT, first + TURN),
            allocated(3, BANDIT, now - timedelta(minutes=10)),
            improved("polisher", 60, first + timedelta(minutes=5)),
            improved("scout", 240, first + TURN + timedelta(minutes=5)),
        ],
    )

    assert build_bandit_comparison(spark, settings.lake, Gate("bandit"), now) == 2
    row = read(spark, SEARCH_BANDIT, settings.lake).first()
    assert row is not None and (row["control_n"], row["treatment_n"], row["verdict"]) == (1, 1, TOO_FEW)
