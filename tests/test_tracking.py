import json
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.tracking.catalog import AIM_METRICS, SEARCH_TURNS, SEARCH_WORKER_KINDS, TRAINING_ROUNDS
from retro_speedlab_lakehouse.tracking.job import load_tracking

AIM = "https://aim.test/api"
EXPERIMENT = "lab24"
STARTED = datetime(2026, 11, 21, 10, 0, tzinfo=UTC)
TURN = "turn0001"
PPO = "ppo00001"
LEGACY = "legacy01"
TURN_PARAMS = {
    "kind": "search_turn",
    "game": "tiltknight",
    "game_version": "1.4.0",
    "release": "lab24",
    "turn": 4801,
    "arm": "holdout",
    "workers": {"scout": 3, "polisher": 1},
}
PPO_PARAMS: dict[str, Any] = {
    "kind": "ppo_round",
    "game": "tiltknight",
    "game_version": "1.4.0",
    "release": "lab24",
    "hparams": {},
}


def seconds(minutes: int) -> float:
    return (STARTED + timedelta(minutes=minutes)).timestamp()


def trace(name: str, context: dict[str, str], values: Sequence[float | None]) -> dict[str, Any]:
    return {"name": name, "context": context, "iters": list(range(len(values))), "values": values}


def turn_traces(minutes: int) -> list[dict[str, Any]]:
    own = {
        "frames_saved": [6.0] * minutes,
        "best_seconds": [300.0 - minute for minute in range(minutes)],
        "new_records": [1.0] + [0.0] * (minutes - 1),
        "crossings": [10.0] * minutes,
        "attempts_per_second": [4.0] * minutes,
        "stuck_states": [2.0] * minutes,
    }
    scout = {"frames_saved_per_worker_second": [0.03] * minutes, "worker_share": [0.75] * minutes}
    polisher = {"frames_saved_per_worker_second": [0.01] * minutes, "worker_share": [0.25] * minutes}
    return [
        *(trace(name, {}, values) for name, values in own.items()),
        *(trace(name, {"kind": "scout"}, values) for name, values in scout.items()),
        *(trace(name, {"kind": "polisher"}, values) for name, values in polisher.items()),
    ]


PPO_TRACES = [
    trace("policy/reward", {}, [float(round_ * 10) for round_ in range(12)]),
    trace("policy/exits", {}, [1.0] * 12),
    trace("policy/episodes", {}, [4.0] * 12),
    trace("policy/exit_rate", {}, [0.25] * 12),
    trace("policy/loss", {}, [None] + [0.5] * 11),
    trace("__system__cpu", {}, [12.0] * 12),
]


class FakeAim:
    def __init__(self) -> None:
        self.turn_minutes = 30
        self.turn_ended: float | None = None
        self.fetched: list[str] = []

    def runs(self) -> dict[str, tuple[dict[str, Any], float, float | None, list[dict[str, Any]]]]:
        return {
            TURN: (TURN_PARAMS, seconds(60), self.turn_ended, turn_traces(self.turn_minutes)),
            PPO: (PPO_PARAMS, seconds(0), seconds(50), PPO_TRACES),
            LEGACY: ({"job": "policy", "hparams": {"game": "tiltknight"}}, seconds(-60), seconds(-10), PPO_TRACES),
        }

    def listing(self, request: httpx.Request) -> httpx.Response:
        runs = [{"run_id": run, "creation_time": created} for run, (_, created, _, _) in self.runs().items()]
        return httpx.Response(200, json={"id": EXPERIMENT, "runs": runs})

    def info(self, request: httpx.Request, run: str) -> httpx.Response:
        self.fetched.append(run)
        params, created, ended, traces = self.runs()[run]
        props = {"name": run, "experiment": {"id": EXPERIMENT}, "creation_time": created, "end_time": ended}
        metric = [{"name": item["name"], "context": item["context"]} for item in traces]
        return httpx.Response(200, json={"params": params, "traces": {"metric": metric}, "props": props})

    def batch(self, request: httpx.Request, run: str) -> httpx.Response:
        wanted = {(item["name"], json.dumps(item["context"])) for item in json.loads(request.content)}
        traces = self.runs()[run][3]
        return httpx.Response(
            200, json=[item for item in traces if (item["name"], json.dumps(item["context"])) in wanted]
        )


@pytest.fixture
def aim() -> Iterator[FakeAim]:
    fake = FakeAim()
    with respx.mock(assert_all_called=False) as mock:
        mock.get(f"{AIM}/experiments/").respond(json=[{"id": EXPERIMENT}])
        mock.get(f"{AIM}/experiments/{EXPERIMENT}/runs/").mock(side_effect=fake.listing)
        mock.get(url__regex=rf"{AIM}/runs/(?P<run>\w+)/info/").mock(side_effect=fake.info)
        mock.post(url__regex=rf"{AIM}/runs/(?P<run>\w+)/metric/get-batch/").mock(side_effect=fake.batch)
        yield fake


def rows(spark: SparkSession, settings: Settings, table: Any) -> list[dict[str, Any]]:
    return [row.asDict() for row in read(spark, table, settings.lake).collect()]


def test_search_turns_and_ppo_rounds_reach_gold_and_only_open_runs_load_again(
    spark: SparkSession, settings: Settings, aim: FakeAim
) -> None:
    now = STARTED + timedelta(hours=2)
    load_tracking(spark, settings.lake, Gate("tracking"), settings.sources.aim, now)
    aim.turn_minutes, aim.turn_ended, aim.fetched = 60, seconds(120), []
    load_tracking(spark, settings.lake, Gate("tracking"), settings.sources.aim, now)

    assert aim.fetched == [TURN]
    [turn] = rows(spark, settings, SEARCH_TURNS)
    assert (turn["game"], turn["turn"], turn["arm"], turn["minutes"], turn["running"]) == (
        "tiltknight",
        4801,
        "holdout",
        60,
        False,
    )
    assert (turn["frames_saved"], turn["frames_saved_per_hour"], turn["best_seconds"], turn["new_records"]) == (
        360,
        360.0,
        241.0,
        1,
    )
    kinds = {row["kind"]: row for row in rows(spark, settings, SEARCH_WORKER_KINDS)}
    assert {kind: (row["workers"], row["worker_share"]) for kind, row in kinds.items()} == {
        "scout": (3, 0.75),
        "polisher": (1, 0.25),
    }
    assert kinds["scout"]["frames_saved_per_worker_second"] == pytest.approx(0.03)
    rounds = sorted(rows(spark, settings, TRAINING_ROUNDS), key=lambda row: row["round"])
    assert {row["run"] for row in rounds} == {PPO}
    assert [row["reward"] for row in rounds][:3] == [0.0, 10.0, 20.0]
    assert rounds[-1]["reward_trend"] == pytest.approx(65.0)
    assert rounds[0]["loss"] is None
    metrics = {row["metric"] for row in rows(spark, settings, AIM_METRICS)}
    assert "__system__cpu" not in metrics
