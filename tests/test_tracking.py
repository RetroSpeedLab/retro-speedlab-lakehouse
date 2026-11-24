import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from retro_speedlab_lakehouse.lake.catalog import SEARCH_EVENTS
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.lake.write import append
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import PostgresSettings, Settings
from retro_speedlab_lakehouse.stream import search_rows
from retro_speedlab_lakehouse.stream.rows import KAFKA_ROW
from retro_speedlab_lakehouse.tracking import job
from retro_speedlab_lakehouse.tracking.catalog import SEARCH_TURNS, SEARCH_WORKER_KINDS, TRAINING_ROUNDS
from retro_speedlab_lakehouse.tracking.job import load_tracking

STARTED = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=3)
GAME = "tiltknight"
RUN = {"game": GAME, "game_version": "1.4.0", "release": "lab24"}
HYPERPARAMETERS = {"category": "Any%", "learning_rate": 0.0003}


def event(topic: str, payload: dict[str, Any], at: datetime) -> dict[str, Any]:
    return {"topic": topic, "key": GAME, "at": at.isoformat(), "payload": {**payload, "at": at.isoformat()}}


def turn_minute(run: str, step: int, final: bool) -> dict[str, Any]:
    kinds = {
        "scout": {"frames_saved_per_worker_second": 0.03, "worker_share": 0.75},
        "polisher": {"frames_saved_per_worker_second": 0.01, "worker_share": 0.25},
    }
    minute = {
        **RUN,
        "run": run,
        "started_at": STARTED.isoformat(),
        "turn": 4801,
        "arm": "holdout",
        "workers": {"scout": 3, "polisher": 1},
        "step": step,
        "frames_saved": 6,
        "best_seconds": 300.0 - step,
        "new_records": 1 if step == 0 else 0,
        "crossings": 10,
        "attempts_per_second": 4.0,
        "stuck_states": 2,
        "kinds": kinds,
        "final": final,
    }
    return event("runs.turn_minutes", minute, STARTED + timedelta(minutes=step + 1))


def ppo_round(run: str, number: int, started: datetime) -> dict[str, Any]:
    learned = {} if number == 1 else {"loss": 0.5, "entropy": 1.2, "explained_variance": 0.1}
    payload = {
        **RUN,
        "run": run,
        "started_at": started.isoformat(),
        "hyperparameters": HYPERPARAMETERS,
        "round": number,
        "steps": number * 2048,
        "reward": (number - 1) * 10.0,
        "exits": 1,
        "episodes": 4,
        "exit_rate": 0.25,
        **learned,
    }
    return event("policy.rounds", payload, started + timedelta(minutes=number))


def stored(spark: SparkSession, settings: Settings, found: list[dict[str, Any]]) -> None:
    rows = [(json.dumps(item).encode("utf-8"), 0, offset, datetime.now(UTC)) for offset, item in enumerate(found)]
    parsed = search_rows.parsed(spark.createDataFrame(rows, KAFKA_ROW))
    append(SEARCH_EVENTS, settings.lake, search_rows.bronze_rows(parsed), Gate("tracking"))


def rows(spark: SparkSession, settings: Settings, table: Any) -> list[dict[str, Any]]:
    return [row.asDict() for row in read(spark, table, settings.lake).collect()]


def test_turn_minutes_and_ppo_rounds_reach_gold_and_grafana(
    spark: SparkSession, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    published: dict[str, int] = {}

    def publish(database: PostgresSettings, tables: dict[str, DataFrame]) -> int:
        published.update({name: frame.count() for name, frame in tables.items()})
        return sum(published.values())

    monkeypatch.setattr(job, "publish", publish)
    minutes = [turn_minute("turn1", step, step == 59) for step in range(60)]
    older = STARTED - timedelta(hours=1)
    rounds = [ppo_round("old", number, older) for number in range(1, 4)]
    rounds += [ppo_round("new", number, STARTED) for number in range(1, 13)]
    aim_era = event("policy.rounds", {"game": GAME, "run": "aimhash", "round": 1, "reward": 1.0}, STARTED)
    stored(spark, settings, [*minutes, minutes[5], *rounds, aim_era])

    load_tracking(spark, settings, Gate("tracking"), datetime.now(UTC))

    [turn] = rows(spark, settings, SEARCH_TURNS)
    assert (turn["turn"], turn["arm"], turn["minutes"], turn["running"]) == (4801, "holdout", 60, False)
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
    trained = sorted(rows(spark, settings, TRAINING_ROUNDS), key=lambda row: row["round"])
    assert {row["run"] for row in trained} == {"new"}
    assert [row["reward"] for row in trained][:3] == [0.0, 10.0, 20.0]
    assert trained[-1]["reward_trend"] == pytest.approx(65.0)
    assert trained[0]["loss"] is None and "hyperparameters" not in trained[0]
    assert published["search_turn_minutes"] == 60
    assert published["ppo_rounds"] == 15
