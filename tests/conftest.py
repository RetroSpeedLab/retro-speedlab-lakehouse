import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from retro_speedlab_lakehouse.settings import (
    BackendSettings,
    KafkaSettings,
    LakeSettings,
    MiningSettings,
    RelaySettings,
    Settings,
    SparkSettings,
    StreamSettings,
)
from retro_speedlab_lakehouse.spark.session import spark_session
from retro_speedlab_lakehouse.stream.rows import KAFKA_ROW

FIXTURES = Path(__file__).parent / "fixtures"
DELTA = "io.delta:delta-spark_4.1_2.13:4.4.1"


def fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def lake_settings(root: Path) -> LakeSettings:
    return LakeSettings(
        bronze=str(root / "bronze"),
        serving=str(root / "serving"),
        checkpoints=root / "checkpoints",
        endpoint="http://127.0.0.1:1",
        access_key="test",
        secret_key="test",
    )


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    settings = SparkSettings(
        master="local[1]", driver_memory="1g", shuffle_partitions=1, jars_dir=None, packages=[DELTA]
    )
    session = spark_session("lakehouse-tests", settings, lake_settings(tmp_path_factory.mktemp("unused")))
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        backend=BackendSettings(url="https://api.test", api_key="key"),
        kafka=KafkaSettings(
            bootstrap="kafka:9092",
            backend_topics=["runs.live", "runs.snapshots", "lab.runs", "lab.reports", "highscores.entries"],
            planned_topics=["runs.attempts"],
            retention_hours=168,
        ),
        relay=RelaySettings(poll_seconds=1, batch=2, cursor=tmp_path / "cursor"),
        lake=lake_settings(tmp_path),
        spark=SparkSettings(master="local[1]", driver_memory="1g", shuffle_partitions=1, jars_dir=None, packages=[]),
        stream=StreamSettings(live_trigger_seconds=2, silver_trigger_seconds=60, status_seconds=0),
        mining=MiningSettings(
            working=tmp_path / "working",
            speedlab=tmp_path / "speedlab",
            games_url="https://games.retrospeedlab.com",
            min_available_mb=1,
        ),
        sources={},
    )


def event(event_id: int, topic: str, key: str, payload: dict[str, Any], at: datetime) -> dict[str, Any]:
    return {"id": event_id, "topic": topic, "key": key, "at": at.isoformat(), "payload": payload}


def kafka_rows(spark: SparkSession, events: list[dict[str, Any]]) -> DataFrame:
    rows = [(json.dumps(item).encode("utf-8"), 0, offset, datetime.now(UTC)) for offset, item in enumerate(events)]
    return spark.createDataFrame(rows, KAFKA_ROW)
