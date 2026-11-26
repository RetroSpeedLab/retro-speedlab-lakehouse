from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from confluent_kafka.cimpl import NewTopic
from typer.testing import CliRunner

from retro_speedlab_lakehouse import cli
from retro_speedlab_lakehouse.relay import topics
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.stream.rows import LIVE_TOPIC

NEW_TOPIC = "articles.metrics"


class Broker:
    def __init__(self, existing: set[str]) -> None:
        self.topics = existing
        self.created: list[NewTopic] = []

    def admin(self, config: dict[str, str]) -> "Broker":
        return self

    def list_topics(self, timeout: float) -> SimpleNamespace:
        return SimpleNamespace(topics={name: None for name in self.topics})

    def create_topics(self, new_topics: list[NewTopic]) -> dict[str, Future[None]]:
        futures: dict[str, Future[None]] = {}
        for new_topic in new_topics:
            self.topics.add(new_topic.topic)
            self.created.append(new_topic)
            futures[new_topic.topic] = Future()
            futures[new_topic.topic].set_result(None)
        return futures


def test_the_stream_creates_a_new_topic_on_the_broker_before_it_subscribes(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, tmp_path: Path
) -> None:
    subscribed = [LIVE_TOPIC, *settings.kafka.backend_topics, *settings.kafka.search_topics]
    broker = Broker({name for name in subscribed if name != NEW_TOPIC})
    missing_at_start: list[set[str]] = []

    def start(spark: MagicMock, started: Settings) -> None:
        missing_at_start.append(set(subscribed) - broker.topics)

    monkeypatch.setattr(topics, "AdminClient", broker.admin)
    monkeypatch.setattr(cli, "load_settings", MagicMock(return_value=settings))
    monkeypatch.setattr(cli, "spark_session", MagicMock())
    monkeypatch.setattr(cli, "start", start)

    result = CliRunner().invoke(cli.app, ["stream", str(tmp_path / "config.yaml")])

    assert result.exit_code == 0, result.output
    assert missing_at_start == [set()]
    assert [new_topic.topic for new_topic in broker.created] == [NEW_TOPIC]
    retention_ms = str(settings.kafka.retention_hours * topics.MILLISECONDS_PER_HOUR)
    assert broker.created[0].config == {"retention.ms": retention_ms, "compression.type": "zstd"}
