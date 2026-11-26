from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from retro_speedlab_lakehouse import cli
from retro_speedlab_lakehouse.settings import KafkaSettings, Settings


def test_the_stream_creates_its_topics_before_it_subscribes(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, tmp_path: Path
) -> None:
    calls: list[str] = []
    created: list[KafkaSettings] = []

    def create_topics(kafka: KafkaSettings) -> None:
        calls.append("topics")
        created.append(kafka)

    def start(spark: MagicMock, started: Settings) -> None:
        calls.append("start")

    monkeypatch.setattr(cli, "load_settings", MagicMock(return_value=settings))
    monkeypatch.setattr(cli, "spark_session", MagicMock())
    monkeypatch.setattr(cli, "create_topics", create_topics)
    monkeypatch.setattr(cli, "start", start)

    result = CliRunner().invoke(cli.app, ["stream", str(tmp_path / "config.yaml")])

    assert result.exit_code == 0, result.output
    assert calls == ["topics", "start"]
    assert created == [settings.kafka]
