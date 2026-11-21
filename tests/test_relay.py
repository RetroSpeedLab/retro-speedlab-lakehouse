import threading
from contextlib import nullcontext
from datetime import UTC, datetime

import httpx
import pytest

from retro_speedlab_lakehouse.relay import producer
from retro_speedlab_lakehouse.relay.cursor import read_cursor, write_cursor
from retro_speedlab_lakehouse.relay.outbox import OutboxEvent
from retro_speedlab_lakehouse.settings import Settings

EVENT = OutboxEvent(id=7, topic="runs.live", key="duskstep", at=datetime(2026, 11, 21, 22, 17, tzinfo=UTC), payload={})


class KafkaDownOnce:
    def __init__(self) -> None:
        self.flushes = 0
        self.produced: list[str] = []

    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        self.produced.append(topic)

    def flush(self, timeout: float) -> int:
        self.flushes += 1
        return 1 if self.flushes == 1 else 0

    def __len__(self) -> int:
        return 1


def test_kafka_that_does_not_confirm_a_batch_keeps_the_relay_polling_until_it_does(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    kafka = KafkaDownOnce()
    stopped = threading.Event()
    polls: list[int] = []

    def events_after(client: httpx.Client, cursor: int, limit: int) -> list[OutboxEvent]:
        polls.append(cursor)
        if cursor == EVENT.id:
            stopped.set()
            return []
        return [EVENT]

    def kafka_producer(bootstrap: str) -> KafkaDownOnce:
        return kafka

    def backend_client(url: str, api_key: str, timeout_seconds: float) -> nullcontext[None]:
        return nullcontext()

    def wait(timeout: float) -> bool:
        return False

    write_cursor(settings.relay.cursor, 0)
    monkeypatch.setattr(producer, "kafka_producer", kafka_producer)
    monkeypatch.setattr(producer, "backend_client", backend_client)
    monkeypatch.setattr(producer, "events_after", events_after)
    monkeypatch.setattr(stopped, "wait", wait)

    producer.relay(settings, stopped)

    assert polls == [0, 0, EVENT.id]
    assert kafka.produced == ["runs.live", "runs.live"]
    assert read_cursor(settings.relay.cursor) == EVENT.id
