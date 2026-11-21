import json
import threading

import httpx
from confluent_kafka import KafkaException, Producer
from loguru import logger

from retro_speedlab_lakehouse.relay.cursor import read_cursor, write_cursor
from retro_speedlab_lakehouse.relay.outbox import OutboxEvent, backend_client, events_after
from retro_speedlab_lakehouse.settings import Settings

TIMEOUT_SECONDS = 30.0
FLUSH_SECONDS = 30.0


def kafka_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 20,
        }
    )


def encoded(event: OutboxEvent) -> bytes:
    return json.dumps(event.model_dump(mode="json"), separators=(",", ":")).encode("utf-8")


def publish(producer: Producer, events: list[OutboxEvent], topics: list[str]) -> None:
    for event in events:
        if event.topic not in topics:
            raise ValueError(f"The backend sent an event for {event.topic}, which is no backend topic")
        producer.produce(event.topic, key=event.key.encode("utf-8"), value=encoded(event))
    if producer.flush(FLUSH_SECONDS) > 0:
        raise KafkaException(f"Kafka did not confirm {len(producer)} events within {FLUSH_SECONDS} s")


def relay(settings: Settings, stopped: threading.Event) -> None:
    producer = kafka_producer(settings.kafka.bootstrap)
    cursor = read_cursor(settings.relay.cursor)
    logger.info(f"The relay moves the backend's events after {cursor} to Kafka")
    failing = False
    with backend_client(settings.backend.url, settings.backend.api_key, TIMEOUT_SECONDS) as client:
        while not stopped.is_set():
            try:
                events = events_after(client, cursor, settings.relay.batch)
            except httpx.HTTPError as error:
                if not failing:
                    logger.warning(f"The relay cannot reach the backend: {error}")
                failing = True
                stopped.wait(settings.relay.poll_seconds)
                continue
            if failing:
                logger.info("The relay reaches the backend again")
            failing = False
            if events:
                try:
                    publish(producer, events, settings.kafka.backend_topics)
                except KafkaException as error:
                    logger.warning(f"The relay cannot reach Kafka and retries the events after {cursor}: {error}")
                    stopped.wait(settings.relay.poll_seconds)
                    continue
                cursor = events[-1].id
                write_cursor(settings.relay.cursor, cursor)
            if len(events) < settings.relay.batch:
                stopped.wait(settings.relay.poll_seconds)
