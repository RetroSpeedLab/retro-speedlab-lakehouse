import json
import ssl
import threading
from datetime import UTC, datetime

from confluent_kafka import Producer
from loguru import logger

from retro_speedlab_lakehouse.chat.irc import PING, PONG, connected, lines, sender
from retro_speedlab_lakehouse.chat.minutes import MINUTE, fresh_minute
from retro_speedlab_lakehouse.relay.producer import kafka_producer
from retro_speedlab_lakehouse.settings import ChatSettings, Settings

TOPIC = "chat.events"
RECONNECT_SECONDS = 30.0


def publish(producer: Producer, channel: str, payload: dict[str, str | int]) -> None:
    event = {"topic": TOPIC, "key": channel, "at": payload["at"], "payload": payload}
    producer.produce(TOPIC, json.dumps(event).encode("utf-8"), channel.encode("utf-8"))
    producer.poll(0)


def listen(chat: ChatSettings, producer: Producer, connection: ssl.SSLSocket, stopped: threading.Event) -> None:
    minute = fresh_minute(datetime.now(UTC))
    for line in lines(connection):
        if stopped.is_set():
            return
        now = datetime.now(UTC)
        if now >= minute.started + MINUTE:
            publish(producer, chat.channel, minute.payload(chat.channel, now))
            minute = fresh_minute(now)
        if line is None:
            continue
        if line.startswith(PING):
            connection.sendall(PONG)
            continue
        name = sender(line)
        if name is not None and name not in chat.ignored:
            minute.heard(name)


def watch_chat(settings: Settings, stopped: threading.Event) -> None:
    producer = kafka_producer(settings.kafka.bootstrap)
    logger.info("Counting the Twitch chat per minute, without names or messages")
    while not stopped.is_set():
        try:
            with connected(settings.chat.channel) as connection:
                listen(settings.chat, producer, connection, stopped)
        except (OSError, ConnectionError) as error:
            logger.warning(f"The chat connection broke: {error!r}; the minutes since are not counted")
            stopped.wait(RECONNECT_SECONDS)
    producer.flush(RECONNECT_SECONDS)
