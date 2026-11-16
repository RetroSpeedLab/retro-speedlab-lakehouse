import json
import ssl
import threading
from datetime import UTC, datetime

from confluent_kafka import Producer
from loguru import logger

from retro_speedlab_lakehouse.chat.irc import PING, PONG, connected, lines, sender
from retro_speedlab_lakehouse.chat.minutes import MINUTE, ChatMinute, fresh_minute
from retro_speedlab_lakehouse.chat.viewers import TOPIC as VIEWERS_TOPIC
from retro_speedlab_lakehouse.chat.viewers import ViewerCounter
from retro_speedlab_lakehouse.relay.producer import kafka_producer
from retro_speedlab_lakehouse.settings import ChatSettings, Settings

TOPIC = "chat.events"
RECONNECT_SECONDS = 30.0


def publish(producer: Producer, topic: str, channel: str, payload: dict[str, str | int | bool]) -> None:
    event = {"topic": topic, "key": channel, "at": payload["at"], "payload": payload}
    producer.produce(topic, json.dumps(event).encode("utf-8"), channel.encode("utf-8"))
    producer.poll(0)


def close_minute(
    chat: ChatSettings, producer: Producer, viewers: ViewerCounter, minute: ChatMinute, now: datetime
) -> None:
    publish(producer, TOPIC, chat.channel, minute.payload(chat.channel, now))
    counted = viewers.counted(minute.started, now)
    if counted is not None:
        publish(producer, VIEWERS_TOPIC, chat.channel, counted)


def listen(
    chat: ChatSettings,
    producer: Producer,
    viewers: ViewerCounter,
    connection: ssl.SSLSocket,
    stopped: threading.Event,
) -> None:
    listening_since = datetime.now(UTC)
    minute = fresh_minute(listening_since)
    for line in lines(connection):
        if stopped.is_set():
            return
        now = datetime.now(UTC)
        if now >= minute.started + MINUTE:
            if minute.started >= listening_since:
                close_minute(chat, producer, viewers, minute, now)
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
    viewers = ViewerCounter(settings.sources.twitch)
    logger.info("Counting the Twitch chat and viewers per minute, without names or messages")
    while not stopped.is_set():
        try:
            with connected(settings.chat.channel) as connection:
                listen(settings.chat, producer, viewers, connection, stopped)
        except (OSError, ConnectionError) as error:
            logger.warning(f"The chat connection broke: {error!r}; the minutes since are not counted")
            stopped.wait(RECONNECT_SECONDS)
    producer.flush(RECONNECT_SECONDS)
