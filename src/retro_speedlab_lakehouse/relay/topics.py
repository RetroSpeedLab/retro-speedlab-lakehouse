from concurrent.futures import Future

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from loguru import logger

from retro_speedlab_lakehouse.settings import KafkaSettings

PARTITIONS = 1
REPLICAS = 1
MILLISECONDS_PER_HOUR = 3_600_000
TIMEOUT_SECONDS = 30.0


def admin_created(admin: AdminClient, topics: list[NewTopic]) -> list[str]:
    futures: dict[str, Future[None]] = admin.create_topics(topics)  # pyright: ignore[reportUnknownVariableType]
    for future in futures.values():
        future.result()
    return list(futures)


def create_topics(kafka: KafkaSettings) -> None:
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap})
    existing = set(admin.list_topics(timeout=TIMEOUT_SECONDS).topics)
    config = {"retention.ms": str(kafka.retention_hours * MILLISECONDS_PER_HOUR), "compression.type": "zstd"}
    missing = [
        NewTopic(name, num_partitions=PARTITIONS, replication_factor=REPLICAS, config=config)
        for name in kafka.backend_topics + kafka.planned_topics
        if name not in existing
    ]
    for name in admin_created(admin, missing):
        logger.info(f"Created Kafka topic {name}")
