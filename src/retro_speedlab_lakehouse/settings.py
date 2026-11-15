import os
from pathlib import Path
from typing import Any

import yaml
from box import Box
from pydantic import BaseModel, ConfigDict, NonNegativeInt, PositiveFloat, PositiveInt


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class BackendSettings(Frozen):
    url: str
    api_key: str


class KafkaSettings(Frozen):
    bootstrap: str
    backend_topics: list[str]
    planned_topics: list[str]
    retention_hours: PositiveInt


class RelaySettings(Frozen):
    poll_seconds: PositiveFloat
    batch: PositiveInt
    cursor: Path


class LakeSettings(Frozen):
    bronze: str
    serving: str
    checkpoints: Path
    endpoint: str
    access_key: str
    secret_key: str


class SparkSettings(Frozen):
    master: str
    driver_memory: str
    shuffle_partitions: PositiveInt
    jars_dir: Path | None
    packages: list[str]


class StreamSettings(Frozen):
    live_trigger_seconds: PositiveInt
    silver_trigger_seconds: PositiveInt
    status_seconds: NonNegativeInt


class Settings(Frozen):
    backend: BackendSettings
    kafka: KafkaSettings
    relay: RelaySettings
    lake: LakeSettings
    spark: SparkSettings
    stream: StreamSettings
    sources: dict[str, Any]


def secret(name: str) -> str:
    if name not in os.environ:
        raise RuntimeError(f"Set {name}")
    return os.environ[name]


def load_settings(config: Path) -> Settings:
    document: Any = Box(yaml.safe_load(config.read_text(encoding="utf-8")), box_dots=False)
    return Settings(
        backend=BackendSettings(url=document.backend.url, api_key=secret("UPLOAD_API_KEY")),
        kafka=KafkaSettings(**document.kafka.to_dict()),
        relay=RelaySettings(**document.relay.to_dict()),
        lake=LakeSettings(
            **document.lake.to_dict(),
            access_key=secret("LAKE_ACCESS_KEY_ID"),
            secret_key=secret("LAKE_SECRET_ACCESS_KEY"),
        ),
        spark=SparkSettings(**document.spark.to_dict()),
        stream=StreamSettings(**document.stream.to_dict()),
        sources=document.sources.to_dict(),
    )
