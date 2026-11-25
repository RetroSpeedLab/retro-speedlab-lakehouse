from pathlib import Path
from typing import Any, Literal

import yaml
from box import Box
from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveFloat, PositiveInt, model_validator

from retro_speedlab_lakehouse.environment import secret
from retro_speedlab_lakehouse.sources.settings import (
    BlueskySettings,
    GithubSettings,
    Sources,
    TempoSettings,
    UptimeSettings,
    load_sources,
)

DAYS_PER_WEEK = 7


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class BackendSettings(Frozen):
    url: str
    api_key: str


class KafkaSettings(Frozen):
    bootstrap: str
    backend_topics: list[str]
    search_topics: list[str]
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
    search_trigger_seconds: PositiveInt
    status_seconds: NonNegativeInt


class MiningSettings(Frozen):
    working: Path
    speedlab: Path
    games_url: str
    min_available_mb: PositiveInt


class BanditSettings(Frozen):
    min_share: float = Field(gt=0, lt=1)
    days: PositiveInt
    draws: PositiveInt


class EngagementSurface(Frozen):
    family: Literal["beta", "gamma"]
    metric: str
    holdout: bool
    catalog: bool


class EngagementExperiment(Frozen):
    scope: Literal["visit", "time"]
    variants: tuple[str, str]
    goals: tuple[str, ...]


class ExperimentPlanSettings(Frozen):
    horizon_weeks: PositiveInt
    minimum_effect: float = Field(gt=0, lt=1)
    power: float = Field(gt=0.5, lt=1)


class EngagementSettings(Frozen):
    window_days: PositiveInt
    backfill_hours: PositiveInt
    prior_weight: PositiveFloat
    min_visibility: float = Field(gt=0, lt=1)
    draws: PositiveInt
    holdout_share: float = Field(gt=0, lt=0.5)
    block_minutes: PositiveInt
    min_exposures: PositiveInt
    min_hours: PositiveInt
    min_blocks: PositiveInt
    plan: ExperimentPlanSettings
    surfaces: dict[str, EngagementSurface]
    experiments: dict[str, EngagementExperiment]

    @model_validator(mode="after")
    def horizon_in_window(self) -> "EngagementSettings":
        if self.plan.horizon_weeks * DAYS_PER_WEEK > self.window_days:
            raise ValueError(
                f"A test must decide within its {self.window_days}-day window, not {self.plan.horizon_weeks} weeks"
            )
        return self


class ChatSettings(Frozen):
    channel: str
    ignored: frozenset[str]


class PostgresSettings(Frozen):
    host: str
    port: PositiveInt
    name: str
    user: str
    password: str


class Settings(Frozen):
    backend: BackendSettings
    kafka: KafkaSettings
    relay: RelaySettings
    lake: LakeSettings
    spark: SparkSettings
    stream: StreamSettings
    mining: MiningSettings
    bandit: BanditSettings
    engagement: EngagementSettings
    chat: ChatSettings
    experiments_database: PostgresSettings
    sources: Sources


def load_document(config: Path) -> Any:
    return Box(yaml.safe_load(config.read_text(encoding="utf-8")), box_dots=False)


def lake_settings(document: Any) -> LakeSettings:
    return LakeSettings(
        **document.lake.to_dict(),
        access_key=secret("LAKE_ACCESS_KEY_ID"),
        secret_key=secret("LAKE_SECRET_ACCESS_KEY"),
    )


def backend_settings(document: Any) -> BackendSettings:
    return BackendSettings(url=document.backend.url, api_key=secret("UPLOAD_API_KEY"))


def load_backend_settings(config: Path) -> BackendSettings:
    return backend_settings(load_document(config))


def load_settings(config: Path) -> Settings:
    document = load_document(config)
    return Settings(
        backend=backend_settings(document),
        kafka=KafkaSettings(**document.kafka.to_dict()),
        relay=RelaySettings(**document.relay.to_dict()),
        lake=lake_settings(document),
        spark=SparkSettings(**document.spark.to_dict()),
        stream=StreamSettings(**document.stream.to_dict()),
        mining=MiningSettings(**document.mining.to_dict()),
        bandit=BanditSettings(**document.bandit.to_dict()),
        engagement=EngagementSettings(**document.engagement.to_dict()),
        chat=ChatSettings(channel=secret("TWITCH_CHANNEL").lower(), ignored=frozenset(document.chat.ignored)),
        experiments_database=PostgresSettings(
            **document.experiments_database.to_dict(), password=secret("EXPERIMENTS_DATABASE_PASSWORD")
        ),
        sources=load_sources(
            GithubSettings(**document.sources.github.to_dict(), token=secret("GITHUB_TOKEN")),
            UptimeSettings(**document.sources.uptime.to_dict()),
            TempoSettings(**document.sources.tempo.to_dict()),
            BlueskySettings(**document.sources.bluesky.to_dict()),
            frozenset(document.chat.ignored),
            document.sources.umami.games_website,
            document.sources.umami.itch_website,
            document.sources.umami.dropoff_since,
        ),
    )
