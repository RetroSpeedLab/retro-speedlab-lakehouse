import json
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus

import httpx
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from retro_speedlab_lakehouse.batch.lab_runs import forget_deleted_lab_runs, listed_lab_runs
from retro_speedlab_lakehouse.lake.catalog import API
from retro_speedlab_lakehouse.lake.write import append
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings

TIMEOUT_SECONDS = 60.0
GAMES = "/games"
HOURLY_GAME_ENDPOINTS = ("/games/{slug}/lab-runs",)
NIGHTLY_GAME_ENDPOINTS = ("/games/{slug}/speedrun", "/games/{slug}/knowledge", "/games/{slug}/lab-runs/insight")
KEPT_STATUSES = (httpx.codes.OK, httpx.codes.NOT_FOUND, httpx.codes.SERVICE_UNAVAILABLE)
SNAPSHOT = StructType(
    [
        StructField("endpoint", StringType(), False),
        StructField("slug", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("body", StringType(), False),
        StructField("fetched_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class Snapshot:
    endpoint: str
    slug: str | None
    status: int
    body: str
    fetched_at: datetime


@retry(
    retry=retry_if_exception_type(httpx.TransportError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2),
    reraise=True,
)
def snapshot(client: httpx.Client, endpoint: str, slug: str | None) -> Snapshot:
    response = client.get(endpoint if slug is None else endpoint.format(slug=slug))
    if response.status_code not in KEPT_STATUSES:
        response.raise_for_status()
    return Snapshot(endpoint, slug, response.status_code, response.text, datetime.now(UTC))


def snapshots(client: httpx.Client, endpoints: tuple[str, ...]) -> list[Snapshot]:
    games = snapshot(client, GAMES, None)
    slugs = [game["slug"] for game in json.loads(games.body)]
    return [games] + [snapshot(client, endpoint, slug) for slug in slugs for endpoint in endpoints]


def lab_run_bodies(taken: list[Snapshot]) -> list[str]:
    return [item.body for item in taken if item.endpoint == HOURLY_GAME_ENDPOINTS[0] and item.status == HTTPStatus.OK]


def ingest_api(spark: SparkSession, settings: Settings, gate: Gate, endpoints: tuple[str, ...]) -> int:
    with httpx.Client(base_url=settings.backend.url, timeout=TIMEOUT_SECONDS) as client:
        taken = snapshots(client, endpoints)
        forget_deleted_lab_runs(spark, settings.lake, gate, client, listed_lab_runs(lab_run_bodies(taken)))
    rows = spark.createDataFrame(
        [(item.endpoint, item.slug, item.status, item.body, item.fetched_at) for item in taken], SNAPSHOT
    ).withColumn("date", F.to_date("fetched_at"))
    append(API, settings.lake, rows, gate)
    logger.info(f"Kept {len(taken)} snapshots of the backend's API")
    return len(taken)


def ingest_api_hourly(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return ingest_api(spark, settings, gate, HOURLY_GAME_ENDPOINTS)


def ingest_api_nightly(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    return ingest_api(spark, settings, gate, HOURLY_GAME_ENDPOINTS + NIGHTLY_GAME_ENDPOINTS)
