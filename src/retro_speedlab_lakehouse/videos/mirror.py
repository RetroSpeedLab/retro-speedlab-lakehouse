import hashlib
from dataclasses import astuple, dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path

import httpx
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.lake.catalog import VIDEOS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import append
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.http import get_json

SCREENS = ("movie", "compare", "run")
TIMEOUT_SECONDS = 300.0
CHUNK = 1024 * 1024
FILES = "videos"
MP4 = ".mp4"
PARTIAL = ".partial"
VIDEO = StructType(
    [
        StructField("game", StringType(), False),
        StructField("name", StringType(), False),
        StructField("version", StringType(), False),
        StructField("bytes", LongType(), False),
        StructField("sha256", StringType(), False),
        StructField("path", StringType(), False),
        StructField("mirrored_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class Mirrored:
    game: str
    name: str
    version: str
    bytes: int
    sha256: str
    path: str
    mirrored_at: datetime


def video_names(session: httpx.Client, slug: str) -> list[str]:
    response = session.get(f"/games/{slug}/speedrun")
    if response.status_code == HTTPStatus.NOT_FOUND:
        return list(SCREENS)
    response.raise_for_status()
    levels = len(response.json()["levels"])
    return [*SCREENS, *(f"levels/{number}" for number in range(1, levels + 1))]


def saved(response: httpx.Response, target: Path) -> tuple[int, str]:
    target.parent.mkdir(parents=True, exist_ok=True)
    staged = target.with_suffix(PARTIAL)
    digest = hashlib.sha256()
    size = 0
    with staged.open("wb") as file:
        for chunk in response.iter_bytes(CHUNK):
            file.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    staged.replace(target)
    return size, digest.hexdigest()


def mirrored(session: httpx.Client, root: Path, slug: str, name: str, known: str | None) -> Mirrored | None:
    with session.stream("GET", f"/games/{slug}/{name}{MP4}") as response:
        if response.status_code == HTTPStatus.NOT_FOUND:
            return None
        response.raise_for_status()
        version = response.headers["etag"].strip('"')
        if version == known:
            return None
        target = root / slug / f"{name}{MP4}"
        size, sha256 = saved(response, target)
    return Mirrored(slug, name, version, size, sha256, str(target), datetime.now(UTC))


def known_versions(spark: SparkSession, settings: Settings) -> dict[tuple[str, str], str]:
    if not exists(spark, VIDEOS, settings.lake):
        return {}
    newest = (
        read(spark, VIDEOS, settings.lake)
        .groupBy("game", "name")
        .agg(F.max_by("version", "mirrored_at").alias("version"))
    )
    return {(row["game"], row["name"]): row["version"] for row in newest.collect()}


def mirror_videos(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    known = known_versions(spark, settings)
    root = Path(settings.lake.bronze) / FILES
    copied: list[Mirrored] = []
    with httpx.Client(base_url=settings.backend.url, timeout=TIMEOUT_SECONDS) as session:
        for game in get_json(session, "/games", {}):
            slug = game["slug"]
            for name in video_names(session, slug):
                version = known[(slug, name)] if (slug, name) in known else None
                video = mirrored(session, root, slug, name, version)
                if video is not None:
                    copied.append(video)
    rows = spark.createDataFrame([astuple(video) for video in copied], VIDEO).withColumn(
        "date", F.to_date("mirrored_at")
    )
    append(VIDEOS, settings.lake, rows, gate)
    logger.info(f"Mirrored {len(copied)} new videos, {sum(video.bytes for video in copied)} bytes")
    return len(copied)
