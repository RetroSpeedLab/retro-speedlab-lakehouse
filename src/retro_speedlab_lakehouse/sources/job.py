from datetime import datetime

import httpx
from loguru import logger
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.bronze import ingest
from retro_speedlab_lakehouse.sources.gold import build_source_gold
from retro_speedlab_lakehouse.sources.record import Source
from retro_speedlab_lakehouse.sources.silver import build_source_silver
from retro_speedlab_lakehouse.sources.status import build_sources_status


class SourcesFailed(RuntimeError):
    pass


def ingest_sources(spark: SparkSession, lake: LakeSettings, gate: Gate, sources: list[Source], now: datetime) -> int:
    loaded = 0
    failed: list[str] = []
    for source in sources:
        try:
            loaded += ingest(spark, lake, gate, source)
        except (httpx.HTTPError, KeyError, IndexError, ValueError) as error:
            logger.error(f"{source.name} could not be loaded: {error!r}")
            failed.append(source.name)
    built = build_source_silver(spark, lake, gate) + build_source_gold(spark, lake, gate, now)
    written = loaded + built + build_sources_status(spark, lake, gate)
    if failed:
        raise SourcesFailed(f"Kept the other sources; these failed: {', '.join(failed)}")
    return written
