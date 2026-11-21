from datetime import datetime, timedelta

from loguru import logger
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.http import client
from retro_speedlab_lakehouse.sources.settings import AimSettings
from retro_speedlab_lakehouse.tracking.aim import fetched_run
from retro_speedlab_lakehouse.tracking.bronze import due_runs, write_bronze
from retro_speedlab_lakehouse.tracking.gold import build_tracking_gold
from retro_speedlab_lakehouse.tracking.silver import build_tracking_silver


def load_tracking(spark: SparkSession, lake: LakeSettings, gate: Gate, aim: AimSettings, now: datetime) -> int:
    with client(aim.url, {}) as session:
        due = due_runs(spark, lake, session, now, timedelta(hours=aim.open_hours))
        fetched = [fetched_run(session, run) for run in due]
    points = [point for _, run_points in fetched for point in run_points]
    logger.info(f"Aim: {len(fetched)} runs with {len(points)} metric points to load")
    if not fetched:
        return 0
    write_bronze(spark, lake, gate, [run for run, _ in fetched], points, now)
    build_tracking_silver(spark, lake, gate, due)
    return len(points) + build_tracking_gold(spark, lake, gate, now)
