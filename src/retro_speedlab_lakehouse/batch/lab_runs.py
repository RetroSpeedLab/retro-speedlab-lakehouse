import json
import operator
from functools import reduce
from http import HTTPStatus

import httpx
from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import DELETED_LAB_RUNS, LAB_RUNS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import insert_new
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings

LAB_RUN = "/lab-runs/{game}/{stamp}"
RUN_KEYS = ("game", "stamp")
LabRunKey = tuple[str, str]


def listed_lab_runs(bodies: list[str]) -> set[LabRunKey]:
    return {(run["game"], run["stamp"]) for body in bodies for run in json.loads(body)}


def deleted_at_the_backend(client: httpx.Client, runs: list[LabRunKey]) -> list[LabRunKey]:
    return [
        (game, stamp)
        for game, stamp in runs
        if client.get(LAB_RUN.format(game=game, stamp=stamp)).status_code == HTTPStatus.NOT_FOUND
    ]


def forget_deleted_lab_runs(
    spark: SparkSession, lake: LakeSettings, gate: Gate, client: httpx.Client, listed: set[LabRunKey]
) -> int:
    if not exists(spark, LAB_RUNS, lake):
        return 0
    kept = {(row["game"], row["stamp"]) for row in read(spark, LAB_RUNS, lake).select(*RUN_KEYS).collect()}
    deleted = deleted_at_the_backend(client, sorted(kept - listed))
    if not deleted:
        return 0
    forgotten = spark.createDataFrame(deleted, "game string, stamp string")
    insert_new(DELETED_LAB_RUNS, lake, forgotten.withColumn("deleted_at", F.current_timestamp()), gate)
    matches = [(F.col("game") == game) & (F.col("stamp") == stamp) for game, stamp in deleted]
    DeltaTable.forPath(spark, LAB_RUNS.location(lake)).delete(reduce(operator.or_, matches))
    logger.info(f"Forgot {len(deleted)} lab runs the backend deleted: {deleted}")
    return len(deleted)


def without_deleted_lab_runs(spark: SparkSession, lake: LakeSettings, rows: DataFrame) -> DataFrame:
    if not exists(spark, DELETED_LAB_RUNS, lake):
        return rows
    keys = [key for key in RUN_KEYS if key in rows.columns]
    return rows.join(read(spark, DELETED_LAB_RUNS, lake).select(*keys), keys, "left_anti")
