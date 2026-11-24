import json

import httpx
import respx
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.batch.lab_runs import forget_deleted_lab_runs, listed_lab_runs, without_deleted_lab_runs
from retro_speedlab_lakehouse.lake.catalog import LAB_RUNS
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings

LISTED = ("blinkwisp", "2026-11-24T0735")
DELETED = ("blinkwisp", "2026-11-22T0735")
OLDER_MAJOR = ("sumspire", "2026-11-10T0735")
RUNS = (LISTED, DELETED, OLDER_MAJOR)


@respx.mock
def test_a_lab_run_the_backend_deleted_leaves_the_lake_and_its_traces_while_an_older_major_stays(
    spark: SparkSession, settings: Settings
) -> None:
    lab_runs = spark.createDataFrame([(*run, "notes") for run in RUNS], "game string, stamp string, outcome string")
    lab_runs.write.format("delta").save(LAB_RUNS.location(settings.lake))
    respx.get(f"https://api.test/lab-runs/{DELETED[0]}/{DELETED[1]}").respond(404)
    respx.get(f"https://api.test/lab-runs/{OLDER_MAJOR[0]}/{OLDER_MAJOR[1]}").respond(200, json={})
    listed = listed_lab_runs([json.dumps([{"game": LISTED[0], "stamp": LISTED[1]}])])

    with httpx.Client(base_url="https://api.test") as client:
        forgotten = forget_deleted_lab_runs(spark, settings.lake, Gate("test"), client, listed)

    kept = {(row["game"], row["stamp"]) for row in read(spark, LAB_RUNS, settings.lake).collect()}
    costs = spark.createDataFrame([(stamp, 1.0) for _, stamp in RUNS], "stamp string, cost_usd double")
    traced = {row["stamp"] for row in without_deleted_lab_runs(spark, settings.lake, costs).collect()}
    assert forgotten == 1
    assert kept == {LISTED, OLDER_MAJOR}
    assert traced == {LISTED[1], OLDER_MAJOR[1]}
