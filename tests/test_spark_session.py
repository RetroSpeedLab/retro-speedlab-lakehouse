from pathlib import Path

from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.spark.session import lake_options
from retro_speedlab_lakehouse.stream import rows
from retro_speedlab_lakehouse.stream.job import LIVE_QUERY, SCHEDULER_POOL, pooled
from tests.conftest import lake_settings


def test_a_stalled_upload_to_the_lake_is_given_up_and_retried_after_two_minutes(tmp_path: Path) -> None:
    options = lake_options(lake_settings(tmp_path))

    assert options["spark.hadoop.fs.s3a.connection.part.upload.timeout"] == "2m"


def test_a_dead_connection_to_the_lake_is_given_up_and_retried_after_a_minute(tmp_path: Path) -> None:
    options = lake_options(lake_settings(tmp_path))

    assert options["spark.hadoop.fs.s3a.connection.timeout"] == "60s"


def test_each_streaming_query_runs_its_batches_in_its_own_fair_pool(spark: SparkSession) -> None:
    events = spark.createDataFrame([], rows.KAFKA_ROW)

    pooled(LIVE_QUERY, events)

    assert spark.conf.get("spark.scheduler.mode") == "FAIR"
    assert spark.sparkContext.getLocalProperty(SCHEDULER_POOL) == LIVE_QUERY
