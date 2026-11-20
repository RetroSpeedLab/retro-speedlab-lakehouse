from pathlib import Path

from retro_speedlab_lakehouse.spark.session import lake_options
from tests.conftest import lake_settings


def test_a_stalled_upload_to_the_lake_is_given_up_and_retried_after_two_minutes(tmp_path: Path) -> None:
    options = lake_options(lake_settings(tmp_path))

    assert options["spark.hadoop.fs.s3a.connection.part.upload.timeout"] == "2m"


def test_a_dead_connection_to_the_lake_is_given_up_and_retried_after_a_minute(tmp_path: Path) -> None:
    options = lake_options(lake_settings(tmp_path))

    assert options["spark.hadoop.fs.s3a.connection.timeout"] == "60s"
