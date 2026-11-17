from collections.abc import Sequence

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

from retro_speedlab_lakehouse.batch.major import current_major

GAMES = [("comet_courier", "2.1.0"), ("blinkwisp", "1.0.3")]


def kept(spark: SparkSession, runs: Sequence[tuple[str, str | None]]) -> DataFrame:
    games = spark.createDataFrame(GAMES, "slug string, version string")
    rows = spark.createDataFrame(list(runs), "game string, cartridge string")
    return current_major(rows, "cartridge", games)


def test_only_rows_of_the_current_major_stay(spark: SparkSession) -> None:
    runs = [("comet_courier", "2.0.0"), ("comet_courier", "1.9.9"), ("blinkwisp", "1.4.0"), ("gone", "no")]
    rows = kept(spark, runs).orderBy("game").collect()
    assert [(row["game"], row["cartridge"]) for row in rows] == [("blinkwisp", "1.4.0"), ("comet_courier", "2.0.0")]


@pytest.mark.parametrize("version", ["2.0", None])
def test_a_version_that_is_no_semver_fails(spark: SparkSession, version: str | None) -> None:
    with pytest.raises(PySparkException, match="no semantic version"):
        kept(spark, [("comet_courier", version)]).collect()
