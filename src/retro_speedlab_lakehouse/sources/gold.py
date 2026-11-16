from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import GAMES
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import GAME_PLAYS, SILVER_NUMBERS, SILVER_PLAYS, SOURCE_NUMBERS

FRESH = timedelta(days=2)


def count_of(column: str) -> F.Column:
    return F.coalesce(F.element_at(F.col(column), F.col("game")), F.lit(0)).cast("long").alias(column)


def build_numbers_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SILVER_NUMBERS, lake):
        return 0
    fresh = read(spark, SILVER_NUMBERS, lake).where(F.col("measured_at") >= F.lit(now - FRESH))
    numbers = fresh.withColumn("computed_at", F.lit(now))
    replace(SOURCE_NUMBERS, lake, numbers, gate)
    return numbers.count()


def build_plays_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SILVER_PLAYS, lake) or not exists(spark, GAMES, lake):
        return 0
    games = read(spark, GAMES, lake).select(F.col("slug").alias("game"))
    plays = (
        games.crossJoin(read(spark, SILVER_PLAYS, lake))
        .select("game", "mode", "window", count_of("started"), count_of("won"), "measured_at")
        .withColumn("computed_at", F.lit(now))
    )
    replace(GAME_PLAYS, lake, plays, gate)
    return plays.count()


def build_source_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    return build_numbers_gold(spark, lake, gate, now) + build_plays_gold(spark, lake, gate, now)
