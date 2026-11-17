from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.lake.catalog import GAMES, SPLIT_COMPARISON, Table
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace_counted
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.sources.catalog import (
    GAME_PLAYS,
    HUMAN_DROPOFF,
    ITCH_DAILY,
    ITCH_PLAYS,
    SILVER_DROPOFF,
    SILVER_ITCH_DAILY,
    SILVER_ITCH_PLAYS,
    SILVER_NUMBERS,
    SILVER_PLAYS,
    SOURCE_NUMBERS,
)

FRESH = timedelta(days=2)
PLAY_DIMENSIONS = ("mode", "window")
PLAY_COUNTS = ("started", "won")
ITCH_COUNTS = ("views", "campaign", "endless", "cleared", "won")
NO_SPLITS = "game string, level_index int, level string, human_median_frames long, ai_frames long, humans long"


def count_of(column: str) -> F.Column:
    return F.coalesce(F.element_at(F.col(column), F.col("game")), F.lit(0)).cast("long").alias(column)


def build_numbers_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SILVER_NUMBERS, lake):
        return 0
    fresh = read(spark, SILVER_NUMBERS, lake).where(F.col("measured_at") >= F.lit(now - FRESH))
    numbers = fresh.withColumn("computed_at", F.lit(now))
    return replace_counted(SOURCE_NUMBERS, lake, numbers, gate)


def build_per_game(
    spark: SparkSession,
    lake: LakeSettings,
    gate: Gate,
    now: datetime,
    tables: tuple[Table, Table],
    dimensions: tuple[str, ...],
    counts: tuple[str, ...],
) -> int:
    silver, gold = tables
    if not exists(spark, silver, lake) or not exists(spark, GAMES, lake):
        return 0
    games = read(spark, GAMES, lake).select(F.col("slug").alias("game"))
    rows = (
        games.crossJoin(read(spark, silver, lake))
        .select("game", *dimensions, *(count_of(column) for column in counts), "measured_at")
        .withColumn("computed_at", F.lit(now))
    )
    return replace_counted(gold, lake, rows, gate)


def build_plays_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    plays = ((SILVER_PLAYS, GAME_PLAYS), (SILVER_ITCH_PLAYS, ITCH_PLAYS))
    return sum(build_per_game(spark, lake, gate, now, tables, PLAY_DIMENSIONS, PLAY_COUNTS) for tables in plays)


def build_itch_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    return build_per_game(spark, lake, gate, now, (SILVER_ITCH_DAILY, ITCH_DAILY), ("date",), ITCH_COUNTS)


def ended_levels(dropoff: DataFrame) -> DataFrame:
    ended = dropoff.select("game", F.explode("ended").alias("level", "ended"))
    counted = ended.groupBy("game", F.col("level").cast("int").alias("level_index")).agg(
        F.sum("ended").cast("long").alias("ended")
    )
    deepest = counted.groupBy("game").agg(F.max("level_index").alias("deepest"))
    levels = deepest.select("game", F.explode(F.sequence(F.lit(0), "deepest")).alias("level_index"))
    return levels.join(counted, ["game", "level_index"], "left").fillna(0, ["ended"])


def human_dropoff(dropoff: DataFrame, splits: DataFrame, now: datetime) -> DataFrame:
    deeper = Window.partitionBy("game").orderBy(F.col("level_index").desc())
    per_game = Window.partitionBy("game")
    reached = ended_levels(dropoff).join(dropoff.select("game", "won", "since"), "game")
    reached = reached.withColumn("reached", F.sum("ended").over(deeper) + F.col("won"))
    compared = splits.select("game", "level_index", "level", "human_median_frames", "ai_frames", "humans")
    return (
        reached.join(compared, ["game", "level_index"], "left")
        .fillna(0, ["humans"])
        .select(
            "game",
            "level_index",
            "level",
            "reached",
            "ended",
            (F.col("ended") / F.col("reached")).alias("ended_share"),
            "human_median_frames",
            "ai_frames",
            "humans",
            F.max("reached").over(per_game).alias("runs"),
            "won",
            "since",
            F.lit(now).alias("computed_at"),
        )
    )


def build_dropoff_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, SILVER_DROPOFF, lake):
        return 0
    found = exists(spark, SPLIT_COMPARISON, lake)
    splits = read(spark, SPLIT_COMPARISON, lake) if found else spark.createDataFrame([], NO_SPLITS)
    dropoff = human_dropoff(read(spark, SILVER_DROPOFF, lake), splits, now)
    return replace_counted(HUMAN_DROPOFF, lake, dropoff, gate)


def build_source_gold(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    built = (build_numbers_gold, build_plays_gold, build_itch_gold, build_dropoff_gold)
    return sum(build(spark, lake, gate, now) for build in built)
