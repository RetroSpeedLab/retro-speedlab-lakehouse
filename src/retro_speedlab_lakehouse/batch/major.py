from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SEMVER = r"^\d+\.\d+\.\d+$"
CURRENT = "current_version"


def semantic(version: Column) -> Column:
    failure = F.raise_error(F.format_string("Version %s is no semantic version", version))
    return F.when(version.rlike(SEMVER), version).otherwise(failure)


def major(version: Column) -> Column:
    return F.substring_index(semantic(version), ".", 1)


def board_line(version: Column) -> Column:
    return F.substring_index(semantic(version), ".", 2)


def current(rows: DataFrame, version_column: str, games: DataFrame, part: Callable[[Column], Column]) -> DataFrame:
    versions = games.select(F.col("slug").alias("game"), F.col("version").alias(CURRENT))
    joined = rows.join(versions, "game")
    return joined.where(part(F.col(version_column)) == part(F.col(CURRENT))).drop(CURRENT)


def current_major(rows: DataFrame, version_column: str, games: DataFrame) -> DataFrame:
    return current(rows, version_column, games, major)


def current_line(rows: DataFrame, version_column: str, games: DataFrame) -> DataFrame:
    return current(rows, version_column, games, board_line)
