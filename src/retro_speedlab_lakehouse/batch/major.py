from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SEMVER = r"^\d+\.\d+\.\d+$"
CURRENT = "current_version"


def major(version: Column) -> Column:
    return F.substring_index(version, ".", 1)


def board_line(version: Column) -> Column:
    return F.substring_index(version, ".", 2)


def same(part: Callable[[Column], Column], version: Column, current: Column) -> Column:
    semantic = version.rlike(SEMVER) & current.rlike(SEMVER)
    failure = F.raise_error(F.format_string("Version %s or %s is no semantic version", version, current))
    return F.when(semantic, part(version) == part(current)).otherwise(failure)


def current_part(rows: DataFrame, version_column: str, games: DataFrame, part: Callable[[Column], Column]) -> DataFrame:
    versions = games.select(F.col("slug").alias("game"), F.col("version").alias(CURRENT))
    joined = rows.join(versions, "game")
    return joined.where(same(part, F.col(version_column), F.col(CURRENT))).drop(CURRENT)


def current_major(rows: DataFrame, version_column: str, games: DataFrame) -> DataFrame:
    return current_part(rows, version_column, games, major)


def current_line(rows: DataFrame, version_column: str, games: DataFrame) -> DataFrame:
    return current_part(rows, version_column, games, board_line)
