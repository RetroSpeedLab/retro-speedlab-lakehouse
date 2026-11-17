from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SEMVER = r"^\d+\.\d+\.\d+$"
CURRENT = "current_version"


def major(version: Column) -> Column:
    return F.substring_index(version, ".", 1)


def same_major(version: Column, current: Column) -> Column:
    semantic = version.rlike(SEMVER) & current.rlike(SEMVER)
    failure = F.raise_error(F.format_string("Version %s or %s is no semantic version", version, current))
    return F.when(semantic, major(version) == major(current)).otherwise(failure)


def current_major(rows: DataFrame, version_column: str, games: DataFrame) -> DataFrame:
    current = games.select(F.col("slug").alias("game"), F.col("version").alias(CURRENT))
    joined = rows.join(current, "game")
    return joined.where(same_major(F.col(version_column), F.col(CURRENT))).drop(CURRENT)
