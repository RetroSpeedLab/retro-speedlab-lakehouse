from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CONVERTED = 1.0
EVEN = (0.5, 0.5)


@dataclass(frozen=True, slots=True)
class Experiment:
    key: str
    name: str
    hypothesis: str
    variations: tuple[str, ...]
    weights: tuple[float, ...]
    metrics: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Family:
    experiments: list[Experiment]
    units: DataFrame


def measured(rows: DataFrame, metrics: tuple[str, ...]) -> DataFrame:
    pairs = F.array(
        *(F.struct(F.lit(name).alias("metric"), F.col(name).cast("double").alias("value")) for name in metrics)
    )
    exploded = rows.select("experiment", "variation", "unit", "assigned_at", F.explode(pairs).alias("pair"))
    return exploded.select("experiment", "variation", "unit", "assigned_at", "pair.metric", "pair.value")


def counted(cells: DataFrame) -> DataFrame:
    numbered = cells.where(F.col("units") >= 1).withColumn(
        "number", F.explode(F.sequence(F.lit(1), F.col("units").cast("int")))
    )
    return numbered.select(
        "experiment",
        "variation",
        F.concat_ws(":", "experiment", "variation", F.date_format("hour", "yyyyMMddHH"), "number").alias("unit"),
        F.col("hour").alias("assigned_at"),
        "metric",
        F.when(F.col("number") <= F.col("hits"), F.lit(CONVERTED)).alias("value"),
    )
