from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retro_speedlab_lakehouse.batch.api import GAMES as GAMES_ENDPOINT
from retro_speedlab_lakehouse.batch.api import HOURLY_GAME_ENDPOINTS
from retro_speedlab_lakehouse.lake.catalog import API, GAME_RUNS, GAMES, HISTORY, KNOWLEDGE, LAB_PROGRESS, SPLITS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import insert_new, replace_counted
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings

GAME = StructType(
    [
        StructField("number", IntegerType(), False),
        StructField("slug", StringType(), False),
        StructField("title", StringType(), False),
        StructField("version", StringType(), False),
        StructField("genre", StringType(), False),
        StructField("pitch", StringType(), False),
    ]
)
PROGRESS = StructType(
    [
        StructField("game", StringType(), False),
        StructField("stamp", StringType(), False),
        StructField(
            "progress",
            StructType(
                [
                    StructField("progressed", BooleanType(), False),
                    StructField("hours", DoubleType(), True),
                    StructField("seconds", DoubleType(), False),
                ]
            ),
            True,
        ),
    ]
)
SPLIT = StructType(
    [
        StructField("level", StringType(), False),
        StructField("seconds", DoubleType(), False),
        StructField("level_seconds", DoubleType(), False),
        StructField("first_seconds", DoubleType(), False),
    ]
)
LEVEL = StructType([StructField("name", StringType(), False)])
POINT = StructType([StructField("at", TimestampType(), False), StructField("seconds", DoubleType(), False)])
NAMED = StructType([StructField("name", StringType(), True), StructField("title", StringType(), True)])
KNOWLEDGE_KINDS = ("mechanics", "enemies", "powerups", "levels", "ram")


def latest_snapshots(api: DataFrame, endpoint: str) -> DataFrame:
    kept = api.where((F.col("endpoint") == endpoint) & (F.col("status") == 200))
    newest_first = Window.partitionBy("slug").orderBy(F.col("fetched_at").desc())
    return kept.withColumn("recency", F.row_number().over(newest_first)).where("recency = 1").drop("recency")


def games(api: DataFrame) -> DataFrame:
    snapshot = latest_snapshots(api, GAMES_ENDPOINT)
    entries = snapshot.select("fetched_at", F.explode(F.from_json("body", ArrayType(GAME))).alias("game"))
    return entries.select("game.*", "fetched_at")


def lab_progress(api: DataFrame) -> DataFrame:
    snapshots = latest_snapshots(api, HOURLY_GAME_ENDPOINTS[0])
    runs = snapshots.select(F.explode(F.from_json("body", ArrayType(PROGRESS))).alias("run"))
    return runs.where("run.progress IS NOT NULL").select(
        "run.game",
        "run.stamp",
        F.col("run.progress.progressed").alias("progressed"),
        F.col("run.progress.hours").alias("hours"),
        F.col("run.progress.seconds").alias("seconds"),
    )


def payload(runs: DataFrame, path: str, schema: ArrayType) -> DataFrame:
    return runs.select("game", F.explode(F.from_json(F.get_json_object("payload", f"$.{path}"), schema)).alias("item"))


def splits(runs: DataFrame) -> DataFrame:
    levels = payload(runs, "levels", ArrayType(LEVEL)).select("game", F.col("item.name").alias("level"))
    rows = payload(runs, "splits", ArrayType(SPLIT)).select("game", "item.*")
    ordered = rows.join(levels, ["game", "level"]).withColumn(
        "position", F.row_number().over(Window.partitionBy("game").orderBy("seconds"))
    )
    frame_rates = runs.select(
        "game", "version", F.get_json_object("payload", "$.frame_rate").cast("double").alias("frame_rate")
    )
    return ordered.join(frame_rates, "game").withColumn(
        "frames", F.round(F.col("level_seconds") * F.col("frame_rate")).cast("long")
    )


def history(runs: DataFrame) -> DataFrame:
    points = payload(runs, "history", ArrayType(POINT)).select("game", "item.at", "item.seconds")
    return points.join(runs.select("game", "version"), "game").distinct()


def knowledge(runs: DataFrame) -> DataFrame:
    kinds = [
        payload(runs, f"knowledge.{kind}", ArrayType(NAMED)).select(
            "game",
            F.lit(kind).alias("kind"),
            F.coalesce("item.name", "item.title").alias("name"),
        )
        for kind in KNOWLEDGE_KINDS
    ]
    union = kinds[0]
    for kind in kinds[1:]:
        union = union.unionByName(kind)
    return union.where("name IS NOT NULL").distinct()


def build_silver(spark: SparkSession, lake: LakeSettings, gate: Gate) -> int:
    written = 0
    if exists(spark, API, lake):
        api = read(spark, API, lake)
        written += sum(
            replace_counted(table, lake, rows, gate)
            for table, rows in ((GAMES, games(api)), (LAB_PROGRESS, lab_progress(api)))
        )
    if exists(spark, GAME_RUNS, lake):
        runs = read(spark, GAME_RUNS, lake)
        written += sum(
            replace_counted(table, lake, rows, gate)
            for table, rows in ((SPLITS, splits(runs)), (KNOWLEDGE, knowledge(runs)))
        )
        points = history(runs)
        insert_new(HISTORY, lake, points, gate)
        written += points.count()
    return written
