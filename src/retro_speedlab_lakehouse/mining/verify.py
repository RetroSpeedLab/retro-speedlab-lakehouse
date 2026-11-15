import json
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from functools import partial, reduce
from itertools import groupby
from pathlib import Path
from tempfile import TemporaryDirectory

from loguru import logger
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retro_speedlab_lakehouse.lake.catalog import EVENTS, TRACE_PIECES, VERIFIER_REPLAYS, VERIFIER_SPLITS
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import merge
from retro_speedlab_lakehouse.mining.gold import latest_pieces
from retro_speedlab_lakehouse.mining.headroom import ensure_headroom
from retro_speedlab_lakehouse.mining.replay import clocked_by, verified_by
from retro_speedlab_lakehouse.mining.served import ServedGame, served_game
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.stream.rows import HIGHSCORES_TOPIC, field, newest

HUMAN = "human"
AI = "ai"
AI_NAME = "Retro Speedlab AI"
VERIFIED = "verified"
REJECTED = "rejected"
UNSERVED = "unserved"
NO_INPUTS = "no_inputs"
ENTRY_KEYS = ["side", "entry"]
SPLIT = StructType(
    [
        StructField("split", IntegerType(), False),
        StructField("level_index", IntegerType(), False),
        StructField("entered_at", LongType(), False),
        StructField("frames", LongType(), False),
    ]
)
REPLAY = StructType(
    [
        StructField("side", StringType(), False),
        StructField("entry", StringType(), False),
        StructField("game", StringType(), False),
        StructField("version", StringType(), False),
        StructField("name", StringType(), False),
        StructField("frames", LongType(), False),
        StructField("served_version", StringType(), False),
        StructField("status", StringType(), False),
        StructField("won_at", LongType(), True),
        StructField("splits", ArrayType(SPLIT, False), False),
        StructField("replayed_at", TimestampType(), False),
    ]
)
Split = tuple[int, int, int, int]


def human_entries(events: DataFrame) -> DataFrame:
    scores = events.where(F.col("topic") == HIGHSCORES_TOPIC).select(
        field("id").alias("id"),
        field("game").alias("game"),
        field("version").alias("version"),
        field("name").alias("name"),
        field("frames").cast("long").alias("frames"),
        F.unbase64(field("inputs")).alias("inputs"),
        "event_id",
    )
    return newest(scores, ["id", "frames"]).select(
        F.lit(HUMAN).alias("side"),
        F.concat_ws(":", "id", "frames").alias("entry"),
        "game",
        "version",
        "name",
        "frames",
        "inputs",
    )


def ai_entries(pieces: DataFrame) -> DataFrame:
    runs = (
        latest_pieces(pieces)
        .groupBy("game", "record", "version")
        .agg(
            F.sort_array(F.collect_list(F.struct("segment", "inputs"))).alias("parts"), F.sum("frames").alias("frames")
        )
    )
    inputs = F.aggregate("parts", F.lit(bytearray()), lambda joined, part: F.concat(joined, part["inputs"]))
    return runs.select(
        F.lit(AI).alias("side"),
        F.col("record").alias("entry"),
        "game",
        "version",
        F.lit(AI_NAME).alias("name"),
        "frames",
        inputs.alias("inputs"),
    )


def unreplayed(spark: SparkSession, settings: Settings, humans: DataFrame) -> DataFrame:
    if not exists(spark, VERIFIER_REPLAYS, settings.lake):
        return humans
    done = read(spark, VERIFIER_REPLAYS, settings.lake).where(F.col("status") != UNSERVED).select(*ENTRY_KEYS)
    return humans.join(done, ENTRY_KEYS, "left_anti")


def entries(spark: SparkSession, settings: Settings) -> list[DataFrame]:
    found: list[DataFrame] = []
    if exists(spark, EVENTS, settings.lake):
        found.append(unreplayed(spark, settings, human_entries(read(spark, EVENTS, settings.lake))))
    if exists(spark, TRACE_PIECES, settings.lake):
        found.append(ai_entries(read(spark, TRACE_PIECES, settings.lake)))
    return found


def splits_of(levels: list[list[int]], won_at: int) -> list[Split]:
    ends = [entered for entered, _ in levels[1:]] + [won_at]
    return [
        (split, level, entered, end - entered)
        for split, ((entered, level), end) in enumerate(zip(levels, ends, strict=True))
    ]


def replayed(speedlab: Path, served: ServedGame, entry: Row) -> tuple[str, int | None, list[Split]]:
    manifest = served.manifest
    if entry["inputs"] is None:
        return NO_INPUTS, None, []
    if entry["side"] == HUMAN and entry["version"] != manifest.version:
        return UNSERVED, None, []
    inputs = bytes(entry["inputs"])
    verdict = json.loads(verified_by(served.app, manifest.verifier, manifest.cartridge, inputs))
    if verdict["cartridge"] != manifest.cartridge or verdict["version"] != manifest.version:
        raise RuntimeError(f"The verifier played {verdict['cartridge']} {verdict['version']}, not {manifest.cartridge}")
    won_at = verdict["won_at"]
    if won_at is None or won_at != entry["frames"] or len(inputs) != entry["frames"]:
        return REJECTED, won_at, []
    clock = json.loads(clocked_by(speedlab, served.app, manifest.cartridge, inputs))
    if clock["won_at"] != won_at:
        raise RuntimeError(f"The level clock of {entry['entry']} ends on frame {clock['won_at']}, not {won_at}")
    return VERIFIED, won_at, splits_of(clock["levels"], won_at)


def replayed_partition(speedlab: Path, games_url: str, rows: Iterable[Row]) -> Iterator[tuple[object, ...]]:
    for game, grouped in groupby(sorted(rows, key=lambda row: (row["game"], row["entry"])), lambda row: row["game"]):
        with TemporaryDirectory() as directory:
            served = served_game(games_url, game, Path(directory))
            for entry in grouped:
                status, won_at, splits = replayed(speedlab, served, entry)
                head = (entry["side"], entry["entry"], game, entry["version"], entry["name"], entry["frames"])
                yield *head, served.manifest.version, status, won_at, splits, datetime.now(UTC)


def verify_replays(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    ensure_headroom(settings.mining.min_available_mb)
    found = entries(spark, settings)
    if not found:
        return 0
    fresh = reduce(DataFrame.unionByName, found)
    replay = partial(replayed_partition, settings.mining.speedlab, settings.mining.games_url)
    results = fresh.repartition("game").rdd.mapPartitions(replay).collect()
    if not results:
        logger.info("Every run is replayed through its served verifier already")
        return 0
    replays = spark.createDataFrame(results, REPLAY)
    merge(VERIFIER_REPLAYS, settings.lake, replays.drop("splits"), gate)
    splits = replays.select(*ENTRY_KEYS, "game", "version", F.explode("splits").alias("part")).select(
        *ENTRY_KEYS, "game", "version", "part.*"
    )
    merge(VERIFIER_SPLITS, settings.lake, splits, gate)
    logger.info(f"Replayed {len(results)} runs through their games' served verifiers")
    return len(results)
