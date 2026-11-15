import hashlib
import json
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from itertools import groupby
from pathlib import Path
from typing import Any

from loguru import logger
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retro_speedlab_lakehouse.lake.catalog import FRAME_TRACES, TRACE_PIECES
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import append, overwrite_games
from retro_speedlab_lakehouse.mining.headroom import ensure_headroom
from retro_speedlab_lakehouse.mining.replay import piece_traces, record_pieces
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings

RECORD = Path("cache", "tas", "record.msgpack")
SLUG = re.compile(r"[a-z0-9_]+")
PIECE_KEYS = ["game", "record", "segment"]
PIECE_FIELDS = ("segment", "entered", "reached", "won", "first_frame", "frames", "start", "end", "inputs")
PIECE = StructType(
    [
        StructField("game", StringType(), False),
        StructField("record", StringType(), False),
        StructField("version", StringType(), False),
        StructField("cartridge_version", StringType(), False),
        StructField("labels", ArrayType(StringType(), False), False),
        StructField("segment", IntegerType(), False),
        StructField("entered", StringType(), False),
        StructField("reached", StringType(), False),
        StructField("level", StringType(), True),
        StructField("won", BooleanType(), False),
        StructField("first_frame", LongType(), False),
        StructField("frames", LongType(), False),
        StructField("start", StringType(), False),
        StructField("end", StringType(), False),
        StructField("inputs", StringType(), False),
        StructField("mined_at", TimestampType(), False),
    ]
)
FRAME = StructType(
    [
        StructField("game", StringType(), False),
        StructField("record", StringType(), False),
        StructField("segment", IntegerType(), False),
        StructField("level", StringType(), True),
        StructField("frame", LongType(), False),
        StructField("area", IntegerType(), True),
        StructField("x", IntegerType(), True),
        StructField("y", IntegerType(), True),
        StructField("labels", MapType(StringType(), IntegerType(), True), False),
        StructField("identical", BooleanType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class FoundRecord:
    game: str
    path: Path
    digest: str


def found_record(path: Path) -> FoundRecord:
    game = path.parents[len(RECORD.parts) - 1].name
    if not SLUG.fullmatch(game):
        raise ValueError(f"The runner's working directory holds a record of {game!r}, which is no game slug")
    return FoundRecord(game, path, hashlib.sha256(path.read_bytes()).hexdigest())


def found_records(working: Path) -> list[FoundRecord]:
    return [found_record(path) for path in sorted(working.glob(f"*/{RECORD.as_posix()}"))]


def mined(spark: SparkSession, settings: Settings) -> set[tuple[str, str]]:
    if not exists(spark, TRACE_PIECES, settings.lake):
        return set()
    rows = read(spark, TRACE_PIECES, settings.lake).select("game", "record").distinct().collect()
    return {(row["game"], row["record"]) for row in rows}


def level_of(document: dict[str, Any], state: str) -> str | None:
    return document["levels"][state] if state in document["levels"] else None


def piece_rows(document: dict[str, Any], mined_at: datetime) -> list[tuple[object, ...]]:
    head = (document["game"], document["record"], document["version"], document["cartridge_version"])
    return [
        (*head, document["labels"], piece["segment"], piece["entered"], piece["reached"])
        + (level_of(document, piece["entered"]), piece["won"], piece["first_frame"], piece["frames"])
        + (piece["start"], piece["end"], piece["inputs"], mined_at)
        for piece in document["pieces"]
    ]


def frame_rows(piece: Row, trace: dict[str, Any]) -> Iterator[tuple[object, ...]]:
    for frame, area, x, y, *values in trace["frames"]:
        labels = dict(zip(trace["labels"], values, strict=True))
        located = (frame, area, x, y, labels, trace["identical"])
        yield piece["game"], piece["record"], piece["segment"], piece["level"], *located


def traced_partition(speedlab: Path, rows: Iterable[Row]) -> Iterator[tuple[object, ...]]:
    for game, grouped in groupby(sorted(rows, key=lambda row: (row["game"], row["segment"])), lambda row: row["game"]):
        pieces = {row["segment"]: row for row in grouped}
        lines = "".join(json.dumps({field: row[field] for field in PIECE_FIELDS}) + "\n" for row in pieces.values())
        for line in piece_traces(speedlab, game, lines.encode("utf-8")).splitlines():
            trace = json.loads(line)
            yield from frame_rows(pieces[trace["segment"]], trace)


def verdicts(traced: DataFrame) -> DataFrame:
    return traced.groupBy(*PIECE_KEYS).agg(F.first("identical").alias("identical"), F.count("*").alias("traced"))


def mine_records(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    ensure_headroom(settings.mining.min_available_mb)
    known = mined(spark, settings)
    fresh = [record for record in found_records(settings.mining.working) if (record.game, record.digest) not in known]
    if not fresh:
        logger.info("Every record in the runner's working directory is mined already")
        return 0
    mined_at = datetime.now(UTC)
    documents = [json.loads(record_pieces(settings.mining.speedlab, found.game, found.path)) for found in fresh]
    pieces = spark.createDataFrame([row for document in documents for row in piece_rows(document, mined_at)], PIECE)
    games = [found.game for found in fresh]
    trace = partial(traced_partition, settings.mining.speedlab)
    replayed = pieces.repartition("game", "segment").rdd.mapPartitions(trace)
    overwrite_games(FRAME_TRACES, settings.lake, spark.createDataFrame(replayed, FRAME), gate, games)
    traced = read(spark, FRAME_TRACES, settings.lake).where(F.col("game").isin(games))
    kept = pieces.drop("start", "end").withColumn("inputs", F.unbase64("inputs")).join(verdicts(traced), PIECE_KEYS)
    traced_pieces, all_pieces, frames = kept.count(), pieces.count(), traced.count()
    if traced_pieces != all_pieces:
        raise RuntimeError(f"The replays of {', '.join(games)} traced {traced_pieces} of {all_pieces} pieces")
    append(TRACE_PIECES, settings.lake, kept, gate)
    logger.info(f"Mined {', '.join(games)}: {frames} frames in {all_pieces} pieces")
    return frames
