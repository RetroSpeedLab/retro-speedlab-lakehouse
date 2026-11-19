from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.mining.gold import level_segments, mining_summary, ram_heatmap, won_runs
from retro_speedlab_lakehouse.mining.humans import board_history, split_comparison
from retro_speedlab_lakehouse.mining.records import FRAME, PIECE, piece_rows
from retro_speedlab_lakehouse.mining.verify import SPLIT, splits_of
from retro_speedlab_lakehouse.stream.rows import parsed

from .conftest import event, kafka_rows

MINED_AT = datetime(2026, 11, 15, 12, tzinfo=UTC)
GAME = "comet_courier"
TOPIC = "highscores.entries"
NO_BYTES = {"start": "", "end": "", "inputs": ""}
PIECES: dict[str, Any] = {
    "game": "comet_courier",
    "record": "abc",
    "version": "1.2.0",
    "cartridge_version": "1.2.0",
    "labels": ["Stage", "Heat"],
    "levels": {"RooftopRun": "Stage 1", "NeonAlley": "Stage 2"},
    "pieces": [
        {"segment": 0, "entered": "Menu", "reached": "RooftopRun", "won": False, "first_frame": 0, "frames": 2},
        {"segment": 1, "entered": "RooftopRun", "reached": "NeonAlley", "won": False, "first_frame": 2, "frames": 30},
        {"segment": 2, "entered": "NeonAlley", "reached": "NeonAlley", "won": True, "first_frame": 32, "frames": 10},
    ],
}


def latest(spark: SparkSession) -> DataFrame:
    document = {**PIECES, "pieces": [{**piece, **NO_BYTES} for piece in PIECES["pieces"]]}
    rows = spark.createDataFrame(piece_rows(document, MINED_AT), PIECE)
    return rows.drop("start", "end").withColumn("identical", F.lit(True))


def score(index: int, frames: int, at: str) -> dict[str, Any]:
    return {"id": index, "game": GAME, "version": "1.2.0", "name": f"P{index}", "frames": frames, "set_at": at}


def test_the_costliest_level_and_piece_come_from_the_record(spark: SparkSession) -> None:
    pieces = latest(spark)
    segments = level_segments(pieces)
    assert [row["level"] for row in segments.orderBy("segment").collect()] == ["Menu", "Stage 1", "Stage 2"]
    summary = mining_summary(pieces, segments).first()
    assert summary is not None
    assert (summary["frames"], summary["costliest_level"], summary["costliest_piece_segment"]) == (42, "Stage 1", 1)
    assert summary["replays_identically"] and summary["costliest_share"] == 30 / 42


def test_wide_ram_values_fold_into_bins(spark: SparkSession) -> None:
    rows = [("comet_courier", "abc", 1, "Stage 1", frame, None, None, None, {"X": frame}, True) for frame in range(64)]
    heat = ram_heatmap(spark.createDataFrame(rows, FRAME)).collect()
    assert {row["width"] for row in heat} == {2} and sum(row["frames"] for row in heat) == 64


def test_the_board_keeps_only_improvements(spark: SparkSession) -> None:
    scores = [(1, 9000, "2026-11-15T10:00:00Z"), (2, 9500, "2026-11-15T11:00:00Z"), (3, 8000, "2026-11-15T12:00:00Z")]
    events = [event(index, TOPIC, str(index), score(index, frames, at), MINED_AT) for index, frames, at in scores]
    parsed_events = parsed(kafka_rows(spark, events))
    history = board_history(parsed_events, won_runs(parsed_events)).orderBy("at").collect()
    assert [row["frames"] for row in history] == [9000, 8000]


def test_the_board_history_of_a_minor_version_spans_its_patches(spark: SparkSession) -> None:
    scores = [
        (1, 9000, "1.2.0", "2026-11-15T10:00:00Z"),
        (2, 9500, "1.2.1", "2026-11-15T11:00:00Z"),
        (3, 8000, "1.2.1", "2026-11-15T12:00:00Z"),
        (4, 9900, "1.3.0", "2026-11-15T13:00:00Z"),
    ]
    events = [
        event(index, TOPIC, str(index), {**score(index, frames, at), "version": version}, MINED_AT)
        for index, frames, version, at in scores
    ]
    parsed_events = parsed(kafka_rows(spark, events))
    history = board_history(parsed_events, won_runs(parsed_events)).orderBy("at").collect()
    assert [(row["version"], row["frames"]) for row in history] == [("1.2.0", 9000), ("1.2.1", 8000), ("1.3.0", 9900)]


def test_human_splits_line_up_with_the_ai_levels(spark: SparkSession) -> None:
    ai = [("ai", "abc", "comet_courier", "1.2.0", *split) for split in splits_of([[0, 0], [32, 1]], 42)]
    human = [("human", "1:50", "comet_courier", "1.2.0", *split) for split in splits_of([[0, 0], [40, 1]], 50)]
    columns = "side string, entry string, game string, version string, " + ", ".join(
        f"{field.name} {field.dataType.simpleString()}" for field in SPLIT.fields
    )
    kept = spark.createDataFrame(ai + human, columns)
    humans = spark.createDataFrame([("comet_courier", "1:50")], "game string, entry string")
    rows = split_comparison(kept, level_segments(latest(spark)), humans).orderBy("level_index").collect()
    assert [(row["level"], row["ai_frames"], row["human_best_frames"]) for row in rows] == [
        ("Stage 1", 32, 40),
        ("Stage 2", 10, 10),
    ]
