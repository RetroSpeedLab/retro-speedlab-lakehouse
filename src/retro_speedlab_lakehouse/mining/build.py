from pyspark.sql import DataFrame, SparkSession

from retro_speedlab_lakehouse.lake.catalog import (
    BOARD_HISTORY,
    EVENTS,
    FRAME_TRACES,
    GAME_RUNS,
    GAMES,
    HIGHSCORES,
    HUMANS_VS_AI,
    LEVEL_HISTORY,
    LEVEL_SEGMENTS,
    MINING_SUMMARY,
    POSITION_HEATMAP,
    RAM_HEATMAP,
    SPLIT_COMPARISON,
    TRACE_PIECES,
    VERIFICATION_STATS,
    VERIFIER_REPLAYS,
    VERIFIER_SPLITS,
    Table,
)
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.mining.gold import (
    latest_pieces,
    level_history,
    level_segments,
    mining_summary,
    position_heatmap,
    ram_heatmap,
)
from retro_speedlab_lakehouse.mining.humans import (
    board_history,
    humans_vs_ai,
    split_comparison,
    verification_stats,
    verified_splits,
)
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings, Settings

Built = list[tuple[Table, DataFrame]]
NO_REPLAYS = "side string, entry string, status string"


def mined_tables(spark: SparkSession, lake: LakeSettings) -> Built:
    built: Built = []
    if exists(spark, TRACE_PIECES, lake):
        latest = latest_pieces(read(spark, TRACE_PIECES, lake))
        segments = level_segments(latest)
        built += [(LEVEL_SEGMENTS, segments), (MINING_SUMMARY, mining_summary(latest, segments))]
    if exists(spark, FRAME_TRACES, lake):
        frames = read(spark, FRAME_TRACES, lake)
        built += [(POSITION_HEATMAP, position_heatmap(frames)), (RAM_HEATMAP, ram_heatmap(frames))]
    if exists(spark, EVENTS, lake):
        events = read(spark, EVENTS, lake)
        built += [(LEVEL_HISTORY, level_history(events)), (BOARD_HISTORY, board_history(events))]
    return built


def replays_of(spark: SparkSession, lake: LakeSettings) -> DataFrame:
    if exists(spark, VERIFIER_REPLAYS, lake):
        return read(spark, VERIFIER_REPLAYS, lake)
    return spark.createDataFrame([], NO_REPLAYS)


def human_tables(spark: SparkSession, lake: LakeSettings) -> Built:
    if not all(exists(spark, table, lake) for table in (HIGHSCORES, GAMES, GAME_RUNS)):
        return []
    replays = replays_of(spark, lake)
    humans = humans_vs_ai(
        read(spark, HIGHSCORES, lake), read(spark, GAMES, lake), read(spark, GAME_RUNS, lake), replays
    )
    if not all(exists(spark, table, lake) for table in (VERIFIER_SPLITS, TRACE_PIECES)):
        return [(HUMANS_VS_AI, humans)]
    kept = verified_splits(read(spark, VERIFIER_SPLITS, lake), replays)
    segments = level_segments(latest_pieces(read(spark, TRACE_PIECES, lake)))
    return [(HUMANS_VS_AI, humans), (SPLIT_COMPARISON, split_comparison(kept, segments, humans))]


def replay_tables(spark: SparkSession, lake: LakeSettings) -> Built:
    if not exists(spark, VERIFIER_REPLAYS, lake):
        return []
    return [(VERIFICATION_STATS, verification_stats(read(spark, VERIFIER_REPLAYS, lake)))]


def build_mining_gold(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    written = 0
    built = (
        mined_tables(spark, settings.lake) + human_tables(spark, settings.lake) + replay_tables(spark, settings.lake)
    )
    for table, rows in built:
        replace(table, settings.lake, rows, gate)
        written += rows.count()
    return written
