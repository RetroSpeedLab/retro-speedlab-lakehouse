from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.batch.major import current_major
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
    won_runs,
)
from retro_speedlab_lakehouse.mining.humans import (
    board_history,
    humans_vs_ai,
    split_comparison,
    verification_stats,
    verified_splits,
)
from retro_speedlab_lakehouse.mining.verify import AI, HUMAN
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings, Settings

Built = list[tuple[Table, DataFrame]]
NO_REPLAYS = "side string, entry string, status string"


def current_pieces(spark: SparkSession, lake: LakeSettings, games: DataFrame) -> DataFrame:
    return current_major(read(spark, TRACE_PIECES, lake), "cartridge_version", games)


def mined_tables(spark: SparkSession, lake: LakeSettings, games: DataFrame) -> Built:
    built: Built = []
    if exists(spark, TRACE_PIECES, lake):
        latest = latest_pieces(current_pieces(spark, lake, games))
        segments = level_segments(latest)
        built += [(LEVEL_SEGMENTS, segments), (MINING_SUMMARY, mining_summary(latest, segments))]
        if exists(spark, FRAME_TRACES, lake):
            records = latest.select("game", "record").distinct()
            frames = read(spark, FRAME_TRACES, lake).join(records, ["game", "record"], "left_semi")
            built += [(POSITION_HEATMAP, position_heatmap(frames)), (RAM_HEATMAP, ram_heatmap(frames))]
    if exists(spark, EVENTS, lake):
        events = read(spark, EVENTS, lake)
        won = current_major(won_runs(events), "version", games)
        built += [(LEVEL_HISTORY, level_history(won)), (BOARD_HISTORY, board_history(events, won))]
    return built


def replays_of(spark: SparkSession, lake: LakeSettings) -> DataFrame:
    if exists(spark, VERIFIER_REPLAYS, lake):
        return read(spark, VERIFIER_REPLAYS, lake)
    return spark.createDataFrame([], NO_REPLAYS)


def human_tables(spark: SparkSession, lake: LakeSettings, games: DataFrame) -> Built:
    if not all(exists(spark, table, lake) for table in (HIGHSCORES, GAME_RUNS)):
        return []
    replays = replays_of(spark, lake)
    runs = current_major(read(spark, GAME_RUNS, lake), "version", games)
    humans = humans_vs_ai(read(spark, HIGHSCORES, lake), games, runs, replays)
    if not all(exists(spark, table, lake) for table in (VERIFIER_SPLITS, TRACE_PIECES)):
        return [(HUMANS_VS_AI, humans)]
    kept = verified_splits(read(spark, VERIFIER_SPLITS, lake), replays)
    segments = level_segments(latest_pieces(current_pieces(spark, lake, games)))
    return [(HUMANS_VS_AI, humans), (SPLIT_COMPARISON, split_comparison(kept, segments, humans))]


def replay_tables(spark: SparkSession, lake: LakeSettings, games: DataFrame) -> Built:
    if not all(exists(spark, table, lake) for table in (VERIFIER_REPLAYS, TRACE_PIECES)):
        return []
    replays = read(spark, VERIFIER_REPLAYS, lake)
    records = current_pieces(spark, lake, games).select("game", F.col("record").alias("entry")).distinct()
    ai = replays.where(F.col("side") == AI).join(records, ["game", "entry"], "left_semi")
    return [(VERIFICATION_STATS, verification_stats(replays.where(F.col("side") == HUMAN).unionByName(ai)))]


def build_mining_gold(spark: SparkSession, settings: Settings, gate: Gate) -> int:
    games = read(spark, GAMES, settings.lake)
    built = [
        *mined_tables(spark, settings.lake, games),
        *human_tables(spark, settings.lake, games),
        *replay_tables(spark, settings.lake, games),
    ]
    written = 0
    for table, rows in built:
        replace(table, settings.lake, rows, gate)
        written += rows.count()
    return written
