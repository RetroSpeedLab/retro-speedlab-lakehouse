from spark_data_quality import AllowedValues, Check, InRange, NotNull, QualitySuite, RowCount, Unique

from retro_speedlab_lakehouse.lake.catalog import (
    BOARD_HISTORY,
    GAME_RUNS,
    GAME_SUMMARY,
    GAMES,
    HIGHSCORES,
    HISTORY,
    HUMANS_VS_AI,
    IMPROVEMENT,
    KNOWLEDGE,
    LAB_OUTCOMES,
    LAB_PROGRESS,
    LAB_RUNS,
    LEVEL_COSTS,
    LEVEL_HISTORY,
    LEVEL_SEGMENTS,
    LIVE_RUN,
    MINING_SUMMARY,
    POSITION_HEATMAP,
    RAM_HEATMAP,
    RECORDS_DAILY,
    REPORTS,
    RUN_VERSIONS,
    SPLIT_COMPARISON,
    SPLITS,
    VERIFICATION_STATS,
    VERIFIER_REPLAYS,
    VERIFIER_SPLITS,
    Table,
)

OUTCOMES = frozenset({"rejected", "notes", "deployed"})
KNOWLEDGE_KINDS = frozenset({"mechanics", "enemies", "powerups", "levels", "ram"})
SIDES = frozenset({"human", "ai"})
REPLAY_STATUSES = frozenset({"verified", "rejected", "unserved", "no_inputs"})
HUMAN_STATUSES = REPLAY_STATUSES | {"pending"}


def present(*columns: str) -> list[Check]:
    return [NotNull(column) for column in columns]


def keyed(table: Table) -> list[Check]:
    if len(table.keys) == 1:
        return [*present(*table.keys), Unique(table.keys[0])]
    return present(*table.keys)


def at_least_zero(*columns: str) -> list[Check]:
    return [InRange(column, min_value=0) for column in columns]


CHECKS: dict[Table, list[Check]] = {
    LIVE_RUN: [*keyed(LIVE_RUN), *present("game", "event_id", "payload"), RowCount(max_count=1)],
    RUN_VERSIONS: [*keyed(RUN_VERSIONS), *present("game", "received_at"), *at_least_zero("seconds", "frames")],
    GAME_RUNS: [*keyed(GAME_RUNS), *present("event_id", "payload"), *at_least_zero("seconds", "frames")],
    REPORTS: [*keyed(REPORTS), *present("headline", "event_id")],
    LAB_RUNS: [*keyed(LAB_RUNS), AllowedValues("outcome", OUTCOMES), *at_least_zero("minutes", "cost_usd")],
    HIGHSCORES: [*keyed(HIGHSCORES), *present("game", "version"), InRange("frames", min_value=1)],
    GAMES: [*keyed(GAMES), Unique("number"), *present("title", "version")],
    LAB_PROGRESS: [*keyed(LAB_PROGRESS), *present("progressed", "seconds")],
    SPLITS: [*keyed(SPLITS), *at_least_zero("level_seconds", "first_seconds", "frames")],
    HISTORY: [*present("game", "at", "seconds"), *at_least_zero("seconds")],
    KNOWLEDGE: [*keyed(KNOWLEDGE), AllowedValues("kind", KNOWLEDGE_KINDS)],
    GAME_SUMMARY: [*keyed(GAME_SUMMARY), *present("title", "won"), *at_least_zero("levels_cleared", "lab_runs")],
    LEVEL_COSTS: [*keyed(LEVEL_COSTS), InRange("share", min_value=0, max_value=1), *at_least_zero("frames")],
    IMPROVEMENT: [*keyed(IMPROVEMENT), *at_least_zero("seconds")],
    RECORDS_DAILY: [*keyed(RECORDS_DAILY), *at_least_zero("record_seconds", "saved_seconds")],
    LAB_OUTCOMES: [*keyed(LAB_OUTCOMES), AllowedValues("outcome", OUTCOMES), InRange("runs", min_value=1)],
    VERIFIER_REPLAYS: [
        *keyed(VERIFIER_REPLAYS),
        AllowedValues("side", SIDES),
        AllowedValues("status", REPLAY_STATUSES),
        *present("game", "version", "served_version"),
        InRange("frames", min_value=1),
    ],
    VERIFIER_SPLITS: [*keyed(VERIFIER_SPLITS), *present("level_index"), *at_least_zero("entered_at", "frames")],
    LEVEL_SEGMENTS: [*keyed(LEVEL_SEGMENTS), *present("record", "level"), *at_least_zero("frames", "first_frame")],
    LEVEL_HISTORY: [*keyed(LEVEL_HISTORY), *at_least_zero("frames", "first_frames")],
    POSITION_HEATMAP: [*keyed(POSITION_HEATMAP), InRange("frames", min_value=1)],
    RAM_HEATMAP: [*keyed(RAM_HEATMAP), InRange("frames", min_value=1), InRange("width", min_value=1)],
    MINING_SUMMARY: [
        *keyed(MINING_SUMMARY),
        *present("record", "costliest_level", "replays_identically"),
        InRange("frames", min_value=1),
        InRange("costliest_share", min_value=0, max_value=1),
    ],
    BOARD_HISTORY: [*keyed(BOARD_HISTORY), AllowedValues("side", SIDES), InRange("frames", min_value=1)],
    HUMANS_VS_AI: [*keyed(HUMANS_VS_AI), AllowedValues("status", HUMAN_STATUSES), InRange("place", min_value=1)],
    SPLIT_COMPARISON: [*keyed(SPLIT_COMPARISON), *at_least_zero("ai_frames", "human_best_frames", "humans")],
    VERIFICATION_STATS: [*keyed(VERIFICATION_STATS), AllowedValues("side", SIDES), InRange("replays", min_value=1)],
}


def suite_of(table: Table) -> QualitySuite | None:
    if table not in CHECKS:
        return None
    return QualitySuite(name=table.full_name, checks=CHECKS[table])
