from spark_data_quality import AllowedValues, Check, InRange, NotNull, QualitySuite, RowCount, Unique

from retro_speedlab_lakehouse.lake.catalog import (
    GAME_RUNS,
    GAME_SUMMARY,
    GAMES,
    HIGHSCORES,
    HISTORY,
    IMPROVEMENT,
    KNOWLEDGE,
    LAB_OUTCOMES,
    LAB_PROGRESS,
    LAB_RUNS,
    LEVEL_COSTS,
    LIVE_RUN,
    RECORDS_DAILY,
    REPORTS,
    RUN_VERSIONS,
    SPLITS,
    Table,
)

OUTCOMES = frozenset({"rejected", "notes", "deployed"})
KNOWLEDGE_KINDS = frozenset({"mechanics", "enemies", "powerups", "levels", "ram"})


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
}


def suite_of(table: Table) -> QualitySuite | None:
    if table not in CHECKS:
        return None
    return QualitySuite(name=table.full_name, checks=CHECKS[table])
