from spark_data_quality import AllowedValues, Check, InRange, NotNull, QualitySuite, RowCount, Unique, UniqueKey

from retro_speedlab_lakehouse.cockpit.catalog import (
    AUDIENCE_HOURS,
    COCKPIT_DAYS,
    COCKPIT_FINDINGS,
    COCKPIT_SUMMARY,
    SEARCH_HOURS,
)
from retro_speedlab_lakehouse.cockpit.findings import CLAUDE, FINDINGS, RANKS
from retro_speedlab_lakehouse.engagement.catalog import (
    COMMUNITY_DAILY,
    CONTENT_RANKING,
    ENGAGEMENT_EVENTS,
    ENGAGEMENT_EXPERIMENTS,
    TWITCH_CHANNEL_OUTCOMES,
)
from retro_speedlab_lakehouse.lake.catalog import (
    BOARD_HISTORY,
    CHAT_PULSE,
    GAME_RUNS,
    GAME_SUMMARY,
    GAMES,
    HIGHSCORES,
    HISTORY,
    HOST_METRICS,
    HOST_STATUS,
    HUMANS_VS_AI,
    IMPROVEMENT,
    KNOWLEDGE,
    LAB_EXPERIMENTS,
    LAB_OUTCOMES,
    LAB_PROGRESS,
    LAB_ROI,
    LAB_RUNS,
    LAB_STATUS,
    LEVEL_COSTS,
    LEVEL_HISTORY,
    LEVEL_SEGMENTS,
    LIVE_RUN,
    MINING_SUMMARY,
    POLICY_PULSE,
    POSITION_HEATMAP,
    PROMOTION_DECISIONS,
    PROMOTION_PREFERENCES,
    PROMOTION_ROI,
    RAM_HEATMAP,
    RECORDS_DAILY,
    REPORTS,
    RUN_VERSIONS,
    SEARCH_ALLOCATION,
    SEARCH_BANDIT,
    SEARCH_PULSE,
    SPLIT_COMPARISON,
    SPLITS,
    VERIFICATION_STATS,
    VERIFIER_REPLAYS,
    VERIFIER_SPLITS,
    Table,
)
from retro_speedlab_lakehouse.sources.catalog import (
    GAME_PLAYS,
    HUMAN_DROPOFF,
    ITCH_DAILY,
    ITCH_PLAYS,
    LANGFUSE_DAILY,
    SILVER_DROPOFF,
    SILVER_ITCH_DAILY,
    SILVER_ITCH_PLAYS,
    SILVER_LAB_COSTS,
    SILVER_NUMBERS,
    SILVER_PLAYS,
    SOURCE_NUMBERS,
)
from retro_speedlab_lakehouse.sources.plays import MODES, WINDOWS

OUTCOMES = frozenset({"rejected", "notes", "deployed"})
KNOWLEDGE_KINDS = frozenset({"mechanics", "enemies", "powerups", "levels", "ram"})
SIDES = frozenset({"human", "ai"})
REPLAY_STATUSES = frozenset({"verified", "rejected", "unserved", "no_inputs"})
HUMAN_STATUSES = REPLAY_STATUSES | {"pending"}
LAB_KINDS = frozenset({"started", "judging", "finished"})
VERDICTS = frozenset({"better", "worse", "no clear change", "not enough data yet"})
PLAY_MODES = frozenset(MODES)
PLAY_WINDOWS = frozenset(WINDOWS)
ENGAGEMENT_VERDICTS = VERDICTS | {"sample ratio mismatch"}
ARMS = frozenset({"fixed", "adaptive", "holdout"})
FAMILIES = frozenset({"beta", "gamma"})


def present(*columns: str) -> list[Check]:
    return [NotNull(column) for column in columns]


def keyed(table: Table) -> list[Check]:
    if len(table.keys) == 1:
        return [*present(*table.keys), Unique(table.keys[0])]
    return [*present(*table.keys), UniqueKey(table.keys)]


def at_least_zero(*columns: str) -> list[Check]:
    return [InRange(column, min_value=0) for column in columns]


def host_checks(table: Table) -> list[Check]:
    shares = [InRange(column, min_value=0, max_value=100) for column in ("disk_percent", "memory_percent")]
    return [*keyed(table), *shares, *at_least_zero("load_1", "load_5", "load_15"), InRange("cpus", min_value=1)]


CHECKS: dict[Table, list[Check]] = {
    LIVE_RUN: [*keyed(LIVE_RUN), *present("game", "event_id", "payload"), RowCount(max_count=1)],
    RUN_VERSIONS: [*keyed(RUN_VERSIONS), *present("game", "received_at"), *at_least_zero("seconds", "frames")],
    GAME_RUNS: [*keyed(GAME_RUNS), *present("event_id", "payload"), *at_least_zero("seconds", "frames")],
    REPORTS: [*keyed(REPORTS), *present("headline", "event_id")],
    LAB_RUNS: [*keyed(LAB_RUNS), AllowedValues("outcome", OUTCOMES), *at_least_zero("minutes", "cost_usd")],
    HIGHSCORES: [*keyed(HIGHSCORES), *present("game", "version"), InRange("frames", min_value=1)],
    HOST_STATUS: host_checks(HOST_STATUS),
    HOST_METRICS: host_checks(HOST_METRICS),
    GAMES: [*keyed(GAMES), Unique("number"), *present("title", "version"), RowCount(min_count=1)],
    LAB_PROGRESS: [*keyed(LAB_PROGRESS), *present("progressed", "seconds")],
    SPLITS: [*keyed(SPLITS), *at_least_zero("level_seconds", "first_seconds", "frames")],
    HISTORY: [*keyed(HISTORY), *at_least_zero("seconds")],
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
    SEARCH_PULSE: [
        *keyed(SEARCH_PULSE),
        *at_least_zero("attempts_last_hour", "attempts_per_minute", "frames_saved_last_hour", "records_last_hour"),
        *present("last_event_at", "updated_at"),
    ],
    POLICY_PULSE: [
        *keyed(POLICY_PULSE),
        *present("round", "reward", "last_event_at"),
        InRange("rounds_last_hour", min_value=1),
    ],
    LAB_STATUS: [
        *keyed(LAB_STATUS),
        *present("game", "stamp", "last_event_at"),
        AllowedValues("kind", LAB_KINDS),
        RowCount(max_count=1),
    ],
    CHAT_PULSE: [
        *keyed(CHAT_PULSE),
        *at_least_zero("messages_last_minute", "chatters_last_minute", "messages_last_hour"),
    ],
    LAB_EXPERIMENTS: [
        *keyed(LAB_EXPERIMENTS),
        AllowedValues("verdict", VERDICTS),
        *at_least_zero("control_n", "treatment_n"),
    ],
    SEARCH_ALLOCATION: [
        *keyed(SEARCH_ALLOCATION),
        InRange("share", min_value=0, max_value=1),
        InRange("best_probability", min_value=0, max_value=1),
        *at_least_zero("saved", "improvements", "worker_seconds", "rate_mean", "rate_lower", "rate_upper"),
    ],
    SEARCH_BANDIT: [
        *keyed(SEARCH_BANDIT),
        AllowedValues("verdict", VERDICTS),
        *at_least_zero("control_n", "treatment_n"),
        RowCount(max_count=1),
    ],
    ENGAGEMENT_EVENTS: [*keyed(ENGAGEMENT_EVENTS), AllowedValues("arm", ARMS), *at_least_zero("exposures", "outcomes")],
    COMMUNITY_DAILY: [*keyed(COMMUNITY_DAILY), *at_least_zero("discord_messages", "discord_joins")],
    CONTENT_RANKING: [
        *keyed(CONTENT_RANKING),
        AllowedValues("family", FAMILIES),
        InRange("share", min_value=0, max_value=1),
        InRange("lead_probability", min_value=0, max_value=1),
        InRange("sampled_rank", min_value=1),
        InRange("rank", min_value=1),
        *at_least_zero("exposures", "outcomes", "mean", "lower", "upper"),
        *present("period", "computed_at"),
    ],
    ENGAGEMENT_EXPERIMENTS: [
        *keyed(ENGAGEMENT_EXPERIMENTS),
        AllowedValues("verdict", ENGAGEMENT_VERDICTS),
        *at_least_zero("control_n", "treatment_n"),
        *present("primary", "srm_ok", "computed_at"),
    ],
    TWITCH_CHANNEL_OUTCOMES: [
        *keyed(TWITCH_CHANNEL_OUTCOMES),
        AllowedValues("arm", ARMS),
        *present("ended_at", "hours", "new_followers", "followers_per_hour"),
        *at_least_zero("hours", "average_viewers", "peak_viewers", "viewer_hours", "chat_messages"),
    ],
    SILVER_NUMBERS: [*keyed(SILVER_NUMBERS), *present("label", "value", "unit", "window", "measured_at")],
    SOURCE_NUMBERS: [*keyed(SOURCE_NUMBERS), *present("label", "value", "unit", "window", "computed_at")],
    **{
        table: [
            *keyed(table),
            AllowedValues("mode", PLAY_MODES),
            AllowedValues("window", PLAY_WINDOWS),
            *present("started", "won", "measured_at"),
        ]
        for table in (SILVER_PLAYS, SILVER_ITCH_PLAYS)
    },
    SILVER_ITCH_DAILY: [
        *keyed(SILVER_ITCH_DAILY),
        *present("views", "campaign", "endless", "cleared", "won", "measured_at"),
        *at_least_zero("pageviews", "visitors"),
    ],
    ITCH_DAILY: [
        *keyed(ITCH_DAILY),
        *at_least_zero("views", "campaign", "endless", "cleared", "won"),
        *present("measured_at", "computed_at"),
    ],
    COCKPIT_DAYS: [*keyed(COCKPIT_DAYS), *present("day", "computed_at"), InRange("days_ago", min_value=0)],
    SEARCH_HOURS: [
        *keyed(SEARCH_HOURS),
        *at_least_zero("attempts", "improvements", "frames_saved", "records", "ppo_rounds"),
    ],
    AUDIENCE_HOURS: [*keyed(AUDIENCE_HOURS), *at_least_zero("viewers_peak", "live_minutes", "chat_messages")],
    COCKPIT_SUMMARY: [
        *keyed(COCKPIT_SUMMARY),
        AllowedValues("source", frozenset({CLAUDE, FINDINGS})),
        *present("summary"),
    ],
    COCKPIT_FINDINGS: [
        *keyed(COCKPIT_FINDINGS),
        AllowedValues("verdict", frozenset(str(verdict) for verdict in RANKS)),
        *present("statement", "rank", "computed_at"),
    ],
    LANGFUSE_DAILY: [*keyed(LANGFUSE_DAILY), *at_least_zero("observations", "tokens", "cost_usd")],
    **{
        table: [
            *keyed(table),
            AllowedValues("mode", PLAY_MODES),
            AllowedValues("window", PLAY_WINDOWS),
            *at_least_zero("started", "won"),
            *present("started", "won", "measured_at", "computed_at"),
        ]
        for table in (GAME_PLAYS, ITCH_PLAYS)
    },
    SILVER_DROPOFF: [*keyed(SILVER_DROPOFF), *present("ended", "won", "since", "measured_at"), *at_least_zero("won")],
    HUMAN_DROPOFF: [
        *keyed(HUMAN_DROPOFF),
        InRange("reached", min_value=1),
        InRange("ended_share", min_value=0, max_value=1),
        *at_least_zero("ended", "won", "humans"),
        *present("runs", "since", "computed_at"),
    ],
    SILVER_LAB_COSTS: [*keyed(SILVER_LAB_COSTS), *at_least_zero("observations", "tokens", "cost_usd")],
    LAB_ROI: [
        *keyed(LAB_ROI),
        AllowedValues("outcome", OUTCOMES),
        *at_least_zero("cost_usd", "reported_cost_usd", "searched_hours", "total_cost_usd"),
        *present("finished_at", "counted", "computed_at"),
    ],
    PROMOTION_ROI: [
        *keyed(PROMOTION_ROI),
        InRange("drafts", min_value=1),
        *at_least_zero("cost_usd", "likes", "views", "follows"),
    ],
    PROMOTION_DECISIONS: [
        *keyed(PROMOTION_DECISIONS),
        *present("channel", "kind", "variant", "text", "approved", "decided_at"),
        *at_least_zero("length", "numbers"),
    ],
    PROMOTION_PREFERENCES: [
        *keyed(PROMOTION_PREFERENCES),
        InRange("decisions", min_value=1),
        *at_least_zero("approved", "rejected"),
        *(InRange(column, min_value=0, max_value=1) for column in ("approval_rate", "lower", "upper")),
        *present("computed_at"),
    ],
}


def suite_of(table: Table) -> QualitySuite | None:
    if table not in CHECKS:
        return None
    return QualitySuite(name=table.full_name, checks=CHECKS[table])
