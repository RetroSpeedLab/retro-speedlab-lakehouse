from charts import Columns, lake_table, virtual

VARCHAR = "VARCHAR"
DOUBLE = "DOUBLE"
BIGINT = "BIGINT"
INTEGER = "INTEGER"
BOOLEAN = "BOOLEAN"
TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"
DATE = "DATE"
RUNNER_MINUTES = 5
STREAM_LAG_MINUTES = 10
JOB_GAP_MINUTES = 90
UPTIME_PERCENT = 99
DISK_PERCENT = 85
HOST_MINUTES = 5
LAST_DAY = "current_timestamp - INTERVAL 24 HOURS"
UNION = "\n    UNION ALL\n    "


def minutes_since(column: str) -> str:
    return f"(epoch(current_timestamp) - epoch({column})) / 60"


def check(item: str, value: str, unit: str, detail: str, as_of: str, alert: str, source: str) -> str:
    return (
        f"SELECT '{item}' AS item, CAST({value} AS DOUBLE) AS value, '{unit}' AS unit, "
        f"CAST({detail} AS VARCHAR) AS detail, {as_of} AS as_of, {alert} AS alert FROM {source}"
    )


RUNNER = minutes_since("max(run_updated_at)")
SEARCHED = minutes_since("max(last_event_at)")
LAG = f"greatest(max(lag_seconds) / 60, {minutes_since('max(updated_at)')})"
LAST_JOB = minutes_since("max(finished_at)")
FAILED_JOBS = f"count(*) FILTER (WHERE status = 'failed' AND finished_at >= {LAST_DAY})"
QUALITY_FAILS = f"count(*) FILTER (WHERE checked_at >= {LAST_DAY})"
FAILED_JOB_NAMES = f"string_agg(DISTINCT CASE WHEN status = 'failed' AND finished_at >= {LAST_DAY} THEN job END, ', ')"
CHECKS = (
    check(
        "Runner reported",
        RUNNER,
        "min ago",
        "max(game)",
        "max(run_updated_at)",
        f"{RUNNER} > {RUNNER_MINUTES}",
        "live.live_run",
    ),
    check(
        "Search events",
        SEARCHED,
        "min ago",
        "concat(count(*), ' games searched in the last hour')",
        "max(last_event_at)",
        f"{SEARCHED} > {RUNNER_MINUTES}",
        "live.search_pulse",
    ),
    check(
        "Spark stream lag",
        LAG,
        "min",
        "concat(max(events_per_minute), ' events per minute')",
        "max(updated_at)",
        f"{LAG} > {STREAM_LAG_MINUTES}",
        "live.stream_status",
    ),
    check(
        "Last Airflow job",
        LAST_JOB,
        "min ago",
        "max_by(concat(dag, ' ', job, ' ', status), finished_at)",
        "max(finished_at)",
        f"{LAST_JOB} > {JOB_GAP_MINUTES} OR max_by(status, finished_at) = 'failed'",
        "gold.platform_jobs",
    ),
    check(
        "Failed Airflow jobs, 24 h",
        FAILED_JOBS,
        "runs",
        FAILED_JOB_NAMES,
        "max(finished_at)",
        f"{FAILED_JOBS} > 0",
        "gold.platform_jobs",
    ),
    check(
        "Quality gate fails, 24 h",
        QUALITY_FAILS,
        "reports",
        f"string_agg(DISTINCT CASE WHEN checked_at >= {LAST_DAY} THEN \"table\" END, ', ')",
        "max(checked_at)",
        f"{QUALITY_FAILS} > 0",
        "gold.quality_failures",
    ),
    check(
        "Monitor uptime",
        "value",
        "% up in 24 h",
        "label",
        "measured_at",
        f"value < {UPTIME_PERCENT}",
        "gold.source_numbers WHERE source = 'uptime' AND metric LIKE 'uptime/%'",
    ),
)
HEALTH_SQL = f"""SELECT
    CASE WHEN value IS NULL THEN 1 WHEN alert THEN 0 ELSE 2 END AS severity_order,
    CASE WHEN value IS NULL THEN 'NO DATA' WHEN alert THEN 'ALERT' ELSE 'OK' END AS state,
    item,
    round(value, 1) AS value,
    unit,
    detail,
    as_of
FROM (
    {UNION.join(CHECKS)}
) AS checks"""
HOSTS_SQL = f"""SELECT
    CASE WHEN disk_percent > {DISK_PERCENT} OR minutes > {HOST_MINUTES} THEN 'ALERT' ELSE 'OK' END AS state,
    host,
    round(disk_percent, 1) AS disk_percent,
    fullest_disk,
    round(memory_percent, 1) AS memory_percent,
    round(load_5 / cpus, 2) AS load_per_core,
    round(minutes, 1) AS reported_minutes_ago,
    measured_at
FROM (SELECT *, {minutes_since("measured_at")} AS minutes FROM live.host_metrics) AS hosts"""

HEALTH = virtual(
    "cockpit_health",
    HEALTH_SQL,
    (
        ("severity_order", INTEGER),
        ("state", VARCHAR),
        ("item", VARCHAR),
        ("value", DOUBLE),
        ("unit", VARCHAR),
        ("detail", VARCHAR),
        ("as_of", TIMESTAMPTZ),
    ),
    "One row per health check of the runner, the stream, Airflow, the quality gates and the monitors, alerts first.",
)
HOSTS = virtual(
    "cockpit_hosts",
    HOSTS_SQL,
    (
        ("state", VARCHAR),
        ("host", VARCHAR),
        ("disk_percent", DOUBLE),
        ("fullest_disk", VARCHAR),
        ("memory_percent", DOUBLE),
        ("load_per_core", DOUBLE),
        ("reported_minutes_ago", DOUBLE),
        ("measured_at", TIMESTAMPTZ),
    ),
    "The newest disk, memory and load of every server.",
)

COMPARISON: Columns = (
    ("control_n", BIGINT),
    ("treatment_n", BIGINT),
    ("enough", BOOLEAN),
    ("control_estimate", DOUBLE),
    ("treatment_estimate", DOUBLE),
    ("effect", DOUBLE),
    ("ci_lower", DOUBLE),
    ("ci_upper", DOUBLE),
    ("confidence", DOUBLE),
    ("p_value", DOUBLE),
)
COCKPIT_DAYS = lake_table(
    "gold",
    "cockpit_days",
    (("day", DATE), ("days_ago", INTEGER), ("metric", VARCHAR), ("value", DOUBLE), ("computed_at", TIMESTAMPTZ)),
)
SEARCH_HOURS = lake_table(
    "gold",
    "search_hours",
    (
        ("hour", TIMESTAMPTZ),
        ("game", VARCHAR),
        ("attempts", BIGINT),
        ("improvements", BIGINT),
        ("frames_saved", DOUBLE),
        ("records", BIGINT),
        ("ppo_rounds", BIGINT),
        ("ppo_reward_sum", DOUBLE),
        ("ppo_loss_sum", DOUBLE),
        ("computed_at", TIMESTAMPTZ),
    ),
)
AUDIENCE_HOURS = lake_table(
    "gold",
    "audience_hours",
    (
        ("hour", TIMESTAMPTZ),
        ("viewers_peak", DOUBLE),
        ("viewers_mean", DOUBLE),
        ("live_minutes", BIGINT),
        ("followers", DOUBLE),
        ("chat_messages", DOUBLE),
        ("chatters_peak", DOUBLE),
        ("computed_at", TIMESTAMPTZ),
    ),
)
LANGFUSE_DAILY = lake_table(
    "silver",
    "langfuse_daily",
    (
        ("date", DATE),
        ("observations", BIGINT),
        ("tokens", BIGINT),
        ("cost_usd", DOUBLE),
        ("measured_at", TIMESTAMPTZ),
    ),
)
HOST_METRICS = lake_table(
    "silver",
    "host_metrics",
    (
        ("host", VARCHAR),
        ("measured_at", TIMESTAMPTZ),
        ("disk_percent", DOUBLE),
        ("fullest_disk", VARCHAR),
        ("memory_percent", DOUBLE),
        ("memory_used_bytes", BIGINT),
        ("memory_total_bytes", BIGINT),
        ("load_1", DOUBLE),
        ("load_5", DOUBLE),
        ("load_15", DOUBLE),
        ("cpus", INTEGER),
        ("event_id", BIGINT),
        ("received_at", TIMESTAMPTZ),
    ),
)
STREAM_STATUS = lake_table(
    "live",
    "stream_status",
    (
        ("stream", VARCHAR),
        ("events_per_minute", BIGINT),
        ("lag_seconds", DOUBLE),
        ("lag_max_seconds", DOUBLE),
        ("last_event_at", TIMESTAMPTZ),
        ("updated_at", TIMESTAMPTZ),
    ),
)
POLICY_PULSE = lake_table(
    "live",
    "policy_pulse",
    (
        ("game", VARCHAR),
        ("run", VARCHAR),
        ("round", BIGINT),
        ("reward", DOUBLE),
        ("loss", DOUBLE),
        ("entropy", DOUBLE),
        ("exit_rate", DOUBLE),
        ("last_event_at", TIMESTAMPTZ),
        ("rounds_last_hour", BIGINT),
        ("reward_mean_last_hour", DOUBLE),
        ("updated_at", TIMESTAMPTZ),
    ),
)
LAB_RUNS = lake_table(
    "silver",
    "lab_runs",
    (
        ("game", VARCHAR),
        ("stamp", VARCHAR),
        ("started_at", TIMESTAMPTZ),
        ("outcome", VARCHAR),
        ("cost_usd", DOUBLE),
        ("minutes", DOUBLE),
        ("turns", INTEGER),
        ("subagents", INTEGER),
        ("commits", INTEGER),
        ("tool_errors", INTEGER),
        ("event_id", BIGINT),
    ),
)
LAB_EXPERIMENTS = lake_table(
    "gold",
    "lab_experiments",
    (
        ("game", VARCHAR),
        ("stamp", VARCHAR),
        ("changed_at", TIMESTAMPTZ),
        ("metric", VARCHAR),
        ("better", VARCHAR),
        *COMPARISON,
        ("holm_p", DOUBLE),
        ("bh_p", DOUBLE),
        ("verdict", VARCHAR),
        ("computed_at", TIMESTAMPTZ),
    ),
)
LAB_ROI = lake_table(
    "gold",
    "lab_roi",
    (
        ("game", VARCHAR),
        ("stamp", VARCHAR),
        ("outcome", VARCHAR),
        ("finished_at", TIMESTAMPTZ),
        ("cost_usd", DOUBLE),
        ("reported_cost_usd", DOUBLE),
        ("searched_hours", BIGINT),
        ("frames_saved", DOUBLE),
        ("frames_saved_lower", DOUBLE),
        ("frames_saved_upper", DOUBLE),
        ("frames_per_dollar", DOUBLE),
        ("counted", BOOLEAN),
        ("total_cost_usd", DOUBLE),
        ("total_frames_saved", DOUBLE),
        ("total_frames_per_dollar", DOUBLE),
        ("harm_flagged", BOOLEAN),
        ("harm_after_hours", BIGINT),
        ("computed_at", TIMESTAMPTZ),
    ),
)
PROMOTION_ROI = lake_table(
    "gold",
    "promotion_roi",
    (
        ("channel", VARCHAR),
        ("variant", INTEGER),
        ("drafts", BIGINT),
        ("cost_usd", DOUBLE),
        ("views", BIGINT),
        ("likes", BIGINT),
        ("follows", BIGINT),
        ("views_per_dollar", DOUBLE),
        ("views_lower", DOUBLE),
        ("views_upper", DOUBLE),
        ("likes_per_dollar", DOUBLE),
        ("likes_lower", DOUBLE),
        ("likes_upper", DOUBLE),
        ("computed_at", TIMESTAMPTZ),
    ),
)
SEARCH_ALLOCATION = lake_table(
    "gold",
    "search_allocation",
    (
        ("game", VARCHAR),
        ("role", VARCHAR),
        ("share", DOUBLE),
        ("best_probability", DOUBLE),
        ("saved", BIGINT),
        ("improvements", BIGINT),
        ("worker_seconds", DOUBLE),
        ("rate_mean", DOUBLE),
        ("rate_lower", DOUBLE),
        ("rate_upper", DOUBLE),
        ("days", BIGINT),
        ("computed_at", TIMESTAMPTZ),
    ),
)
SEARCH_BANDIT = lake_table(
    "gold",
    "search_bandit",
    (("metric", VARCHAR), ("better", VARCHAR), *COMPARISON, ("verdict", VARCHAR), ("computed_at", TIMESTAMPTZ)),
)
SOURCE_NUMBERS = lake_table(
    "gold",
    "source_numbers",
    (
        ("source", VARCHAR),
        ("metric", VARCHAR),
        ("label", VARCHAR),
        ("value", DOUBLE),
        ("unit", VARCHAR),
        ("window", VARCHAR),
        ("measured_at", TIMESTAMPTZ),
        ("computed_at", TIMESTAMPTZ),
    ),
)
PLAY_COLUMNS = (
    ("game", VARCHAR),
    ("mode", VARCHAR),
    ("window", VARCHAR),
    ("started", BIGINT),
    ("won", BIGINT),
    ("measured_at", TIMESTAMPTZ),
    ("computed_at", TIMESTAMPTZ),
)
GAME_PLAYS = lake_table("gold", "game_plays", PLAY_COLUMNS)
ITCH_PLAYS = lake_table("gold", "itch_plays", PLAY_COLUMNS)
ITCH_DAILY = lake_table(
    "gold",
    "itch_daily",
    (
        ("game", VARCHAR),
        ("date", DATE),
        ("views", BIGINT),
        ("campaign", BIGINT),
        ("endless", BIGINT),
        ("cleared", BIGINT),
        ("won", BIGINT),
        ("measured_at", TIMESTAMPTZ),
        ("computed_at", TIMESTAMPTZ),
    ),
)
ITCH_VISITS = lake_table(
    "silver",
    "itch_daily",
    (("date", DATE), ("pageviews", BIGINT), ("visitors", BIGINT), ("measured_at", TIMESTAMPTZ)),
)
COMMUNITY_DAILY = lake_table(
    "silver",
    "community_daily",
    (("date", DATE), ("discord_messages", BIGINT), ("discord_joins", BIGINT), ("measured_at", TIMESTAMPTZ)),
)
CONTENT_RANKING = lake_table(
    "gold",
    "content_ranking",
    (
        ("surface", VARCHAR),
        ("item", VARCHAR),
        ("family", VARCHAR),
        ("exposures", DOUBLE),
        ("outcomes", DOUBLE),
        ("mean", DOUBLE),
        ("lower", DOUBLE),
        ("upper", DOUBLE),
        ("lead_probability", DOUBLE),
        ("share", DOUBLE),
        ("rank", BIGINT),
        ("computed_at", TIMESTAMPTZ),
    ),
)
ENGAGEMENT_EXPERIMENTS = lake_table(
    "gold",
    "engagement_experiments",
    (
        ("experiment", VARCHAR),
        ("kind", VARCHAR),
        ("metric", VARCHAR),
        ("primary", BOOLEAN),
        ("control", VARCHAR),
        ("treatment", VARCHAR),
        *COMPARISON,
        ("holm_p", DOUBLE),
        ("srm_ok", BOOLEAN),
        ("verdict", VARCHAR),
        ("winner", VARCHAR),
        ("computed_at", TIMESTAMPTZ),
    ),
)
TWITCH_CHANNEL_OUTCOMES = lake_table(
    "gold",
    "twitch_channel_outcomes",
    (
        ("item", VARCHAR),
        ("arm", VARCHAR),
        ("game", VARCHAR),
        ("started_at", TIMESTAMPTZ),
        ("ended_at", TIMESTAMPTZ),
        ("hours", DOUBLE),
        ("average_viewers", DOUBLE),
        ("peak_viewers", DOUBLE),
        ("viewer_hours", DOUBLE),
        ("new_followers", DOUBLE),
        ("followers_per_hour", DOUBLE),
        ("chat_messages", DOUBLE),
        ("chat_per_viewer_hour", DOUBLE),
    ),
)
QUALITY_REPORTS = lake_table(
    "gold",
    "quality_reports",
    (
        ("table", VARCHAR),
        ("run", VARCHAR),
        ("status", VARCHAR),
        ("checks", INTEGER),
        ("failed", INTEGER),
        ("rows", BIGINT),
        ("checked_at", TIMESTAMPTZ),
    ),
)
SOURCES_STATUS = lake_table(
    "gold",
    "sources_status",
    (
        ("source", VARCHAR),
        ("stream", VARCHAR),
        ("last_records", BIGINT),
        ("last_ingest_at", TIMESTAMPTZ),
        ("watermark", VARCHAR),
        ("total_records", BIGINT),
    ),
)
GAME_SUMMARY = lake_table(
    "gold",
    "game_summary",
    (
        ("game", VARCHAR),
        ("title", VARCHAR),
        ("won", BOOLEAN),
        ("run_seconds", DOUBLE),
        ("record_seconds", DOUBLE),
        ("frames", BIGINT),
        ("levels", INTEGER),
        ("run_updated_at", TIMESTAMPTZ),
        ("levels_cleared", BIGINT),
        ("saved_since_first_clear_seconds", DOUBLE),
        ("costliest_level", VARCHAR),
        ("costliest_frames", BIGINT),
        ("costliest_share", DOUBLE),
        ("first_at", TIMESTAMPTZ),
        ("last_change_at", TIMESTAMPTZ),
        ("history_points", BIGINT),
        ("lab_runs", BIGINT),
        ("lab_deployed", BIGINT),
        ("lab_minutes", DOUBLE),
        ("hours_searched", DOUBLE),
        ("saved_since_yesterday_seconds", DOUBLE),
        ("computed_at", TIMESTAMPTZ),
    ),
)
LEVEL_COSTS = lake_table(
    "gold",
    "level_costs",
    (
        ("game", VARCHAR),
        ("level", VARCHAR),
        ("position", INTEGER),
        ("frames", BIGINT),
        ("level_seconds", DOUBLE),
        ("first_seconds", DOUBLE),
        ("saved_seconds", DOUBLE),
        ("share", DOUBLE),
        ("costliest", BOOLEAN),
    ),
)
LAB_OUTCOMES = lake_table(
    "gold",
    "lab_outcomes",
    (
        ("game", VARCHAR),
        ("outcome", VARCHAR),
        ("runs", BIGINT),
        ("minutes", DOUBLE),
        ("cost_usd", DOUBLE),
        ("commits", BIGINT),
    ),
)
PLATFORM_TABLES = lake_table(
    "gold",
    "platform_tables",
    (
        ("table", VARCHAR),
        ("layer", VARCHAR),
        ("store", VARCHAR),
        ("rows", BIGINT),
        ("bytes", BIGINT),
        ("files", BIGINT),
        ("version", BIGINT),
        ("modified_at", TIMESTAMPTZ),
        ("measured_at", TIMESTAMPTZ),
    ),
)
PLATFORM_JOBS = lake_table(
    "gold",
    "platform_jobs",
    (
        ("dag", VARCHAR),
        ("run_id", VARCHAR),
        ("job", VARCHAR),
        ("started_at", TIMESTAMPTZ),
        ("finished_at", TIMESTAMPTZ),
        ("status", VARCHAR),
        ("rows", BIGINT),
        ("error", VARCHAR),
    ),
)
