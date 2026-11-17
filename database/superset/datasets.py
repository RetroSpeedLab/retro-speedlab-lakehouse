from charts import Columns, lake_table, virtual

STRING = "STRING"
DOUBLE = "DOUBLE"
BIGINT = "BIGINT"
INT = "INT"
BOOLEAN = "BOOLEAN"
TIMESTAMP = "TIMESTAMP"
DATE = "DATE"
RUNNER_MINUTES = 5
STREAM_LAG_MINUTES = 10
JOB_GAP_MINUTES = 90
UPTIME_PERCENT = 99
DISK_PERCENT = 85
HOST_MINUTES = 5
LAST_DAY = "current_timestamp() - INTERVAL 24 HOURS"
UNION = "\n    UNION ALL\n    "


def minutes_since(column: str) -> str:
    return f"(unix_timestamp(current_timestamp()) - unix_timestamp({column})) / 60"


def check(item: str, value: str, unit: str, detail: str, as_of: str, alert: str, source: str) -> str:
    return (
        f"SELECT '{item}' AS item, CAST({value} AS DOUBLE) AS value, '{unit}' AS unit, "
        f"CAST({detail} AS STRING) AS detail, {as_of} AS as_of, {alert} AS alert FROM {source}"
    )


RUNNER = minutes_since("max(run_updated_at)")
SEARCHED = minutes_since("max(last_event_at)")
LAG = f"greatest(max(lag_seconds) / 60, {minutes_since('max(updated_at)')})"
LAST_JOB = minutes_since("max(finished_at)")
FAILED_JOBS = f"count_if(status = 'failed' AND finished_at >= {LAST_DAY})"
FAILED_JOB_NAMES = (
    f"array_join(collect_set(CASE WHEN status = 'failed' AND finished_at >= {LAST_DAY} THEN job END), ', ')"
)
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
        f"count_if(checked_at >= {LAST_DAY})",
        "reports",
        f"array_join(collect_set(CASE WHEN checked_at >= {LAST_DAY} THEN `table` END), ', ')",
        "max(checked_at)",
        f"count_if(checked_at >= {LAST_DAY}) > 0",
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
        ("severity_order", INT),
        ("state", STRING),
        ("item", STRING),
        ("value", DOUBLE),
        ("unit", STRING),
        ("detail", STRING),
        ("as_of", TIMESTAMP),
    ),
    "One row per health check of the runner, the stream, Airflow, the quality gates and the monitors, alerts first.",
)
HOSTS = virtual(
    "cockpit_hosts",
    HOSTS_SQL,
    (
        ("state", STRING),
        ("host", STRING),
        ("disk_percent", DOUBLE),
        ("fullest_disk", STRING),
        ("memory_percent", DOUBLE),
        ("load_per_core", DOUBLE),
        ("reported_minutes_ago", DOUBLE),
        ("measured_at", TIMESTAMP),
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
    (("day", DATE), ("days_ago", INT), ("metric", STRING), ("value", DOUBLE), ("computed_at", TIMESTAMP)),
)
SEARCH_HOURS = lake_table(
    "gold",
    "search_hours",
    (
        ("hour", TIMESTAMP),
        ("game", STRING),
        ("attempts", BIGINT),
        ("improvements", BIGINT),
        ("frames_saved", DOUBLE),
        ("records", BIGINT),
        ("ppo_rounds", BIGINT),
        ("ppo_reward_sum", DOUBLE),
        ("ppo_loss_sum", DOUBLE),
        ("computed_at", TIMESTAMP),
    ),
)
AUDIENCE_HOURS = lake_table(
    "gold",
    "audience_hours",
    (
        ("hour", TIMESTAMP),
        ("viewers_peak", DOUBLE),
        ("viewers_mean", DOUBLE),
        ("live_minutes", BIGINT),
        ("followers", DOUBLE),
        ("chat_messages", DOUBLE),
        ("chatters_peak", DOUBLE),
        ("computed_at", TIMESTAMP),
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
        ("measured_at", TIMESTAMP),
    ),
)
HOST_METRICS = lake_table(
    "silver",
    "host_metrics",
    (
        ("host", STRING),
        ("measured_at", TIMESTAMP),
        ("disk_percent", DOUBLE),
        ("fullest_disk", STRING),
        ("memory_percent", DOUBLE),
        ("memory_used_bytes", BIGINT),
        ("memory_total_bytes", BIGINT),
        ("load_1", DOUBLE),
        ("load_5", DOUBLE),
        ("load_15", DOUBLE),
        ("cpus", INT),
        ("event_id", BIGINT),
        ("received_at", TIMESTAMP),
    ),
)
STREAM_STATUS = lake_table(
    "live",
    "stream_status",
    (
        ("stream", STRING),
        ("events_per_minute", BIGINT),
        ("lag_seconds", DOUBLE),
        ("lag_max_seconds", DOUBLE),
        ("last_event_at", TIMESTAMP),
        ("updated_at", TIMESTAMP),
    ),
)
POLICY_PULSE = lake_table(
    "live",
    "policy_pulse",
    (
        ("game", STRING),
        ("run", STRING),
        ("round", BIGINT),
        ("reward", DOUBLE),
        ("loss", DOUBLE),
        ("entropy", DOUBLE),
        ("exit_rate", DOUBLE),
        ("last_event_at", TIMESTAMP),
        ("rounds_last_hour", BIGINT),
        ("reward_mean_last_hour", DOUBLE),
        ("updated_at", TIMESTAMP),
    ),
)
LAB_RUNS = lake_table(
    "silver",
    "lab_runs",
    (
        ("game", STRING),
        ("stamp", STRING),
        ("started_at", TIMESTAMP),
        ("outcome", STRING),
        ("cost_usd", DOUBLE),
        ("minutes", DOUBLE),
        ("turns", INT),
        ("subagents", INT),
        ("commits", INT),
        ("tool_errors", INT),
        ("event_id", BIGINT),
    ),
)
LAB_EXPERIMENTS = lake_table(
    "gold",
    "lab_experiments",
    (
        ("game", STRING),
        ("stamp", STRING),
        ("changed_at", TIMESTAMP),
        ("metric", STRING),
        ("better", STRING),
        *COMPARISON,
        ("holm_p", DOUBLE),
        ("bh_p", DOUBLE),
        ("verdict", STRING),
        ("computed_at", TIMESTAMP),
    ),
)
LAB_ROI = lake_table(
    "gold",
    "lab_roi",
    (
        ("game", STRING),
        ("stamp", STRING),
        ("outcome", STRING),
        ("finished_at", TIMESTAMP),
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
        ("computed_at", TIMESTAMP),
    ),
)
PROMOTION_ROI = lake_table(
    "gold",
    "promotion_roi",
    (
        ("channel", STRING),
        ("variant", INT),
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
        ("computed_at", TIMESTAMP),
    ),
)
SEARCH_ALLOCATION = lake_table(
    "gold",
    "search_allocation",
    (
        ("game", STRING),
        ("role", STRING),
        ("share", DOUBLE),
        ("best_probability", DOUBLE),
        ("saved", BIGINT),
        ("improvements", BIGINT),
        ("worker_seconds", DOUBLE),
        ("rate_mean", DOUBLE),
        ("rate_lower", DOUBLE),
        ("rate_upper", DOUBLE),
        ("days", BIGINT),
        ("computed_at", TIMESTAMP),
    ),
)
SEARCH_BANDIT = lake_table(
    "gold",
    "search_bandit",
    (("metric", STRING), ("better", STRING), *COMPARISON, ("verdict", STRING), ("computed_at", TIMESTAMP)),
)
SOURCE_NUMBERS = lake_table(
    "gold",
    "source_numbers",
    (
        ("source", STRING),
        ("metric", STRING),
        ("label", STRING),
        ("value", DOUBLE),
        ("unit", STRING),
        ("window", STRING),
        ("measured_at", TIMESTAMP),
        ("computed_at", TIMESTAMP),
    ),
)
PLAY_COLUMNS = (
    ("game", STRING),
    ("mode", STRING),
    ("window", STRING),
    ("started", BIGINT),
    ("won", BIGINT),
    ("measured_at", TIMESTAMP),
    ("computed_at", TIMESTAMP),
)
GAME_PLAYS = lake_table("gold", "game_plays", PLAY_COLUMNS)
ITCH_PLAYS = lake_table("gold", "itch_plays", PLAY_COLUMNS)
ITCH_DAILY = lake_table(
    "gold",
    "itch_daily",
    (
        ("game", STRING),
        ("date", DATE),
        ("views", BIGINT),
        ("campaign", BIGINT),
        ("endless", BIGINT),
        ("cleared", BIGINT),
        ("won", BIGINT),
        ("measured_at", TIMESTAMP),
        ("computed_at", TIMESTAMP),
    ),
)
ITCH_VISITS = lake_table(
    "silver",
    "itch_daily",
    (("date", DATE), ("pageviews", BIGINT), ("visitors", BIGINT), ("measured_at", TIMESTAMP)),
)
COMMUNITY_DAILY = lake_table(
    "silver",
    "community_daily",
    (("date", DATE), ("discord_messages", BIGINT), ("discord_joins", BIGINT), ("measured_at", TIMESTAMP)),
)
CONTENT_RANKING = lake_table(
    "gold",
    "content_ranking",
    (
        ("surface", STRING),
        ("item", STRING),
        ("family", STRING),
        ("exposures", DOUBLE),
        ("outcomes", DOUBLE),
        ("mean", DOUBLE),
        ("lower", DOUBLE),
        ("upper", DOUBLE),
        ("lead_probability", DOUBLE),
        ("share", DOUBLE),
        ("rank", BIGINT),
        ("computed_at", TIMESTAMP),
    ),
)
ENGAGEMENT_EXPERIMENTS = lake_table(
    "gold",
    "engagement_experiments",
    (
        ("experiment", STRING),
        ("kind", STRING),
        ("metric", STRING),
        ("primary", BOOLEAN),
        ("control", STRING),
        ("treatment", STRING),
        *COMPARISON,
        ("holm_p", DOUBLE),
        ("srm_ok", BOOLEAN),
        ("verdict", STRING),
        ("winner", STRING),
        ("computed_at", TIMESTAMP),
    ),
)
TWITCH_CHANNEL_OUTCOMES = lake_table(
    "gold",
    "twitch_channel_outcomes",
    (
        ("item", STRING),
        ("arm", STRING),
        ("game", STRING),
        ("started_at", TIMESTAMP),
        ("ended_at", TIMESTAMP),
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
        ("table", STRING),
        ("run", STRING),
        ("status", STRING),
        ("checks", INT),
        ("failed", INT),
        ("rows", BIGINT),
        ("checked_at", TIMESTAMP),
    ),
)
SOURCES_STATUS = lake_table(
    "gold",
    "sources_status",
    (
        ("source", STRING),
        ("stream", STRING),
        ("last_records", BIGINT),
        ("last_ingest_at", TIMESTAMP),
        ("watermark", STRING),
        ("total_records", BIGINT),
    ),
)
GAME_SUMMARY = lake_table(
    "gold",
    "game_summary",
    (
        ("game", STRING),
        ("title", STRING),
        ("won", BOOLEAN),
        ("run_seconds", DOUBLE),
        ("record_seconds", DOUBLE),
        ("frames", BIGINT),
        ("levels", INT),
        ("run_updated_at", TIMESTAMP),
        ("levels_cleared", BIGINT),
        ("saved_since_first_clear_seconds", DOUBLE),
        ("costliest_level", STRING),
        ("costliest_frames", BIGINT),
        ("costliest_share", DOUBLE),
        ("first_at", TIMESTAMP),
        ("last_change_at", TIMESTAMP),
        ("history_points", BIGINT),
        ("lab_runs", BIGINT),
        ("lab_deployed", BIGINT),
        ("lab_minutes", DOUBLE),
        ("hours_searched", DOUBLE),
        ("saved_since_yesterday_seconds", DOUBLE),
        ("computed_at", TIMESTAMP),
    ),
)
LEVEL_COSTS = lake_table(
    "gold",
    "level_costs",
    (
        ("game", STRING),
        ("level", STRING),
        ("position", INT),
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
        ("game", STRING),
        ("outcome", STRING),
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
        ("table", STRING),
        ("layer", STRING),
        ("store", STRING),
        ("rows", BIGINT),
        ("bytes", BIGINT),
        ("files", BIGINT),
        ("version", BIGINT),
        ("modified_at", TIMESTAMP),
        ("measured_at", TIMESTAMP),
    ),
)
PLATFORM_JOBS = lake_table(
    "gold",
    "platform_jobs",
    (
        ("dag", STRING),
        ("run_id", STRING),
        ("job", STRING),
        ("started_at", TIMESTAMP),
        ("finished_at", TIMESTAMP),
        ("status", STRING),
        ("rows", BIGINT),
        ("error", STRING),
    ),
)
