from dataclasses import dataclass

from charts import Chart, columns_over_time, findings, headline, kpi, number, ranked, table, trend
from datasets import (
    AUDIENCE_HOURS,
    COCKPIT_DAYS,
    COCKPIT_FINDINGS,
    COCKPIT_SUMMARY,
    COMMUNITY_DAILY,
    CONTENT_RANKING,
    ENGAGEMENT_EXPERIMENTS,
    GAME_PLAYS,
    GAME_SUMMARY,
    HEALTH,
    HOST_METRICS,
    HOSTS,
    ITCH_DAILY,
    ITCH_PLAYS,
    ITCH_VISITS,
    LAB_EXPERIMENTS,
    LAB_OUTCOMES,
    LAB_ROI,
    LAB_RUNS,
    LANGFUSE_DAILY,
    LAST_DAY,
    LEVEL_COSTS,
    PLATFORM_JOBS,
    PLATFORM_TABLES,
    POLICY_PULSE,
    PROMOTION_ROI,
    QUALITY_REPORTS,
    SEARCH_ALLOCATION,
    SEARCH_BANDIT,
    SEARCH_HOURS,
    SOURCE_NUMBERS,
    SOURCES_STATUS,
    STREAM_STATUS,
    TWITCH_CHANNEL_OUTCOMES,
    minutes_since,
)

TILE_HEIGHT = 24
HEADLINE_HEIGHT = 16
FINDINGS_HEIGHT = 70
STRIP_HEIGHT = 40
CHART_HEIGHT = 50
TABLE_HEIGHT = 60
REFRESH_SECONDS = 300
NO_REFRESH = 0
WEEK = ("hour >= current_timestamp - INTERVAL 7 DAYS",)
JOB_MINUTES = "(epoch(finished_at) - epoch(started_at)) / 60"
SAVED_SINCE_FIRST_CLEAR = "saved_since_first_clear_seconds"
LAB_PER_DOLLAR = "MAX(total_frames_per_dollar)"
PROMOTION_PER_DOLLAR = "SUM(views) / NULLIF(SUM(cost_usd), 0)"
LAB_RUN_PER_DOLLAR = "SUM(frames_saved) / NULLIF(SUM(cost_usd), 0)"
SEARCH_PER_CPU_HOUR = "SUM(saved) / NULLIF(SUM(worker_seconds) / 3600, 0)"
COMPARISON = ("control_n", "treatment_n", "control_estimate", "treatment_estimate", "effect", "ci_lower", "ci_upper")


@dataclass(frozen=True, slots=True)
class Row:
    height: int
    charts: tuple[Chart, ...]


@dataclass(frozen=True, slots=True)
class Section:
    title: str
    rows: tuple[Row, ...]


@dataclass(frozen=True, slots=True)
class Dashboard:
    title: str
    slug: str
    refresh_seconds: int
    sections: tuple[Section, ...]

    @property
    def charts(self) -> list[Chart]:
        return [chart for section in self.sections for row in section.rows for chart in row.charts]


def day_tile(metric: str, expression: str) -> Chart:
    return kpi(COCKPIT_DAYS, metric, "day", expression, (f"metric = '{metric}'",))


def source_tile(name: str, source: str, metric: str) -> Chart:
    return number(SOURCE_NUMBERS, name, "MAX(value)", (f"source = '{source}'", f"metric = '{metric}'"))


def numbers_table(name: str, sources: str) -> Chart:
    columns = ("source", "label", "value", "unit", "window", "measured_at")
    return table(SOURCE_NUMBERS, name, columns, "label", (f"source IN ({sources})",))


def meaning(name: str, areas: tuple[str, ...], height: int) -> Section:
    listed = ", ".join(f"'{area}'" for area in areas)
    return Section("What this means", (Row(height, (findings(COCKPIT_FINDINGS, name, (f"area IN ({listed})",)),)),))


def tiles(*charts: Chart) -> Row:
    return Row(TILE_HEIGHT, charts)


def charts(*charts: Chart) -> Row:
    return Row(CHART_HEIGHT, charts)


def tables(*charts: Chart) -> Row:
    return Row(TABLE_HEIGHT, charts)


OVERVIEW = Dashboard(
    "Cockpit",
    "cockpit",
    REFRESH_SECONDS,
    (
        Section("Today", (Row(HEADLINE_HEIGHT, (headline(COCKPIT_SUMMARY, "Today in one line"),)),)),
        meaning(
            "Findings across the platform",
            ("search", "records", "lab", "platform", "audience", "cost", "data quality"),
            FINDINGS_HEIGHT,
        ),
        Section(
            "Health",
            (
                tables(
                    table(
                        HEALTH,
                        "Health checks",
                        ("state", "item", "value", "unit", "detail", "as_of"),
                        "severity_order",
                        (),
                    ),
                    table(
                        HOSTS,
                        "Servers",
                        (
                            "state",
                            "host",
                            "disk_percent",
                            "fullest_disk",
                            "memory_percent",
                            "load_per_core",
                            "reported_minutes_ago",
                        ),
                        "-disk_percent",
                        (),
                    ),
                ),
            ),
        ),
        Section(
            "Last 24 hours against the 24 hours before",
            (
                tiles(
                    day_tile("Records", "SUM(value)"),
                    day_tile("Frames saved", "SUM(value)"),
                    day_tile("Lab runs", "SUM(value)"),
                    day_tile("Highscores", "SUM(value)"),
                ),
                tiles(
                    day_tile("Human game starts", "SUM(value)"),
                    day_tile("Peak viewers", "MAX(value)"),
                    day_tile("Twitch followers", "MAX(value)"),
                    day_tile("Chat messages", "SUM(value)"),
                ),
                tiles(
                    day_tile("Discord members", "MAX(value)"),
                    kpi(
                        LANGFUSE_DAILY,
                        "Claude cost per full day (USD)",
                        "date",
                        "SUM(cost_usd)",
                        ("date < current_date",),
                    ),
                ),
            ),
        ),
        Section(
            "Return on cost",
            (
                tiles(
                    number(LAB_ROI, "Lab frames saved per Claude dollar", LAB_PER_DOLLAR, ()),
                    number(PROMOTION_ROI, "Promotion views per Claude dollar", PROMOTION_PER_DOLLAR, ()),
                    number(SEARCH_ALLOCATION, "Search frames saved per CPU-hour", SEARCH_PER_CPU_HOUR, ()),
                    number(LAB_ROI, "Lab changes flagged as harmful", "COUNT(*) FILTER (WHERE harm_flagged)", ()),
                ),
            ),
        ),
    ),
)
SEARCH_AI = Dashboard(
    "Search and AI",
    "search-ai",
    NO_REFRESH,
    (
        meaning("Findings for search, records and the lab", ("search", "records", "lab"), STRIP_HEIGHT),
        Section(
            "Records",
            (
                tiles(
                    number(GAME_SUMMARY, "Games with a record", "COUNT(record_seconds)", ()),
                    number(GAME_SUMMARY, "Seconds saved since the first clears", f"SUM({SAVED_SINCE_FIRST_CLEAR})", ()),
                    number(GAME_SUMMARY, "Seconds saved since yesterday", "SUM(saved_since_yesterday_seconds)", ()),
                    number(GAME_SUMMARY, "Hours searched", "SUM(hours_searched)", ()),
                ),
                tables(
                    table(
                        GAME_SUMMARY,
                        "Records per game",
                        (
                            "game",
                            "title",
                            "record_seconds",
                            "levels_cleared",
                            "levels",
                            SAVED_SINCE_FIRST_CLEAR,
                            "saved_since_yesterday_seconds",
                            "costliest_level",
                            "lab_runs",
                            "hours_searched",
                        ),
                        "game",
                        (),
                    ),
                ),
            ),
        ),
        Section(
            "Search",
            (
                charts(
                    columns_over_time(
                        SEARCH_HOURS, "Frames saved per hour", "hour", "PT1H", "SUM(frames_saved)", (), WEEK
                    ),
                    ranked(SEARCH_HOURS, "Frames saved per game, 7 days", "game", "SUM(frames_saved)", (), WEEK),
                ),
                charts(
                    columns_over_time(SEARCH_HOURS, "Records per day", "hour", "P1D", "SUM(records)", (), WEEK),
                    table(
                        LEVEL_COSTS,
                        "Costliest level per game",
                        ("game", "level", "frames", "level_seconds", "share"),
                        "game",
                        ("costliest = true",),
                    ),
                ),
            ),
        ),
        Section(
            "Search bandit",
            (
                charts(
                    ranked(SEARCH_ALLOCATION, "Bandit share of workers per game", "game", "SUM(share)", ("role",), ()),
                    table(
                        SEARCH_BANDIT,
                        "Bandit against its holdout",
                        ("metric", "better", *COMPARISON, "p_value", "verdict"),
                        "metric",
                        (),
                    ),
                ),
            ),
        ),
        Section(
            "PPO policy",
            (
                charts(
                    trend(
                        SEARCH_HOURS,
                        "PPO mean reward per hour",
                        "hour",
                        "PT1H",
                        "SUM(ppo_reward_sum) / SUM(ppo_rounds)",
                        (),
                        WEEK + ("ppo_rounds > 0",),
                    ),
                ),
                tables(
                    table(
                        POLICY_PULSE,
                        "Policy now",
                        (
                            "game",
                            "round",
                            "reward",
                            "loss",
                            "entropy",
                            "exit_rate",
                            "rounds_last_hour",
                            "last_event_at",
                        ),
                        "-last_event_at",
                        (),
                    ),
                ),
            ),
        ),
        Section(
            "Lab",
            (
                charts(
                    ranked(LAB_OUTCOMES, "Lab runs per outcome", "outcome", "SUM(runs)", (), ()),
                    table(
                        LAB_RUNS,
                        "Lab runs, last 14 days",
                        ("started_at", "game", "outcome", "minutes", "cost_usd", "commits"),
                        "-started_at",
                        ("started_at >= current_timestamp - INTERVAL 14 DAYS",),
                    ),
                ),
                tables(
                    table(
                        LAB_EXPERIMENTS,
                        "Lab changes as experiments",
                        ("changed_at", "game", "metric", "better", *COMPARISON, "holm_p", "verdict"),
                        "-changed_at",
                        (),
                    ),
                ),
            ),
        ),
    ),
)
AUDIENCE = Dashboard(
    "Audience",
    "audience",
    NO_REFRESH,
    (
        meaning("Findings for the audience", ("audience",), STRIP_HEIGHT),
        Section(
            "Now",
            (
                tiles(
                    source_tile("Twitch followers now", "twitch", "followers"),
                    source_tile("Discord members now", "discord", "members"),
                    source_tile("Visitors, 7 days", "umami", "visitors"),
                    source_tile("Human game starts, 7 days", "umami", "game_plays"),
                ),
            ),
        ),
        Section(
            "Twitch",
            (
                charts(
                    trend(
                        AUDIENCE_HOURS, "Twitch viewers per hour, peak", "hour", "PT1H", "MAX(viewers_peak)", (), WEEK
                    ),
                    columns_over_time(
                        AUDIENCE_HOURS, "Chat messages per hour", "hour", "PT1H", "SUM(chat_messages)", (), WEEK
                    ),
                ),
                charts(trend(AUDIENCE_HOURS, "Twitch followers per hour", "hour", "PT1H", "MAX(followers)", (), WEEK)),
            ),
        ),
        Section(
            "Site, games and Discord",
            (
                charts(
                    ranked(
                        GAME_PLAYS,
                        "Human game starts per game, 7 days",
                        "game",
                        "SUM(started)",
                        (),
                        ("\"window\" = 'last_week'",),
                    ),
                    numbers_table("Site and channel numbers", "'umami', 'itch', 'twitch', 'discord'"),
                ),
                charts(
                    columns_over_time(
                        COMMUNITY_DAILY, "Discord messages per day", "date", "P1D", "SUM(discord_messages)", (), ()
                    ),
                    columns_over_time(
                        COMMUNITY_DAILY, "Discord joins per day", "date", "P1D", "SUM(discord_joins)", (), ()
                    ),
                ),
            ),
        ),
        Section(
            "LAB24 on itch.io",
            (
                tiles(
                    source_tile("itch.io visitors, 7 days", "itch", "visitors"),
                    source_tile("itch.io game starts, 7 days", "itch", "game_plays"),
                    source_tile("itch.io campaigns won, 7 days", "itch", "game_wins"),
                ),
                charts(
                    ranked(
                        ITCH_PLAYS,
                        "itch.io game starts per game, 7 days",
                        "game",
                        "SUM(started)",
                        (),
                        ("\"window\" = 'last_week'",),
                    ),
                    columns_over_time(
                        ITCH_DAILY, "itch.io game starts per day", "date", "P1D", "SUM(campaign + endless)", (), ()
                    ),
                ),
                charts(
                    columns_over_time(ITCH_VISITS, "itch.io visitors per day", "date", "P1D", "SUM(visitors)", (), ()),
                    columns_over_time(
                        ITCH_DAILY, "itch.io levels cleared per day", "date", "P1D", "SUM(cleared)", (), ()
                    ),
                ),
            ),
        ),
        Section(
            "Engagement",
            (
                tables(
                    table(
                        CONTENT_RANKING,
                        "Engagement ranking",
                        ("surface", "rank", "item", "share", "mean", "lower", "upper", "lead_probability", "exposures"),
                        "rank",
                        (),
                    ),
                ),
                tables(
                    table(
                        ENGAGEMENT_EXPERIMENTS,
                        "A/B and holdout results",
                        (
                            "experiment",
                            "kind",
                            "metric",
                            "control",
                            "treatment",
                            *COMPARISON,
                            "holm_p",
                            "verdict",
                            "winner",
                        ),
                        "experiment",
                        (),
                    ),
                ),
                charts(
                    ranked(
                        TWITCH_CHANNEL_OUTCOMES,
                        "Twitch new followers per hour by category and title",
                        "item",
                        "SUM(new_followers) / SUM(hours)",
                        (),
                        (),
                    ),
                    ranked(
                        TWITCH_CHANNEL_OUTCOMES,
                        "Twitch average viewers by category and title",
                        "item",
                        "SUM(average_viewers * hours) / SUM(hours)",
                        (),
                        (),
                    ),
                ),
                tables(
                    table(
                        TWITCH_CHANNEL_OUTCOMES,
                        "Twitch channel turns",
                        (
                            "started_at",
                            "item",
                            "arm",
                            "game",
                            "hours",
                            "average_viewers",
                            "peak_viewers",
                            "new_followers",
                            "followers_per_hour",
                            "chat_per_viewer_hour",
                        ),
                        "-started_at",
                        (),
                    ),
                ),
            ),
        ),
    ),
)
PLATFORM_COST = Dashboard(
    "Platform and cost",
    "platform-cost",
    NO_REFRESH,
    (
        meaning("Findings for the platform, data quality and cost", ("platform", "data quality", "cost"), STRIP_HEIGHT),
        Section(
            "Now",
            (
                tiles(
                    number(
                        STREAM_STATUS,
                        "Stream lag (min)",
                        f"MAX(greatest(lag_seconds / 60, {minutes_since('updated_at')}))",
                        (),
                    ),
                    number(
                        PLATFORM_JOBS,
                        "Failed job runs, 24 h",
                        "COUNT(*)",
                        ("status = 'failed'", f"finished_at >= {LAST_DAY}"),
                    ),
                    number(
                        QUALITY_REPORTS,
                        "Quality gate fails, 24 h",
                        "COUNT(*)",
                        ("status <> 'PASS'", f"checked_at >= {LAST_DAY}"),
                    ),
                ),
            ),
        ),
        Section(
            "Airflow",
            (
                charts(
                    columns_over_time(
                        PLATFORM_JOBS,
                        "Job runs per day by status",
                        "started_at",
                        "P1D",
                        "COUNT(*)",
                        ("status",),
                        ("started_at >= current_timestamp - INTERVAL 14 DAYS",),
                    ),
                    ranked(PLATFORM_JOBS, "Average job minutes", "job", f"AVG({JOB_MINUTES})", (), ()),
                ),
                tables(
                    table(
                        PLATFORM_JOBS,
                        "Failed job runs",
                        ("started_at", "dag", "job", "error"),
                        "-started_at",
                        ("status = 'failed'",),
                    ),
                    table(
                        SOURCES_STATUS,
                        "Source loads",
                        ("source", "stream", "last_ingest_at", "last_records", "total_records"),
                        "source",
                        (),
                    ),
                ),
            ),
        ),
        Section(
            "Quality",
            (
                tables(
                    table(
                        QUALITY_REPORTS,
                        "Quality gate fails, 7 days",
                        ("checked_at", "table", "run", "failed", "checks"),
                        "-checked_at",
                        ("status <> 'PASS'", "checked_at >= current_timestamp - INTERVAL 7 DAYS"),
                    ),
                ),
            ),
        ),
        Section(
            "Servers",
            (
                charts(
                    trend(
                        HOST_METRICS,
                        "Disk use per server (%)",
                        "measured_at",
                        "PT1H",
                        "MAX(disk_percent)",
                        ("host",),
                        (),
                    ),
                    trend(
                        HOST_METRICS,
                        "Memory use per server (%)",
                        "measured_at",
                        "PT1H",
                        "AVG(memory_percent)",
                        ("host",),
                        (),
                    ),
                ),
                charts(
                    trend(
                        HOST_METRICS,
                        "Load per core per server",
                        "measured_at",
                        "PT1H",
                        "AVG(load_5 / cpus)",
                        ("host",),
                        (),
                    )
                ),
            ),
        ),
        Section(
            "Storage, cost and errors",
            (
                charts(
                    ranked(PLATFORM_TABLES, "Largest tables (GB)", "table", "SUM(bytes) / 1e9", (), ()),
                    columns_over_time(
                        LANGFUSE_DAILY, "Claude cost per day (USD)", "date", "P1D", "SUM(cost_usd)", (), ()
                    ),
                ),
                tables(
                    numbers_table("Errors (Bugsink)", "'bugsink'"),
                    numbers_table("Uptime (Uptime Kuma)", "'uptime'"),
                ),
            ),
        ),
    ),
)
RETURN_ON_COST = Dashboard(
    "Return on cost",
    "return-on-cost",
    NO_REFRESH,
    (
        meaning("Findings for cost and the lab", ("cost", "lab"), STRIP_HEIGHT),
        Section(
            "Lab: frames saved per Claude dollar",
            (
                tiles(
                    number(LAB_ROI, "Lab frames saved, counted runs", "MAX(total_frames_saved)", ()),
                    number(LAB_ROI, "Lab Claude cost, counted runs (USD)", "MAX(total_cost_usd)", ()),
                    number(PROMOTION_ROI, "Promotion views", "SUM(views)", ()),
                    number(PROMOTION_ROI, "Promotion Claude cost (USD)", "SUM(cost_usd)", ()),
                ),
                charts(
                    ranked(
                        LAB_ROI,
                        "Lab frames saved per Claude dollar per game",
                        "game",
                        LAB_RUN_PER_DOLLAR,
                        (),
                        ("counted = true",),
                    ),
                    table(
                        LAB_ROI,
                        "Lab runs with their return",
                        (
                            "finished_at",
                            "game",
                            "outcome",
                            "cost_usd",
                            "frames_saved",
                            "frames_saved_lower",
                            "frames_saved_upper",
                            "frames_per_dollar",
                            "counted",
                            "harm_flagged",
                            "harm_after_hours",
                        ),
                        "-finished_at",
                        (),
                    ),
                ),
            ),
        ),
        Section(
            "Promotion: views per Claude dollar",
            (
                charts(
                    ranked(
                        PROMOTION_ROI,
                        "Promotion views per Claude dollar per network",
                        "channel",
                        PROMOTION_PER_DOLLAR,
                        (),
                        (),
                    ),
                    table(
                        PROMOTION_ROI,
                        "Promotion per network and variant",
                        (
                            "channel",
                            "variant",
                            "drafts",
                            "cost_usd",
                            "views",
                            "views_per_dollar",
                            "views_lower",
                            "views_upper",
                            "likes",
                            "likes_per_dollar",
                            "likes_lower",
                            "likes_upper",
                            "follows",
                        ),
                        "-views_per_dollar",
                        (),
                    ),
                ),
            ),
        ),
        Section(
            "Search: frames saved per CPU-hour",
            (
                charts(
                    ranked(
                        SEARCH_ALLOCATION,
                        "Frames saved per CPU-hour per worker kind",
                        "role",
                        SEARCH_PER_CPU_HOUR,
                        (),
                        (),
                    ),
                    ranked(
                        SEARCH_ALLOCATION, "Frames saved per CPU-hour per game", "game", SEARCH_PER_CPU_HOUR, (), ()
                    ),
                ),
                tables(
                    table(
                        SEARCH_ALLOCATION,
                        "Search return per game and worker kind",
                        (
                            "game",
                            "role",
                            "saved",
                            "worker_seconds",
                            "rate_mean",
                            "rate_lower",
                            "rate_upper",
                            "share",
                            "days",
                        ),
                        "game",
                        (),
                    ),
                ),
            ),
        ),
    ),
)
DASHBOARDS = (OVERVIEW, RETURN_ON_COST, SEARCH_AI, AUDIENCE, PLATFORM_COST)
