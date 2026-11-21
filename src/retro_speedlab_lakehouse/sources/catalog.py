from retro_speedlab_lakehouse.lake.catalog import Layer, Table, table

SILVER_NUMBERS = table(Layer.SILVER, "source_numbers", ("source", "metric"))
SOURCE_NUMBERS = table(Layer.GOLD, "source_numbers", ("source", "metric"))
SILVER_PLAYS = table(Layer.SILVER, "game_plays", ("mode", "window"))
GAME_PLAYS = table(Layer.GOLD, "game_plays", ("game", "mode", "window"))
SILVER_ITCH_PLAYS = table(Layer.SILVER, "itch_plays", ("mode", "window"))
ITCH_PLAYS = table(Layer.GOLD, "itch_plays", ("game", "mode", "window"))
SILVER_ITCH_DAILY = table(Layer.SILVER, "itch_daily", ("date",))
ITCH_DAILY = table(Layer.GOLD, "itch_daily", ("date", "game"))
SILVER_DROPOFF = table(Layer.SILVER, "game_dropoff", ("game",))
HUMAN_DROPOFF = table(Layer.GOLD, "human_dropoff", ("game", "level_index"))
SILVER_LAB_COSTS = table(Layer.SILVER, "lab_costs", ("stamp",))
TRACE_DAILY = table(Layer.SILVER, "trace_daily", ("date",))
SILVER_LAB_ROUNDS = table(Layer.SILVER, "lab_rounds", ("stamp", "round"))
LAB_RUN_ROUNDS = table(Layer.GOLD, "lab_run_rounds", ("stamp", "round"))
LAB_RUN_TRACES = table(Layer.GOLD, "lab_run_traces", ("game", "stamp"))
LAB_TRACE_DAILY = table(Layer.GOLD, "lab_trace_daily", ("date", "game"))
SILVER_BLUESKY_POSTS = table(Layer.SILVER, "bluesky_posts", ("uri", "measured_at"))
BLUESKY_POSTS = table(Layer.GOLD, "bluesky_posts", ("uri",))
YOUTUBE_DAILY = table(Layer.SILVER, "youtube_daily", ("date",))
SOURCE_TABLES: tuple[Table, ...] = (
    SILVER_NUMBERS,
    SOURCE_NUMBERS,
    SILVER_PLAYS,
    GAME_PLAYS,
    SILVER_ITCH_PLAYS,
    ITCH_PLAYS,
    SILVER_ITCH_DAILY,
    ITCH_DAILY,
    SILVER_DROPOFF,
    HUMAN_DROPOFF,
    SILVER_LAB_COSTS,
    TRACE_DAILY,
    SILVER_LAB_ROUNDS,
    LAB_RUN_ROUNDS,
    LAB_RUN_TRACES,
    LAB_TRACE_DAILY,
    SILVER_BLUESKY_POSTS,
    BLUESKY_POSTS,
    YOUTUBE_DAILY,
)
