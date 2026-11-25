from retro_speedlab_lakehouse.lake.catalog import Layer, Table, table

ENGAGEMENT_EVENTS = table(Layer.SILVER, "engagement_events", ("surface", "item", "arm", "hour"))
COMMUNITY_DAILY = table(Layer.SILVER, "community_daily", ("date",))
CONTENT_RANKING = table(Layer.GOLD, "content_ranking", ("surface", "item"))
ENGAGEMENT_EXPERIMENTS = table(Layer.GOLD, "engagement_experiments", ("experiment", "metric"))
EXPERIMENT_PLAN = table(Layer.GOLD, "experiment_plan", ("experiment",))
TWITCH_CHANNEL_OUTCOMES = table(Layer.GOLD, "twitch_channel_outcomes", ("item", "started_at"))
ENGAGEMENT_TABLES: tuple[Table, ...] = (
    ENGAGEMENT_EVENTS,
    COMMUNITY_DAILY,
    CONTENT_RANKING,
    ENGAGEMENT_EXPERIMENTS,
    EXPERIMENT_PLAN,
    TWITCH_CHANNEL_OUTCOMES,
)
