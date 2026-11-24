from retro_speedlab_lakehouse.lake.catalog import Layer, Table, table

TURN_MINUTES = table(Layer.SILVER, "search_turn_minutes", ("run", "step"))
KIND_MINUTES = table(Layer.SILVER, "search_kind_minutes", ("run", "kind", "step"))
PPO_ROUNDS = table(Layer.SILVER, "ppo_rounds", ("run", "round"))
SEARCH_TURNS = table(Layer.GOLD, "search_turns", ("game", "turn"))
SEARCH_WORKER_KINDS = table(Layer.GOLD, "search_worker_kinds", ("game", "turn", "kind"))
TRAINING_ROUNDS = table(Layer.GOLD, "training_rounds", ("game", "round"))
TRACKING_TABLES: tuple[Table, ...] = (
    TURN_MINUTES,
    KIND_MINUTES,
    PPO_ROUNDS,
    SEARCH_TURNS,
    SEARCH_WORKER_KINDS,
    TRAINING_ROUNDS,
)
