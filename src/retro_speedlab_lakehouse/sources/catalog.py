from retro_speedlab_lakehouse.lake.catalog import Layer, Table, table

SILVER_NUMBERS = table(Layer.SILVER, "source_numbers", ("source", "metric"))
SOURCE_NUMBERS = table(Layer.GOLD, "source_numbers", ("source", "metric"))
SILVER_PLAYS = table(Layer.SILVER, "game_plays", ("mode", "window"))
GAME_PLAYS = table(Layer.GOLD, "game_plays", ("game", "mode", "window"))
SOURCE_TABLES: tuple[Table, ...] = (SILVER_NUMBERS, SOURCE_NUMBERS, SILVER_PLAYS, GAME_PLAYS)
