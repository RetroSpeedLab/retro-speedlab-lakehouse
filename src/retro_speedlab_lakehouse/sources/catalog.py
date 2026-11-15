from retro_speedlab_lakehouse.lake.catalog import Layer, Table, table

SILVER_NUMBERS = table(Layer.SILVER, "source_numbers", ("source", "metric"))
SOURCE_NUMBERS = table(Layer.GOLD, "source_numbers", ("source", "metric"))
SOURCE_TABLES: tuple[Table, ...] = (SILVER_NUMBERS, SOURCE_NUMBERS)
