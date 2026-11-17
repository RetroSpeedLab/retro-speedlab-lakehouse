from retro_speedlab_lakehouse.lake.catalog import Layer, Table, table

COCKPIT_DAYS = table(Layer.GOLD, "cockpit_days", ("metric", "days_ago"))
SEARCH_HOURS = table(Layer.GOLD, "search_hours", ("hour", "game"))
AUDIENCE_HOURS = table(Layer.GOLD, "audience_hours", ("hour",))
COCKPIT_FINDINGS = table(Layer.GOLD, "cockpit_findings", ("area", "subject"))
COCKPIT_TABLES: tuple[Table, ...] = (COCKPIT_DAYS, SEARCH_HOURS, AUDIENCE_HOURS, COCKPIT_FINDINGS)
