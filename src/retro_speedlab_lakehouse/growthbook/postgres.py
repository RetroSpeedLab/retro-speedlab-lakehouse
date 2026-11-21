from datetime import datetime

import psycopg

from retro_speedlab_lakehouse.settings import GrowthbookDatabase

SCHEMA = """
CREATE TABLE IF NOT EXISTS units (
    unit_id text PRIMARY KEY,
    experiment_id text NOT NULL,
    variation_id text NOT NULL,
    assigned_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS metric_events (
    unit_id text NOT NULL REFERENCES units,
    metric text NOT NULL,
    value double precision NOT NULL,
    measured_at timestamptz NOT NULL,
    PRIMARY KEY (unit_id, metric)
);
"""
COPY_UNITS = "COPY units (unit_id, experiment_id, variation_id, assigned_at) FROM STDIN"
COPY_EVENTS = "COPY metric_events (unit_id, metric, value, measured_at) FROM STDIN"
Unit = tuple[str, str, str, datetime]
Event = tuple[str, str, float, datetime]


def publish(database: GrowthbookDatabase, units: list[Unit], events: list[Event]) -> None:
    with (
        psycopg.connect(
            host=database.host,
            port=database.port,
            dbname=database.name,
            user=database.user,
            password=database.password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(SCHEMA)
        cursor.execute("TRUNCATE metric_events, units")
        with cursor.copy(COPY_UNITS) as copy:
            for unit in units:
                copy.write_row(unit)
        with cursor.copy(COPY_EVENTS) as copy:
            for event in events:
                copy.write_row(event)
