from typing import LiteralString

import psycopg
from psycopg import sql
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    TimestampType,
)

from retro_speedlab_lakehouse.settings import PostgresSettings

COLUMN_TYPES: dict[type[DataType], LiteralString] = {
    StringType: "text",
    IntegerType: "integer",
    LongType: "bigint",
    DoubleType: "double precision",
    BooleanType: "boolean",
    TimestampType: "timestamptz",
    DateType: "date",
}
UTC_SESSION = "-c timezone=UTC"


def column(field: StructField) -> sql.Composable:
    kind = type(field.dataType)
    if kind not in COLUMN_TYPES:
        raise ValueError(f"the column {field.name} has the type {field.dataType}, which Postgres is not given")
    return sql.SQL("{} {}").format(sql.Identifier(field.name), sql.SQL(COLUMN_TYPES[kind]))


def replace_table(cursor: psycopg.Cursor[tuple[object, ...]], name: str, rows: DataFrame) -> int:
    table = sql.Identifier(name)
    columns = sql.SQL(", ").join(column(field) for field in rows.schema.fields)
    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
    cursor.execute(sql.SQL("CREATE TABLE {} ({})").format(table, columns))
    collected = rows.collect()
    with cursor.copy(sql.SQL("COPY {} FROM STDIN").format(table)) as copy:
        for row in collected:
            copy.write_row([row[name] for name in rows.columns])
    return len(collected)


def publish(database: PostgresSettings, tables: dict[str, DataFrame]) -> int:
    with (
        psycopg.connect(
            host=database.host,
            port=database.port,
            dbname=database.name,
            user=database.user,
            password=database.password,
            options=UTC_SESSION,
        ) as connection,
        connection.cursor() as cursor,
    ):
        return sum(replace_table(cursor, name, rows) for name, rows in tables.items())
