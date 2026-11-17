import os
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from pathlib import Path

import duckdb
from bundle import cockpit_datasets
from charts import Columns, Dataset
from lake_datasets import EXTENSIONS, SNAPSHOT
from loguru import logger
from sqlglot import exp, parse_one

NEXT_SNAPSHOT = SNAPSHOT.with_suffix(".next")
BUCKET = "s3://retro-speedlab-lake"
BRONZE = Path("/lake/bronze/bronze")
SECRET = (
    "CREATE SECRET lake (TYPE s3, KEY_ID $key_id, SECRET $secret, "
    "ENDPOINT 'lake.retrospeedlab.com', URL_STYLE 'path', REGION 'garage')"
)
SERVING_TABLES = f"SELECT \"table\" FROM delta_scan('{BUCKET}/gold/platform_tables') WHERE store = 'lake'"
COMMITS = "main.commits"
MEMORY_LIMIT = "768MB"
SPILL = "/tmp/lake-snapshot"
READ_THREADS = 8
COPIES = 4
REFRESH_SECONDS = 30


def connect() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(
        config={
            "extension_directory": EXTENSIONS,
            "memory_limit": MEMORY_LIMIT,
            "temp_directory": SPILL,
            "threads": READ_THREADS,
            "enable_external_file_cache": False,
            "preserve_insertion_order": False,
        }
    )
    secret = {"key_id": os.environ["LAKE_ACCESS_KEY_ID"], "secret": os.environ["LAKE_SECRET_ACCESS_KEY"]}
    connection.execute(SECRET, secret)
    return connection


def referenced(dataset: Dataset) -> set[str]:
    if dataset.sql is None:
        return {f"{dataset.schema}.{dataset.name}"}
    return {f"{table.db}.{table.name}" for table in parse_one(dataset.sql, read="duckdb").find_all(exp.Table)}


def declared_tables() -> dict[str, Columns]:
    datasets = cockpit_datasets().values()
    return {f"{dataset.schema}.{dataset.name}": dataset.columns for dataset in datasets if dataset.sql is None}


def serving_tables(connection: duckdb.DuckDBPyConnection) -> list[str]:
    listed = {name for (name,) in connection.execute(SERVING_TABLES).fetchall()}
    return sorted(listed.union(*(referenced(dataset) for dataset in cockpit_datasets().values())))


def create_empty(cursor: duckdb.DuckDBPyConnection, name: str, columns: Columns) -> None:
    definition = ", ".join(f'"{column}" {kind}' for column, kind in columns)
    cursor.execute(f"CREATE TABLE snapshot.{name} ({definition})")


def previous_commits(connection: duckdb.DuckDBPyConnection) -> dict[str, str]:
    if not SNAPSHOT.exists():
        return {}
    connection.execute(f"ATTACH '{SNAPSHOT}' AS previous (READ_ONLY)")
    return dict(connection.execute(f"SELECT name, commit FROM previous.{COMMITS}").fetchall())


def copy(
    connection: duckdb.DuckDBPyConnection, name: str, previous: dict[str, str], declared: dict[str, Columns]
) -> tuple[str, str] | None:
    cursor = connection.cursor()
    location = f"{BUCKET}/{name.replace('.', '/')}"
    commit = cursor.execute("SELECT max(file) FROM glob(?)", [f"{location}/_delta_log/*.json"]).fetchall()[0][0]
    if commit is None:
        if name in declared:
            create_empty(cursor, name, declared[name])
        return None
    unchanged = name in previous and previous[name] == commit
    source = f"previous.{name}" if unchanged else f"delta_scan('{location}')"
    cursor.execute(f"CREATE TABLE snapshot.{name} AS SELECT * FROM {source}")
    return name, commit


def snapshot(connection: duckdb.DuckDBPyConnection, previous: dict[str, str]) -> int:
    serving = serving_tables(connection)
    bronze = sorted(log.parent for log in BRONZE.glob("*/_delta_log"))
    for layer in sorted({name.split(".")[0] for name in serving} | {"bronze"}):
        connection.execute(f"CREATE SCHEMA snapshot.{layer}")
    with ThreadPoolExecutor(COPIES) as pool:
        copies = pool.map(copy, repeat(connection), serving, repeat(previous), repeat(declared_tables()))
        commits = [commit for commit in copies if commit]
    for table in bronze:
        connection.execute(f"CREATE VIEW snapshot.bronze.{table.name} AS SELECT * FROM delta_scan('{table}')")
    connection.execute(f"CREATE TABLE snapshot.{COMMITS} (name VARCHAR, commit VARCHAR)")
    connection.executemany(f"INSERT INTO snapshot.{COMMITS} VALUES (?, ?)", commits)
    return len(commits) + len(bronze)


def refresh(connection: duckdb.DuckDBPyConnection) -> None:
    started = time.monotonic()
    NEXT_SNAPSHOT.unlink(missing_ok=True)
    previous = previous_commits(connection)
    connection.execute(f"ATTACH '{NEXT_SNAPSHOT}' AS snapshot")
    tables = snapshot(connection, previous)
    connection.execute("DETACH snapshot")
    connection.execute("DETACH DATABASE IF EXISTS previous")
    NEXT_SNAPSHOT.replace(SNAPSHOT)
    logger.info(f"The lake snapshot holds {tables} tables, taken in {time.monotonic() - started:.0f} s")


def main() -> None:
    with connect() as connection:
        while True:
            refresh(connection)
            time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()
