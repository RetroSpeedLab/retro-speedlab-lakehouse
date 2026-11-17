import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
from loguru import logger

SNAPSHOT = Path("/app/superset_home/lake.duckdb")
NEXT_SNAPSHOT = SNAPSHOT.with_suffix(".next")
EXTENSIONS = "/app/duckdb-extensions"
BUCKET = "s3://retro-speedlab-lake"
BRONZE = Path("/lake/bronze/bronze")
SECRET = (
    "CREATE SECRET lake (TYPE s3, KEY_ID $key_id, SECRET $secret, "
    "ENDPOINT 'lake.retrospeedlab.com', URL_STYLE 'path', REGION 'garage')"
)
SERVING_TABLES = f"SELECT \"table\" FROM delta_scan('{BUCKET}/gold/platform_tables') WHERE store = 'lake'"
COMMITS = "main.commits"
COPIES = 8
READ_THREADS = 16


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(path, config={"extension_directory": EXTENSIONS, "threads": READ_THREADS})
    secret = {"key_id": os.environ["LAKE_ACCESS_KEY_ID"], "secret": os.environ["LAKE_SECRET_ACCESS_KEY"]}
    connection.execute(SECRET, secret)
    return connection


def previous_commits(connection: duckdb.DuckDBPyConnection) -> dict[str, str]:
    if not SNAPSHOT.exists():
        return {}
    connection.execute(f"ATTACH '{SNAPSHOT}' AS previous (READ_ONLY)")
    return dict(connection.execute(f"SELECT name, commit FROM previous.{COMMITS}").fetchall())


def copy(connection: duckdb.DuckDBPyConnection, name: str, previous: dict[str, str]) -> tuple[str, str] | None:
    cursor = connection.cursor()
    location = f"{BUCKET}/{name.replace('.', '/')}"
    commit = cursor.execute("SELECT max(file) FROM glob(?)", [f"{location}/_delta_log/*.json"]).fetchall()[0][0]
    unchanged = name in previous and previous[name] == commit
    source = f"previous.{name}" if unchanged else f"delta_scan('{location}')"
    try:
        cursor.execute(f"CREATE TABLE {name} AS SELECT * FROM {source}")
    except duckdb.IOException as error:
        logger.warning(f"{name} is not in the lake: {error}")
        return None
    return name, commit


def build(connection: duckdb.DuckDBPyConnection) -> int:
    previous = previous_commits(connection)
    serving = [name for (name,) in connection.execute(SERVING_TABLES).fetchall()]
    bronze = sorted(log.parent for log in BRONZE.glob("*/_delta_log"))
    for layer in sorted({name.split(".")[0] for name in serving} | {"bronze"}):
        connection.execute(f"CREATE SCHEMA {layer}")
    with ThreadPoolExecutor(COPIES) as pool:
        commits = [commit for commit in pool.map(lambda name: copy(connection, name, previous), serving) if commit]
    for table in bronze:
        connection.execute(f"CREATE VIEW bronze.{table.name} AS SELECT * FROM delta_scan('{table}')")
    connection.execute(f"CREATE TABLE {COMMITS} (name VARCHAR, commit VARCHAR)")
    connection.executemany(f"INSERT INTO {COMMITS} VALUES (?, ?)", commits)
    return len(commits) + len(bronze)


def main() -> None:
    started = time.monotonic()
    NEXT_SNAPSHOT.unlink(missing_ok=True)
    with connect(NEXT_SNAPSHOT) as connection:
        tables = build(connection)
    NEXT_SNAPSHOT.replace(SNAPSHOT)
    logger.info(f"The lake snapshot holds {tables} tables, taken in {time.monotonic() - started:.0f} s")


if __name__ == "__main__":
    main()
