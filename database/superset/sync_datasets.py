from typing import Any
from uuid import UUID

import duckdb
from bundle import DATABASE
from lake_datasets import LAYERS, SNAPSHOT, LakeDataset, lake_datasets
from loguru import logger
from superset.app import create_app

LAKE_TABLES = "SELECT table_schema, table_name FROM information_schema.tables WHERE list_contains(?, table_schema)"


def listing() -> dict[str, list[str]]:
    with duckdb.connect(SNAPSHOT, read_only=True) as connection:
        rows = connection.execute(LAKE_TABLES, [list(LAYERS)]).fetchall()
    tables: dict[str, list[str]] = {}
    for schema, table in rows:
        tables.setdefault(schema, []).append(table)
    return tables


class DatasetSync:
    def __init__(self, session: Any, database: Any, model: Any) -> None:
        self.session = session
        self.database = database
        self.model = model

    def dataset(self, lake: LakeDataset) -> Any:
        existing = (
            self.session.query(self.model)
            .filter_by(database_id=self.database.id, schema=lake.schema, table_name=lake.table)
            .one_or_none()
        )
        if existing is not None:
            return existing
        created = self.model(table_name=lake.table, schema=lake.schema, database=self.database, uuid=UUID(lake.uuid))
        self.session.add(created)
        return created

    def sync(self, lake: LakeDataset) -> None:
        dataset = self.dataset(lake)
        dataset.description = lake.description
        dataset.fetch_metadata()
        self.session.commit()

    def run(self) -> None:
        datasets = lake_datasets(listing())
        for lake in datasets:
            self.sync(lake)
        logger.info(f"{len(datasets)} lake datasets are in sync")


def main() -> None:
    with create_app().app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        database = db.session.query(Database).filter_by(database_name=DATABASE).one()
        DatasetSync(db.session, database, SqlaTable).run()


if __name__ == "__main__":
    main()
