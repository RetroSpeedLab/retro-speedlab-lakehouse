from typing import Any
from uuid import UUID

from bundle import DATABASE
from lake_datasets import LAYERS, LakeDataset, lake_datasets
from loguru import logger
from sqlalchemy.exc import OperationalError
from superset.app import create_app


class DatasetSync:
    def __init__(self, session: Any, database: Any, model: Any) -> None:
        self.session = session
        self.database = database
        self.model = model
        self.skipped: list[str] = []

    def listing(self) -> dict[str, list[str]]:
        tables: dict[str, list[str]] = {}
        for schema in LAYERS:
            try:
                names = self.database.get_all_table_names_in_schema(catalog=None, schema=schema, cache=False)
            except OperationalError:
                self.skipped.append(schema)
                continue
            tables[schema] = [name for name, _, _ in names]
        return tables

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

    def sync(self, lake: LakeDataset) -> bool:
        dataset = self.dataset(lake)
        dataset.description = lake.description
        try:
            dataset.fetch_metadata()
        except OperationalError:
            self.session.rollback()
            self.skipped.append(f"{lake.schema}.{lake.table}")
            return False
        self.session.commit()
        return True

    def run(self) -> None:
        synced = [lake for lake in lake_datasets(self.listing()) if self.sync(lake)]
        logger.info(f"{len(synced)} lake datasets are in sync")
        if self.skipped:
            logger.warning(f"Not in the lake yet, skipped: {', '.join(self.skipped)}")


def main() -> None:
    with create_app().app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        database = db.session.query(Database).filter_by(database_name=DATABASE).one()
        DatasetSync(db.session, database, SqlaTable).run()


if __name__ == "__main__":
    main()
