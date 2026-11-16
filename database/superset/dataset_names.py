from alembic.migration import MigrationContext
from alembic.operations import Operations
from loguru import logger
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from superset.app import create_app

DATASETS = "tables"
NAME_ONLY = ["table_name"]
NAMING = {"uq": "uq_%(table_name)s_%(column_0_name)s"}
NAME_ONLY_CONSTRAINT = "uq_tables_table_name"


def unique_by_name_only(engine: Engine) -> bool:
    return any(
        constraint["column_names"] == NAME_ONLY for constraint in inspect(engine).get_unique_constraints(DATASETS)
    )


def allow_one_name_per_schema(engine: Engine) -> None:
    if not unique_by_name_only(engine):
        return
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        with connection.begin():
            operations = Operations(MigrationContext.configure(connection))
            with operations.batch_alter_table(DATASETS, naming_convention=NAMING) as batch:
                batch.drop_constraint(NAME_ONLY_CONSTRAINT, type_="unique")
            violations = connection.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise RuntimeError(f"Dropping the name-only constraint broke {len(violations)} foreign keys")
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
    logger.info("Datasets in different schemas may now share a table name")


def main() -> None:
    with create_app().app_context():
        from superset import db

        allow_one_name_per_schema(db.engine)


if __name__ == "__main__":
    main()
