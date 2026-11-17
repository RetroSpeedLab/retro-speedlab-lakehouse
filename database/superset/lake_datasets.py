import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

SNAPSHOT = Path("/app/superset_home/lake.duckdb")
EXTENSIONS = "/app/duckdb-extensions"
NAMESPACE = uuid.UUID("5d0e4a8c-7f62-4c1b-9b8e-2f4a6d1c3e70")
LAYERS = {
    "bronze": "raw records as they were ingested",
    "silver": "cleaned and deduplicated records",
    "gold": "aggregates that serve the dashboards and the site",
    "live": "current state that the streaming job keeps up to date",
}


def stable_uuid(name: str) -> str:
    return str(uuid.uuid5(NAMESPACE, name))


@dataclass(frozen=True, slots=True)
class LakeDataset:
    schema: str
    table: str

    @property
    def uuid(self) -> str:
        return stable_uuid(f"dataset/{self.schema}.{self.table}")

    @property
    def description(self) -> str:
        return f"{self.schema.capitalize()} layer, {LAYERS[self.schema]}: the {self.schema}.{self.table} table."


def lake_datasets(listing: Mapping[str, Iterable[str]]) -> list[LakeDataset]:
    unknown = set(listing) - set(LAYERS)
    if unknown:
        raise ValueError(f"No lake layer is called {', '.join(sorted(unknown))}")
    return [LakeDataset(schema, table) for schema, tables in listing.items() for table in sorted(tables)]
