from lake_datasets import LAYERS, lake_datasets

from retro_speedlab_lakehouse.batch.jobs import ALL_TABLES
from retro_speedlab_lakehouse.lake.catalog import Layer


def test_every_catalog_table_gets_one_described_dataset() -> None:
    listing = {layer.value: [table.name for table in ALL_TABLES if table.layer is layer] for layer in Layer}
    datasets = lake_datasets(listing)
    assert set(LAYERS) == {layer.value for layer in Layer}
    assert sorted(f"{dataset.schema}.{dataset.table}" for dataset in datasets) == sorted(
        table.full_name for table in ALL_TABLES
    )
    assert len({dataset.uuid for dataset in datasets}) == len(ALL_TABLES)
    assert all(f"{dataset.schema}.{dataset.table}" in dataset.description for dataset in datasets)
