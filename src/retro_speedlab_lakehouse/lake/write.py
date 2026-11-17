from delta.tables import DeltaMergeBuilder, DeltaTable
from pyspark.sql import DataFrame

from retro_speedlab_lakehouse.lake.catalog import Table
from retro_speedlab_lakehouse.lake.tables import append_rows, ensure
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings

EVENT_ID = "event_id"


def matching(table: Table) -> str:
    return " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in table.keys)


def merging(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate) -> DeltaMergeBuilder:
    gate.check(table, rows)
    ensure(table, lake, rows)
    return (
        DeltaTable.forPath(rows.sparkSession, table.location(lake))
        .alias("target")
        .merge(rows.alias("source"), matching(table))
    )


def append(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate) -> None:
    gate.check(table, rows)
    append_rows(table, lake, rows)


def merge(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate) -> None:
    merging(table, lake, rows, gate).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def insert_new(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate) -> None:
    merging(table, lake, rows, gate).whenNotMatchedInsertAll().execute()


def merge_newer(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate) -> None:
    newer = f"source.`{EVENT_ID}` > target.`{EVENT_ID}`"
    merging(table, lake, rows, gate).whenMatchedUpdateAll(condition=newer).whenNotMatchedInsertAll().execute()


def overwrite_games(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate, games: list[str]) -> None:
    gate.check(table, rows)
    ensure(table, lake, rows)
    chosen = ", ".join(f"'{game}'" for game in games)
    writer = rows.write.format("delta").mode("overwrite").option("replaceWhere", f"game IN ({chosen})")
    writer.save(table.location(lake))


def overwrite(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate) -> None:
    gate.check(table, rows)
    ensure(table, lake, rows)
    rows.write.format("delta").mode("overwrite").save(table.location(lake))


def replace(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate) -> None:
    builder = merging(table, lake, rows, gate).whenMatchedUpdateAll().whenNotMatchedInsertAll()
    builder.whenNotMatchedBySourceDelete().execute()


def replace_counted(table: Table, lake: LakeSettings, rows: DataFrame, gate: Gate) -> int:
    kept = rows.persist()
    replace(table, lake, kept, gate)
    count = kept.count()
    kept.unpersist()
    return count
