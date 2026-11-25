from retro_speedlab_lakehouse.lake.catalog import BOOKKEEPING, Table

reads: set[str] = set()
writes: set[str] = set()


def read_of(table: Table) -> None:
    if table.full_name not in BOOKKEEPING:
        reads.add(table.full_name)


def write_of(table: Table) -> None:
    if table.full_name not in BOOKKEEPING:
        writes.add(table.full_name)


def inputs() -> tuple[str, ...]:
    return tuple(sorted(reads - writes))


def outputs() -> tuple[str, ...]:
    return tuple(sorted(writes))


def clear() -> None:
    reads.clear()
    writes.clear()
