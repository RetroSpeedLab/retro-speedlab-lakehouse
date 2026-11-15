from pathlib import Path


def read_cursor(path: Path) -> int:
    return int(path.read_text(encoding="utf-8").strip())


def write_cursor(path: Path, cursor: int) -> None:
    staged = path.with_suffix(".tmp")
    staged.write_text(f"{cursor}\n", encoding="utf-8")
    staged.replace(path)
