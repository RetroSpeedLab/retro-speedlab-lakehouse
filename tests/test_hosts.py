from datetime import UTC, datetime
from pathlib import Path

import pytest
from collector import measured, mounted

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)


def test_the_collector_measures_every_mounted_disk(tmp_path: Path) -> None:
    (tmp_path / "root").mkdir()
    (tmp_path / "data").mkdir()
    metrics = measured(mounted(tmp_path), NOW)
    assert sorted(metrics["disks"]) == ["data", "root"]
    assert 0 <= metrics["disks"]["root"]["percent"] <= 100
    assert 0 <= metrics["memory_percent"] <= 100
    assert metrics["cpus"] >= 1
    assert metrics["measured_at"] == NOW.isoformat()


def test_the_collector_needs_a_disk(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError):
        mounted(tmp_path)
