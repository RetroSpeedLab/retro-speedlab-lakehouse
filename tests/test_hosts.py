from datetime import UTC, datetime
from pathlib import Path

import pytest
from collector import gpu, measured, mounted

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)


def test_the_collector_measures_every_mounted_disk(tmp_path: Path) -> None:
    (tmp_path / "root").mkdir()
    (tmp_path / "data").mkdir()
    metrics = measured(mounted(tmp_path), "none", NOW)
    assert sorted(metrics["disks"]) == ["data", "root"]
    assert 0 <= metrics["disks"]["root"]["percent"] <= 100
    assert 0 <= metrics["memory_percent"] <= 100
    assert metrics["cpus"] >= 1
    assert metrics["measured_at"] == NOW.isoformat()
    assert 0 <= metrics["cpu_percent"] <= 100
    assert metrics["gpus"] == []


def test_the_collector_reads_a_gpu_from_nvidia_smi() -> None:
    assert gpu("0, NVIDIA GeForce RTX 2070, 6, 178, 8192, 49, 34.10") == {
        "index": 0,
        "name": "NVIDIA GeForce RTX 2070",
        "utilization_percent": 6.0,
        "memory_used_bytes": 178 * 1024 * 1024,
        "memory_total_bytes": 8192 * 1024 * 1024,
        "temperature_celsius": 49.0,
        "power_watts": 34.1,
    }


def test_the_collector_needs_a_disk(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError):
        mounted(tmp_path)
