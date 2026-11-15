from pathlib import Path

MEMINFO = Path("/proc/meminfo")
AVAILABLE = "MemAvailable:"
KIB_PER_MIB = 1024


def available_mb() -> int:
    for line in MEMINFO.read_text(encoding="utf-8").splitlines():
        if line.startswith(AVAILABLE):
            return int(line.split()[1]) // KIB_PER_MIB
    raise RuntimeError(f"{MEMINFO} names no {AVAILABLE}")


def ensure_headroom(minimum_mb: int) -> None:
    available = available_mb()
    if available < minimum_mb:
        raise RuntimeError(f"Only {available} MB are available; mining starts with at least {minimum_mb} MB")
