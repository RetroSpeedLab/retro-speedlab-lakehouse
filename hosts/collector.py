import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import psutil
from loguru import logger

DISKS = Path("/disks")
INTERVAL_SECONDS = 60
TIMEOUT_SECONDS = 15.0


def environment(name: str) -> str:
    if name not in os.environ:
        raise RuntimeError(f"Set {name}")
    return os.environ[name]


def disk(path: Path) -> dict[str, Any]:
    usage = psutil.disk_usage(str(path))
    return {"percent": usage.percent, "used_bytes": usage.used, "total_bytes": usage.total}


def mounted(disks: Path) -> list[Path]:
    found = sorted(path for path in disks.iterdir() if path.is_dir())
    if not found:
        raise RuntimeError(f"Mount at least one disk under {disks}")
    return found


def measured(disks: list[Path], now: datetime) -> dict[str, Any]:
    memory = psutil.virtual_memory()
    load_1, load_5, load_15 = psutil.getloadavg()
    cpus = psutil.cpu_count()
    if cpus is None:
        raise RuntimeError("The number of CPUs is unknown")
    return {
        "measured_at": now.isoformat(),
        "disks": {path.name: disk(path) for path in disks},
        "memory_percent": memory.percent,
        "memory_used_bytes": memory.total - memory.available,
        "memory_total_bytes": memory.total,
        "load_1": load_1,
        "load_5": load_5,
        "load_15": load_15,
        "cpus": cpus,
    }


def report(client: httpx.Client, host: str, metrics: dict[str, Any]) -> None:
    try:
        client.put(f"/hosts/{host}", json=metrics).raise_for_status()
    except httpx.HTTPError as error:
        logger.warning(f"The backend did not take the metrics of {host}: {error!r}")


def main() -> None:
    host, disks = environment("HOST_NAME"), mounted(DISKS)
    headers = {"X-API-Key": environment("UPLOAD_API_KEY")}
    logger.info(f"Reporting {host} with the disks {', '.join(path.name for path in disks)}")
    with httpx.Client(base_url=environment("BACKEND_URL"), headers=headers, timeout=TIMEOUT_SECONDS) as client:
        while True:
            report(client, host, measured(disks, datetime.now(UTC)))
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
