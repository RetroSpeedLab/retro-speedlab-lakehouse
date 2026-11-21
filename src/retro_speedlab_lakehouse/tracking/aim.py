import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import get_json, sent

SYSTEM = "__system__"
PAGE = 100


@dataclass(frozen=True, slots=True)
class AimRun:
    run: str
    experiment: str
    name: str
    params: str
    created_at: datetime
    ended_at: datetime | None


@dataclass(frozen=True, slots=True)
class AimPoint:
    run: str
    metric: str
    context: str
    step: int
    value: float


def moment(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, tz=UTC)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def experiments(session: httpx.Client) -> list[str]:
    return [experiment["id"] for experiment in get_json(session, "/api/experiments/", {})]


def created_since(session: httpx.Client, experiment: str, since: datetime | None) -> list[str]:
    hashes: list[str] = []
    offset: str | None = None
    while True:
        params = {"limit": PAGE} if offset is None else {"limit": PAGE, "offset": offset}
        page = get_json(session, f"/api/experiments/{experiment}/runs/", params)["runs"]
        kept = [run for run in page if since is None or moment(run["creation_time"]) >= since]
        hashes += [run["run_id"] for run in kept]
        if len(page) < PAGE or len(kept) < len(page) or page[-1]["run_id"] == offset:
            return hashes
        offset = page[-1]["run_id"]


def listed_runs(session: httpx.Client, since: datetime | None) -> list[str]:
    return [run for experiment in experiments(session) for run in created_since(session, experiment, since)]


def run_of(run: str, info: dict[str, Any]) -> AimRun:
    props = info["props"]
    ended_at = None if props["end_time"] is None else moment(props["end_time"])
    experiment = props["experiment"]["id"]
    return AimRun(run, experiment, props["name"], canonical(info["params"]), moment(props["creation_time"]), ended_at)


def points_of(run: str, traces: list[dict[str, Any]]) -> list[AimPoint]:
    return [
        AimPoint(run, trace["name"], canonical(trace["context"]), int(step), float(value))
        for trace in traces
        for step, value in zip(trace["iters"], trace["values"], strict=True)
        if value is not None
    ]


def fetched_run(session: httpx.Client, run: str) -> tuple[AimRun, list[AimPoint]]:
    info = get_json(session, f"/api/runs/{run}/info/", {"sequence": "metric"})
    wanted = [
        {"name": trace["name"], "context": trace["context"]}
        for trace in info["traces"]["metric"]
        if not trace["name"].startswith(SYSTEM)
    ]
    if not wanted:
        return run_of(run, info), []
    # shortcut: Aim's REST batch samples a metric to 200 steps across the run; read the SDK if every step must land
    traces = sent(session, "POST", f"/api/runs/{run}/metric/get-batch/", {}, wanted).json()
    return run_of(run, info), points_of(run, traces)
