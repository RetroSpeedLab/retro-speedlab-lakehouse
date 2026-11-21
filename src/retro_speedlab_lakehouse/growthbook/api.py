from datetime import datetime
from typing import Any

import httpx
from loguru import logger

from retro_speedlab_lakehouse.growthbook.units import Experiment
from retro_speedlab_lakehouse.settings import GrowthbookSettings

TIMEOUT = 30.0
PHASE = "Main"
TAGS = ["lake"]


def experiment_body(experiment: Experiment, growthbook: GrowthbookSettings, started_at: datetime) -> dict[str, Any]:
    split = [
        {"variationId": key, "weight": weight}
        for key, weight in zip(experiment.variations, experiment.weights, strict=True)
    ]
    return {
        "trackingKey": experiment.key,
        "name": experiment.name,
        "hypothesis": experiment.hypothesis,
        "datasourceId": growthbook.datasource,
        "assignmentQuery": {"id": growthbook.assignment_query, "identifierType": growthbook.identifier},
        "variations": [{"id": key, "key": key, "name": key} for key in experiment.variations],
        "phases": [{"name": PHASE, "dateStarted": started_at.isoformat(), "trafficSplit": split}],
        "metrics": list(experiment.metrics),
        "status": "running",
        "autoRefresh": True,
        "statsEngine": "frequentist",
        "attributionModel": "experimentDuration",
        "tags": TAGS,
    }


def experiment_id(client: httpx.Client, body: dict[str, Any]) -> str:
    found = client.get("/experiments", params={"trackingKey": body["trackingKey"]}).raise_for_status().json()
    if found["experiments"]:
        return found["experiments"][0]["id"]
    created = client.post("/experiments", json=body).raise_for_status().json()
    logger.info(f"GrowthBook: created the experiment {body['trackingKey']}")
    return created["experiment"]["id"]


def sync_experiments(growthbook: GrowthbookSettings, experiments: list[Experiment], starts: dict[str, datetime]) -> int:
    headers = {"Authorization": f"Bearer {growthbook.api_key}"}
    with httpx.Client(base_url=f"{growthbook.api}/api/v1", headers=headers, timeout=TIMEOUT) as client:
        started = [experiment for experiment in experiments if experiment.key in starts]
        for experiment in started:
            found = experiment_id(client, experiment_body(experiment, growthbook, starts[experiment.key]))
            client.post(f"/experiments/{found}/snapshot", json={}).raise_for_status()
    return len(started)
