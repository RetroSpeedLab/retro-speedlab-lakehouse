import json
from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import TIMEOUT_SECONDS
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import WandbSettings

GRAPHQL = "https://api.wandb.ai/graphql"
POLICY = json.dumps({"jobType": "policy"})
CHECKPOINTS = "model"
REWARD = "policy/reward"
QUERY = """query Training($entity: String!, $project: String!, $filters: JSONString!, $checkpoints: String!) {
  project(name: $project, entityName: $entity) {
    runs(first: 1, order: "-created_at", filters: $filters) { totalCount edges { node { config summaryMetrics } } }
    artifactType(name: $checkpoints) { artifactCollections { edges { node { artifacts { totalCount } } } } }
  }
}"""


def training(settings: WandbSettings) -> dict[str, Any]:
    entity, project = settings.project.split("/")
    variables = {"entity": entity, "project": project, "filters": POLICY, "checkpoints": CHECKPOINTS}
    response = httpx.post(
        GRAPHQL, json={"query": QUERY, "variables": variables}, auth=("api", settings.api_key), timeout=TIMEOUT_SECONDS
    )
    response.raise_for_status()
    payload = response.json()
    if "errors" in payload:
        raise ValueError(f"W&B refused the query: {payload['errors']}")
    return payload["data"]["project"]


def latest_reward(runs: dict[str, Any]) -> list[Number]:
    if not runs["edges"]:
        return []
    newest = runs["edges"][0]["node"]
    summary = json.loads(newest["summaryMetrics"])
    if REWARD not in summary:
        return []
    game = json.loads(newest["config"])["game"]["value"]
    return [Number("latest_reward", "Latest PPO reward", round(summary[REWARD], 1), "", f"per episode, {game}")]


def checkpoints(artifact_type: dict[str, Any] | None) -> list[Number]:
    if artifact_type is None:
        return []
    collections = artifact_type["artifactCollections"]["edges"]
    versions = sum(collection["node"]["artifacts"]["totalCount"] for collection in collections)
    return [Number("checkpoints", "Policy checkpoints", versions, "", "versioned in W&B")]


def wandb_numbers(settings: WandbSettings) -> list[Number]:
    project = training(settings)
    runs = project["runs"]
    trained = Number("policy_runs", "PPO training runs", runs["totalCount"], "", "all time")
    return [trained, *latest_reward(runs), *checkpoints(project["artifactType"])]
