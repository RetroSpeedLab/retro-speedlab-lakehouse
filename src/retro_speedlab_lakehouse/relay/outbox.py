from datetime import datetime
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

EVENTS_PATH = "/lakehouse/events"


class OutboxEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: int
    topic: str
    key: str
    at: datetime
    payload: dict[str, Any]


class OutboxPage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    events: list[OutboxEvent]


def backend_client(url: str, api_key: str, timeout_seconds: float) -> httpx.Client:
    return httpx.Client(base_url=url, headers={"X-API-Key": api_key}, timeout=timeout_seconds)


def events_after(client: httpx.Client, cursor: int, limit: int) -> list[OutboxEvent]:
    response = client.get(EVENTS_PATH, params={"after": cursor, "limit": limit})
    response.raise_for_status()
    return OutboxPage.model_validate(response.json()).events
