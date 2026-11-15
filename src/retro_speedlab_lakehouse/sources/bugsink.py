from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx

from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import BugsinkSettings


def every_result(session: httpx.Client, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    cursor: dict[str, str] = {}
    while True:
        page = get_json(session, path, {**params, **cursor})
        results.extend(page["results"])
        following: str | None = page["next"]
        if following is None:
            return results
        cursor = {"cursor": parse_qs(urlparse(following).query)["cursor"][0]}


def bugsink_numbers(settings: BugsinkSettings) -> list[Number]:
    with client(settings.url, {"Authorization": f"Bearer {settings.token}"}) as session:
        projects = every_result(session, "/projects/", {})
        issues = [every_result(session, "/issues/", {"project": project["id"]}) for project in projects]
    resolved = sum(issue["is_resolved"] for listed in issues for issue in listed)
    total = sum(len(listed) for listed in issues)
    return [
        Number("projects", "Projects", len(projects), "", "reporting errors"),
        Number("open_issues", "Open issues", total - resolved, "", "now"),
        Number("resolved_issues", "Resolved issues", resolved, "", "all time"),
        Number("events", "Error events", sum(project["digested_event_count"] for project in projects), "", "all time"),
    ]
