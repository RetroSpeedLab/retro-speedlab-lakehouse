import re
from datetime import datetime, timedelta
from http import HTTPStatus

import httpx

from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import GithubSettings

API = "https://api.github.com"
LAST_PAGE = re.compile(r'[?&]page=(\d+)>; rel="last"')
WEEK = timedelta(days=7)
EMPTY = HTTPStatus.CONFLICT
PUBLIC = "public repositories"


def commits_since(session: httpx.Client, repository: str, since: datetime) -> int:
    response = session.get(f"/repos/{repository}/commits", params={"since": since.isoformat(), "per_page": 1})
    if response.status_code == EMPTY:
        return 0
    response.raise_for_status()
    match = LAST_PAGE.search(response.headers["link"]) if "link" in response.headers else None
    return int(match.group(1)) if match else len(response.json())


def github_numbers(settings: GithubSettings, now: datetime) -> list[Number]:
    with client(API, {"Authorization": f"Bearer {settings.token}"}) as session:
        details = [get_json(session, f"/repos/{repository}", {}) for repository in settings.repositories]
        public = [repository for repository in details if not repository["private"]]
        commits = sum(commits_since(session, repository, now - WEEK) for repository in settings.repositories)
    return [
        Number("repositories", "Repositories", len(settings.repositories), "", f"{len(public)} of them public"),
        Number("commits_7d", "Commits", commits, "", "last 7 days, every repository"),
        Number("stars", "Stars", sum(repository["stargazers_count"] for repository in public), "", PUBLIC),
        Number("forks", "Forks", sum(repository["forks_count"] for repository in public), "", PUBLIC),
    ]
