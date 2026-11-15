from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

TIMEOUT_SECONDS = 30.0


def client(base_url: str, headers: dict[str, str]) -> httpx.Client:
    return httpx.Client(base_url=base_url, headers=headers, timeout=TIMEOUT_SECONDS)


@retry(
    retry=retry_if_exception_type(httpx.TransportError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2),
    reraise=True,
)
def sent(
    session: httpx.Client, method: str, path: str, params: dict[str, Any], body: dict[str, Any] | None
) -> httpx.Response:
    response = session.request(method, path, params=params, json=body)
    response.raise_for_status()
    return response


def get_json(session: httpx.Client, path: str, params: dict[str, Any]) -> Any:
    return sent(session, "GET", path, params, None).json()
