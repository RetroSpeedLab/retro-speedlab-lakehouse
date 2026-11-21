from typing import Any, TypeGuard

import httpx
from loguru import logger
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_exponential

TIMEOUT_SECONDS = 30.0
ATTEMPTS = 4
LONGEST_RETRY_AFTER_SECONDS = 60.0
RETRY_AFTER = "Retry-After"
TOO_MANY_REQUESTS = 429
backoff = wait_exponential(multiplier=2)


def client(base_url: str, headers: dict[str, str]) -> httpx.Client:
    return httpx.Client(base_url=base_url, headers=headers, timeout=TIMEOUT_SECONDS)


def rate_limited(error: BaseException) -> TypeGuard[httpx.HTTPStatusError]:
    return isinstance(error, httpx.HTTPStatusError) and error.response.status_code == TOO_MANY_REQUESTS


def asked_wait(error: BaseException) -> float:
    if not rate_limited(error) or RETRY_AFTER not in error.response.headers:
        return 0.0
    return float(error.response.headers[RETRY_AFTER])


def transient(error: BaseException) -> bool:
    if isinstance(error, httpx.TransportError):
        return True
    return rate_limited(error) and asked_wait(error) <= LONGEST_RETRY_AFTER_SECONDS


def failure(state: RetryCallState) -> BaseException:
    error = None if state.outcome is None else state.outcome.exception()
    if error is None:
        raise RuntimeError("tenacity retried a call that did not fail")
    return error


def waited(state: RetryCallState) -> float:
    return max(asked_wait(failure(state)), backoff(state))


def warn(state: RetryCallState) -> None:
    logger.warning(f"Retrying in {state.upcoming_sleep:.0f} s after {failure(state)!r}")


@retry(
    retry=retry_if_exception(transient),
    stop=stop_after_attempt(ATTEMPTS),
    wait=waited,
    before_sleep=warn,
    reraise=True,
)
def sent(
    session: httpx.Client, method: str, path: str, params: dict[str, Any], body: dict[str, Any] | list[Any] | None
) -> httpx.Response:
    response = session.request(method, path, params=params, json=body)
    response.raise_for_status()
    return response


def get_json(session: httpx.Client, path: str, params: dict[str, Any]) -> Any:
    return sent(session, "GET", path, params, None).json()
