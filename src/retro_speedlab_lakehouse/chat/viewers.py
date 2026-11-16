from dataclasses import dataclass
from datetime import datetime

import httpx
from loguru import logger

from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.settings import TwitchSettings
from retro_speedlab_lakehouse.sources.twitch import HELIX, app_token

TOPIC = "twitch.minutes"


@dataclass(frozen=True, slots=True)
class Helix:
    headers: dict[str, str]
    user: str


def connected_helix(settings: TwitchSettings) -> Helix:
    headers = {"Authorization": f"Bearer {app_token(settings)}", "Client-Id": settings.client_id}
    with client(HELIX, headers) as session:
        user = get_json(session, "/users", {"login": settings.channel})["data"][0]["id"]
    return Helix(headers, user)


def minute_payload(helix: Helix, channel: str, minute: datetime, now: datetime) -> dict[str, str | int | bool]:
    with client(HELIX, helix.headers) as session:
        streams = get_json(session, "/streams", {"user_id": helix.user})["data"]
        followers = get_json(session, "/channels/followers", {"broadcaster_id": helix.user})["total"]
    return {
        "channel": channel,
        "minute": minute.isoformat(),
        "live": bool(streams),
        "viewers": sum(stream["viewer_count"] for stream in streams),
        "followers": followers,
        "at": now.isoformat(),
    }


class ViewerCounter:
    def __init__(self, settings: TwitchSettings) -> None:
        self.settings = settings
        self.helix: Helix | None = None

    def counted(self, minute: datetime, now: datetime) -> dict[str, str | int | bool] | None:
        try:
            if self.helix is None:
                self.helix = connected_helix(self.settings)
            return minute_payload(self.helix, self.settings.channel.lower(), minute, now)
        except (httpx.HTTPError, KeyError, IndexError) as error:
            logger.warning(f"The viewers of {minute:%H:%M} are not counted: {error!r}")
            self.helix = None
            return None
