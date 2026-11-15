from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import TIMEOUT_SECONDS, client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import TwitchSettings

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
HELIX = "https://api.twitch.tv/helix"
PAGE = 100


def app_token(settings: TwitchSettings) -> str:
    credentials = {
        "client_id": settings.client_id,
        "client_secret": settings.client_secret,
        "grant_type": "client_credentials",
    }
    response = httpx.post(TOKEN_URL, data=credentials, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()["access_token"]


def every_page(session: httpx.Client, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cursor: dict[str, str] = {}
    while True:
        page = get_json(session, path, {**params, **cursor, "first": PAGE})
        items.extend(page["data"])
        if not page["data"] or "cursor" not in page["pagination"]:
            return items
        cursor = {"after": page["pagination"]["cursor"]}


def twitch_numbers(settings: TwitchSettings) -> list[Number]:
    headers = {"Authorization": f"Bearer {app_token(settings)}", "Client-Id": settings.client_id}
    with client(HELIX, headers) as session:
        user = get_json(session, "/users", {"login": settings.channel})["data"][0]
        followers = get_json(session, "/channels/followers", {"broadcaster_id": user["id"]})["total"]
        videos = every_page(session, "/videos", {"user_id": user["id"]})
        clips = every_page(session, "/clips", {"broadcaster_id": user["id"]})
        streams = get_json(session, "/streams", {"user_id": user["id"]})["data"]
    numbers = [
        Number("followers", "Followers", followers, "", "all time"),
        Number("videos", "Videos", len(videos), "", "kept on the channel"),
        Number("video_views", "Video views", sum(video["view_count"] for video in videos), "", "all kept videos"),
        Number("clips", "Clips", len(clips), "", "all time"),
        Number("clip_views", "Clip views", sum(clip["view_count"] for clip in clips), "", "all clips"),
    ]
    live = [Number("viewers", "Viewers now", stream["viewer_count"], "", "live at the last load") for stream in streams]
    return numbers + live
