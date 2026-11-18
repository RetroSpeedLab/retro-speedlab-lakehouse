from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.record import SourceRecord
from retro_speedlab_lakehouse.sources.settings import BlueskySettings

API = "https://public.api.bsky.app"
FEED = "/xrpc/app.bsky.feed.getAuthorFeed"
PROFILE = "/xrpc/app.bsky.actor.getProfile"
PAGE_SIZE = 100


@dataclass(frozen=True, slots=True)
class BlueskyPost:
    uri: str
    text: str
    created_at: datetime
    likes: int
    reposts: int
    replies: int
    quotes: int

    @property
    def record(self) -> SourceRecord:
        return SourceRecord(self.uri, asdict(self))


def post_of(item: dict[str, Any]) -> BlueskyPost:
    post = item["post"]
    return BlueskyPost(
        post["uri"],
        post["record"]["text"],
        datetime.fromisoformat(post["record"]["createdAt"]),
        post["likeCount"],
        post["repostCount"],
        post["replyCount"],
        post["quoteCount"],
    )


def bluesky_posts(settings: BlueskySettings) -> list[BlueskyPost]:
    params: dict[str, Any] = {"actor": settings.handle, "limit": PAGE_SIZE}
    posts: list[BlueskyPost] = []
    with client(API, {}) as session:
        while True:
            page = get_json(session, FEED, params)
            posts += [post_of(item) for item in page["feed"] if "reason" not in item]
            if "cursor" not in page or not page["feed"]:
                return posts
            params = {**params, "cursor": page["cursor"]}


def bluesky_numbers(settings: BlueskySettings) -> list[Number]:
    with client(API, {}) as session:
        profile = get_json(session, PROFILE, {"actor": settings.handle})
    likes = sum(post.likes for post in bluesky_posts(settings))
    return [
        Number("followers", "Followers", profile["followersCount"], "", "on Bluesky"),
        Number("likes", "Likes", likes, "", "on all posts"),
    ]
