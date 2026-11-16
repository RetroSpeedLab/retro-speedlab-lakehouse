from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.record import SourcePage, SourceRecord
from retro_speedlab_lakehouse.sources.settings import DiscordSettings

API = "https://discord.com/api/v10"
DISCORD = "discord"
ACTIVITY = "activity"
DISCORD_EPOCH_MS = 1_420_070_400_000
SNOWFLAKE_SHIFT = 22
VIEW_CHANNEL = 1 << 10
TEXT_CHANNELS = frozenset({0, 5})
SPOKEN = frozenset({0, 19})
JOINED = 7
PAGE = 100
DAY = timedelta(days=1)
BACKFILL_DAYS = 7


def snowflake(moment: datetime) -> int:
    return (int(moment.timestamp() * 1000) - DISCORD_EPOCH_MS) << SNOWFLAKE_SHIFT


def public(channel: dict[str, Any], guild: str) -> bool:
    if channel["type"] not in TEXT_CHANNELS:
        return False
    everyone = [overwrite for overwrite in channel["permission_overwrites"] if overwrite["id"] == guild]
    return not any(int(overwrite["deny"]) & VIEW_CHANNEL for overwrite in everyone)


def human(message: dict[str, Any]) -> bool:
    author = message["author"]
    return message["type"] in SPOKEN and not ("bot" in author and author["bot"])


def day_messages(session: httpx.Client, channel: str, day: datetime) -> list[dict[str, Any]]:
    end = snowflake(day + DAY)
    found: list[dict[str, Any]] = []
    after = snowflake(day)
    while True:
        page = get_json(session, f"/channels/{channel}/messages", {"after": after, "limit": PAGE})
        inside = [message for message in page if int(message["id"]) < end]
        found.extend(inside)
        if len(page) < PAGE or len(inside) < len(page):
            return found
        after = max(int(message["id"]) for message in page)


def days_to_load(watermark: str | None, now: datetime) -> list[datetime]:
    today = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    earliest = today - BACKFILL_DAYS * DAY
    start = earliest if watermark is None else max(datetime.fromisoformat(watermark), earliest)
    return [start + index * DAY for index in range((today - start).days)]


def day_record(session: httpx.Client, channels: list[str], day: datetime) -> SourceRecord:
    messages = [message for channel in channels for message in day_messages(session, channel, day)]
    body = {
        "date": day.date().isoformat(),
        "messages": sum(human(message) for message in messages),
        "joins": sum(message["type"] == JOINED for message in messages),
    }
    return SourceRecord(f"daily/{body['date']}", body)


@dataclass(frozen=True, slots=True)
class DiscordActivity:
    settings: DiscordSettings

    @property
    def name(self) -> str:
        return DISCORD

    @property
    def stream(self) -> str:
        return ACTIVITY

    def fetch(self, watermark: str | None, now: datetime) -> SourcePage:
        days = days_to_load(watermark, now)
        if not days:
            return SourcePage([], watermark)
        with client(API, {"Authorization": f"Bot {self.settings.token}"}) as session:
            guild = get_json(session, f"/channels/{self.settings.channel}", {})["guild_id"]
            listed = get_json(session, f"/guilds/{guild}/channels", {})
            channels = [channel["id"] for channel in listed if public(channel, guild)]
            records = [day_record(session, channels, day) for day in days]
        return SourcePage(records, (days[-1] + DAY).isoformat())
