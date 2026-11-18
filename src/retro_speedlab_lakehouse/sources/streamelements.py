from typing import Any

from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import StreamElementsSettings

API = "https://api.streamelements.com/kappa/v2"


def human_amount(counts: list[dict[str, Any]], ignored: frozenset[str]) -> int:
    return sum(count["amount"] for count in counts if count["name"].lower() not in ignored)


def chat_numbers(chat: dict[str, Any], ignored: frozenset[str]) -> list[Number]:
    humans = [chatter for chatter in chat["chatters"] if chatter["name"].lower() not in ignored]
    return [
        Number("chat_messages", "Chat messages", human_amount(chat["chatters"], ignored), "", "all time, without bots"),
        Number("chatters", "Chatters", len(humans), "", "all time, without bots"),
        Number("commands", "Commands", sum(command["amount"] for command in chat["commands"]), "", "all time"),
    ]


def session_numbers(data: dict[str, Any]) -> list[Number]:
    return [
        Number("followers", "Followers", data["follower-total"]["count"], "", "all time"),
        Number("followers_month", "New followers", data["follower-month"]["count"], "", "this month"),
        Number("subscribers", "Subscribers", data["subscriber-total"]["count"], "", "all time"),
        Number("tips", "Tips", data["tip-count"]["count"], "", "all time"),
        Number("tip_amount", "Tipped", data["tip-total"]["amount"], "USD", "all time"),
        Number("cheers", "Bits cheered", data["cheer-total"]["amount"], "", "all time"),
    ]


def streamelements_numbers(settings: StreamElementsSettings) -> list[Number]:
    with client(API, {"Authorization": f"Bearer {settings.token}"}) as session:
        channel = get_json(session, "/channels/me", {})["_id"]
        data = get_json(session, f"/sessions/{channel}", {})["data"]
        chat = get_json(session, f"/chatstats/{settings.channel}/stats", {})
    return session_numbers(data) + chat_numbers(chat, settings.ignored)
