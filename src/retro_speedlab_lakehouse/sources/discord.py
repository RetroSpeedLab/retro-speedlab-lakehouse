from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import DiscordSettings

API = "https://discord.com/api/v10"


def discord_numbers(settings: DiscordSettings) -> list[Number]:
    with client(API, {"Authorization": f"Bot {settings.token}"}) as session:
        guild = get_json(session, f"/channels/{settings.channel}", {})["guild_id"]
        counts = get_json(session, f"/guilds/{guild}", {"with_counts": "true"})
    return [
        Number("members", "Members", counts["approximate_member_count"], "", "on the server"),
        Number("online", "Online", counts["approximate_presence_count"], "", "at the last load"),
    ]
