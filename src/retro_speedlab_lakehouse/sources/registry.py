from retro_speedlab_lakehouse.sources.bugsink import bugsink_numbers
from retro_speedlab_lakehouse.sources.discord import discord_numbers
from retro_speedlab_lakehouse.sources.github import github_numbers
from retro_speedlab_lakehouse.sources.langfuse import langfuse_numbers
from retro_speedlab_lakehouse.sources.number import NumberSource
from retro_speedlab_lakehouse.sources.record import Source
from retro_speedlab_lakehouse.sources.settings import Sources
from retro_speedlab_lakehouse.sources.twitch import twitch_numbers
from retro_speedlab_lakehouse.sources.umami import umami_numbers
from retro_speedlab_lakehouse.sources.uptime import uptime_numbers
from retro_speedlab_lakehouse.sources.wandb import wandb_numbers

NUMBERS = "numbers"


def hourly_sources(sources: Sources) -> list[Source]:
    return [
        NumberSource("uptime", NUMBERS, lambda now: uptime_numbers(sources.uptime)),
        NumberSource("twitch", NUMBERS, lambda now: twitch_numbers(sources.twitch)),
        NumberSource("discord", NUMBERS, lambda now: discord_numbers(sources.discord)),
        NumberSource("umami", NUMBERS, lambda now: umami_numbers(sources.umami, now)),
        NumberSource("langfuse", NUMBERS, lambda now: langfuse_numbers(sources.langfuse, now)),
        NumberSource("wandb", NUMBERS, lambda now: wandb_numbers(sources.wandb)),
        NumberSource("bugsink", NUMBERS, lambda now: bugsink_numbers(sources.bugsink)),
    ]


def nightly_sources(sources: Sources) -> list[Source]:
    return [*hourly_sources(sources), NumberSource("github", NUMBERS, lambda now: github_numbers(sources.github, now))]
