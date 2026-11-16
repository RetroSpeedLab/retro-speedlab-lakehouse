from retro_speedlab_lakehouse.sources.bugsink import bugsink_numbers
from retro_speedlab_lakehouse.sources.discord import discord_numbers
from retro_speedlab_lakehouse.sources.github import github_numbers
from retro_speedlab_lakehouse.sources.langfuse import langfuse_numbers
from retro_speedlab_lakehouse.sources.plays import umami_plays
from retro_speedlab_lakehouse.sources.record import SnapshotSource, Source
from retro_speedlab_lakehouse.sources.settings import Sources
from retro_speedlab_lakehouse.sources.twitch import twitch_numbers
from retro_speedlab_lakehouse.sources.umami import umami_numbers
from retro_speedlab_lakehouse.sources.uptime import uptime_numbers
from retro_speedlab_lakehouse.sources.wandb import wandb_numbers

NUMBERS = "numbers"
PLAYS = "plays"


def hourly_sources(sources: Sources) -> list[Source]:
    return [
        SnapshotSource("uptime", NUMBERS, lambda now: uptime_numbers(sources.uptime)),
        SnapshotSource("twitch", NUMBERS, lambda now: twitch_numbers(sources.twitch)),
        SnapshotSource("discord", NUMBERS, lambda now: discord_numbers(sources.discord)),
        SnapshotSource("umami", NUMBERS, lambda now: umami_numbers(sources.umami, now)),
        SnapshotSource("umami", PLAYS, lambda now: umami_plays(sources.umami, now)),
        SnapshotSource("langfuse", NUMBERS, lambda now: langfuse_numbers(sources.langfuse, now)),
        SnapshotSource("wandb", NUMBERS, lambda now: wandb_numbers(sources.wandb)),
        SnapshotSource("bugsink", NUMBERS, lambda now: bugsink_numbers(sources.bugsink)),
    ]


def nightly_sources(sources: Sources) -> list[Source]:
    github = SnapshotSource("github", NUMBERS, lambda now: github_numbers(sources.github, now))
    return [*hourly_sources(sources), github]
