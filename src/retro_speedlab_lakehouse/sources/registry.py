from retro_speedlab_lakehouse.sources.bugsink import bugsink_numbers
from retro_speedlab_lakehouse.sources.discord import discord_numbers
from retro_speedlab_lakehouse.sources.dropoff import umami_dropoff
from retro_speedlab_lakehouse.sources.github import github_numbers
from retro_speedlab_lakehouse.sources.itch import itch_days, itch_numbers
from retro_speedlab_lakehouse.sources.langfuse import langfuse_days, langfuse_lab_costs, langfuse_numbers
from retro_speedlab_lakehouse.sources.plays import umami_plays
from retro_speedlab_lakehouse.sources.record import SnapshotSource, Source
from retro_speedlab_lakehouse.sources.settings import Sources
from retro_speedlab_lakehouse.sources.twitch import twitch_numbers
from retro_speedlab_lakehouse.sources.umami import umami_numbers
from retro_speedlab_lakehouse.sources.uptime import uptime_numbers
from retro_speedlab_lakehouse.sources.wandb import wandb_numbers

NUMBERS = "numbers"
PLAYS = "plays"
DROPOFF = "dropoff"
LAB_COSTS = "lab_costs"
DAILY = "daily"
HOURLY_LANGFUSE_DAYS = 1
NIGHTLY_LANGFUSE_DAYS = 8


def each_run_sources(sources: Sources) -> list[Source]:
    return [
        SnapshotSource("uptime", NUMBERS, lambda now: uptime_numbers(sources.uptime)),
        SnapshotSource("twitch", NUMBERS, lambda now: twitch_numbers(sources.twitch)),
        SnapshotSource("discord", NUMBERS, lambda now: discord_numbers(sources.discord)),
        SnapshotSource("umami", NUMBERS, lambda now: umami_numbers(sources.umami, now)),
        SnapshotSource("umami", PLAYS, lambda now: umami_plays(sources.umami, sources.umami.games_website, now)),
        SnapshotSource("umami", DROPOFF, lambda now: umami_dropoff(sources.umami, now)),
        SnapshotSource("itch", NUMBERS, lambda now: itch_numbers(sources.umami, now)),
        SnapshotSource("itch", PLAYS, lambda now: umami_plays(sources.umami, sources.umami.itch_website, now)),
        SnapshotSource("itch", DAILY, lambda now: itch_days(sources.umami, now)),
        SnapshotSource("langfuse", LAB_COSTS, lambda now: langfuse_lab_costs(sources.langfuse, now)),
        SnapshotSource("wandb", NUMBERS, lambda now: wandb_numbers(sources.wandb)),
        SnapshotSource("bugsink", NUMBERS, lambda now: bugsink_numbers(sources.bugsink)),
    ]


def hourly_sources(sources: Sources) -> list[Source]:
    today = SnapshotSource("langfuse", DAILY, lambda now: langfuse_days(sources.langfuse, now, HOURLY_LANGFUSE_DAYS))
    return [*each_run_sources(sources), today]


def nightly_sources(sources: Sources) -> list[Source]:
    return [
        *each_run_sources(sources),
        SnapshotSource("langfuse", NUMBERS, lambda now: langfuse_numbers(sources.langfuse, now)),
        SnapshotSource("langfuse", DAILY, lambda now: langfuse_days(sources.langfuse, now, NIGHTLY_LANGFUSE_DAYS)),
        SnapshotSource("github", NUMBERS, lambda now: github_numbers(sources.github, now)),
    ]
