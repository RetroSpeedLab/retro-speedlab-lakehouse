from retro_speedlab_lakehouse.sources.aim import aim_numbers
from retro_speedlab_lakehouse.sources.bluesky import bluesky_numbers, bluesky_posts
from retro_speedlab_lakehouse.sources.bugsink import bugsink_numbers
from retro_speedlab_lakehouse.sources.discord import discord_numbers
from retro_speedlab_lakehouse.sources.dropoff import umami_dropoff
from retro_speedlab_lakehouse.sources.github import github_numbers
from retro_speedlab_lakehouse.sources.itch import itch_days, itch_numbers
from retro_speedlab_lakehouse.sources.plays import umami_plays
from retro_speedlab_lakehouse.sources.record import SnapshotSource, Source
from retro_speedlab_lakehouse.sources.rounds import TempoRounds
from retro_speedlab_lakehouse.sources.settings import Sources
from retro_speedlab_lakehouse.sources.streamelements import streamelements_numbers
from retro_speedlab_lakehouse.sources.tempo import tempo_days, tempo_lab_costs, tempo_numbers
from retro_speedlab_lakehouse.sources.twitch import twitch_numbers
from retro_speedlab_lakehouse.sources.umami import umami_numbers
from retro_speedlab_lakehouse.sources.uptime import uptime_numbers
from retro_speedlab_lakehouse.sources.youtube import youtube_days, youtube_numbers

NUMBERS = "numbers"
PLAYS = "plays"
DROPOFF = "dropoff"
LAB_COSTS = "lab_costs"
POSTS = "posts"
DAILY = "daily"
HOURLY_TRACE_DAYS = 1
NIGHTLY_TRACE_DAYS = 8


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
        SnapshotSource("tempo", LAB_COSTS, lambda now: tempo_lab_costs(sources.tempo, now)),
        TempoRounds(sources.tempo),
        SnapshotSource("aim", NUMBERS, lambda now: aim_numbers(sources.aim)),
        SnapshotSource("bugsink", NUMBERS, lambda now: bugsink_numbers(sources.bugsink)),
        SnapshotSource("bluesky", NUMBERS, lambda now: bluesky_numbers(sources.bluesky)),
        SnapshotSource("bluesky", POSTS, lambda now: bluesky_posts(sources.bluesky)),
        SnapshotSource("streamelements", NUMBERS, lambda now: streamelements_numbers(sources.streamelements)),
        SnapshotSource("youtube", NUMBERS, lambda now: youtube_numbers(sources.youtube, now)),
        SnapshotSource("youtube", DAILY, lambda now: youtube_days(sources.youtube, now)),
    ]


def hourly_sources(sources: Sources) -> list[Source]:
    today = SnapshotSource("tempo", DAILY, lambda now: tempo_days(sources.tempo, now, HOURLY_TRACE_DAYS))
    return [*each_run_sources(sources), today]


def nightly_sources(sources: Sources) -> list[Source]:
    return [
        *each_run_sources(sources),
        SnapshotSource("tempo", NUMBERS, lambda now: tempo_numbers(sources.tempo, now)),
        SnapshotSource("tempo", DAILY, lambda now: tempo_days(sources.tempo, now, NIGHTLY_TRACE_DAYS)),
        SnapshotSource("github", NUMBERS, lambda now: github_numbers(sources.github, now)),
    ]
