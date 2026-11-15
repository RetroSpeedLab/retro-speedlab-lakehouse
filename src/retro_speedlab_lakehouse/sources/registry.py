from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.record import Source


def hourly_sources(settings: Settings) -> list[Source]:
    return []


def nightly_sources(settings: Settings) -> list[Source]:
    return []
