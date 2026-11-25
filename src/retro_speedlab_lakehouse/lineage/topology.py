from dataclasses import dataclass

from retro_speedlab_lakehouse.lake.catalog import EVENTS, LIVE_RUN, SEARCH_EVENTS, STREAM_STATUS
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.record import Source
from retro_speedlab_lakehouse.sources.registry import hourly_sources, nightly_sources
from retro_speedlab_lakehouse.stream.job import SERVED
from retro_speedlab_lakehouse.stream.rows import LIVE_TOPIC
from retro_speedlab_lakehouse.stream.search import PULSES
from retro_speedlab_lakehouse.stream.search_rows import CHAT_EVENTS

PROCESSES_DAG = "processes"
BACKEND = "service:backend"
RUNNER = "service:runner"
TWITCH = "source:twitch"
GRAFANA = "export:grafana-postgres"
RECORDS = "files:records"

Nodes = tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Process:
    name: str
    inputs: Nodes
    outputs: Nodes


def topics(names: list[str]) -> Nodes:
    return tuple(f"topic:{name}" for name in names)


def sources_of(sources: list[Source]) -> Nodes:
    return tuple(sorted({f"source:{source.name}" for source in sources}))


def processes(settings: Settings) -> tuple[Process, ...]:
    backend = topics(settings.kafka.backend_topics)
    search = topics(settings.kafka.search_topics)
    chat = topics([CHAT_EVENTS])
    return (
        Process("relay", (BACKEND,), backend),
        Process("runner", (RUNNER,), tuple(topic for topic in search if topic not in chat)),
        Process("chat", (TWITCH,), chat),
        Process("stream-live", topics([LIVE_TOPIC]), (LIVE_RUN.full_name, STREAM_STATUS.full_name)),
        Process("stream-entities", backend, (EVENTS.full_name, *(table.full_name for table, _ in SERVED[1:]))),
        Process("stream-search", search, (SEARCH_EVENTS.full_name, *(table.full_name for table, _ in PULSES))),
    )


def externals(settings: Settings) -> dict[str, tuple[Nodes, Nodes]]:
    return {
        "api-hourly": ((BACKEND,), ()),
        "api-nightly": ((BACKEND,), ()),
        "videos-nightly": ((BACKEND,), ()),
        "sources-hourly": (sources_of(hourly_sources(settings.sources)), ()),
        "sources-nightly": (sources_of(nightly_sources(settings.sources)), ()),
        "mine-records": ((RECORDS,), ()),
        "tracking-hourly": ((), (GRAFANA,)),
        "experiments-export": ((), (GRAFANA,)),
    }
