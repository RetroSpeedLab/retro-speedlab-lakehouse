from datetime import datetime, time

NIGHT_JOBS = frozenset(
    {
        "api-nightly",
        "experiments",
        "search-allocation",
        "search-bandit",
        "maintenance",
        "sources-nightly",
        "videos-nightly",
    }
)
NIGHT_START = time(2, 0)
NIGHT_END = time(6, 0)


def outside_the_night(name: str, moment: datetime) -> bool:
    return name in NIGHT_JOBS and not NIGHT_START <= moment.time() < NIGHT_END
