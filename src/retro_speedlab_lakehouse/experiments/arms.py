from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

DEPLOY_LAG = timedelta(minutes=30)
HOUR = timedelta(hours=1)
MAX_HOURS = 72
EARLIEST = datetime.min.replace(tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class SearchHour:
    game: str
    hour: datetime
    attempts: int
    crossings: int
    saved: int
    records: int
    unverified: int


@dataclass(frozen=True, slots=True)
class Change:
    game: str
    stamp: str
    at: datetime


@dataclass(frozen=True, slots=True)
class Arm:
    hours: list[SearchHour]

    @property
    def saved(self) -> list[float]:
        return [float(hour.saved) for hour in self.hours]

    @property
    def attempts(self) -> int:
        return sum(hour.attempts for hour in self.hours)

    @property
    def crossings(self) -> int:
        return sum(hour.crossings for hour in self.hours)

    @property
    def record_hours(self) -> int:
        return sum(hour.records > 0 for hour in self.hours)

    @property
    def verified(self) -> int:
        return sum(hour.records + hour.unverified for hour in self.hours)

    @property
    def unverified(self) -> int:
        return sum(hour.unverified for hour in self.hours)


def within(hours: list[SearchHour], start: datetime, end: datetime) -> list[SearchHour]:
    return [hour for hour in hours if start <= hour.hour and hour.hour + HOUR <= end]


def arms(change: Change, previous: datetime, following: datetime, hours: list[SearchHour]) -> tuple[Arm, Arm]:
    before = within(hours, previous + DEPLOY_LAG, change.at)[-MAX_HOURS:]
    after = within(hours, change.at + DEPLOY_LAG, following)[:MAX_HOURS]
    return Arm(before), Arm(after)


def experiments(changes: list[Change], hours: list[SearchHour], now: datetime) -> list[tuple[Change, Arm, Arm]]:
    found: list[tuple[Change, Arm, Arm]] = []
    for game in sorted({change.game for change in changes}):
        ordered = sorted((change for change in changes if change.game == game), key=lambda change: change.at)
        searched = sorted((hour for hour in hours if hour.game == game), key=lambda hour: hour.hour)
        bounds = [EARLIEST, *(change.at for change in ordered), now]
        for index, change in enumerate(ordered):
            before, after = arms(change, bounds[index], bounds[index + 2], searched)
            found.append((change, before, after))
    return found
