from dataclasses import dataclass
from datetime import datetime, timedelta

MINUTE = timedelta(minutes=1)


def minute_of(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


@dataclass(slots=True)
class ChatMinute:
    started: datetime
    messages: int
    chatters: set[str]

    def heard(self, name: str) -> None:
        self.messages += 1
        self.chatters.add(name)

    def payload(self, channel: str, now: datetime) -> dict[str, str | int]:
        return {
            "channel": channel,
            "minute": self.started.isoformat(),
            "messages": self.messages,
            "chatters": len(self.chatters),
            "at": now.isoformat(),
        }


def fresh_minute(now: datetime) -> ChatMinute:
    return ChatMinute(minute_of(now), 0, set())
