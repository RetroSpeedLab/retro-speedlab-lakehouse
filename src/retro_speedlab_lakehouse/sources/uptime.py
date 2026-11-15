from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import UptimeSettings

PERCENT = 100
DAY_SUFFIX = "_24"
NAME_SEPARATOR = " - "


def monitor_name(name: str) -> str:
    return name.split(NAME_SEPARATOR)[-1]


def uptime_numbers(settings: UptimeSettings) -> list[Number]:
    with client(settings.url, {}) as session:
        page = get_json(session, f"/api/status-page/{settings.page}", {})
        beats = get_json(session, f"/api/status-page/heartbeat/{settings.page}", {})
    monitors = [monitor for group in page["publicGroupList"] for monitor in group["monitorList"]]
    numbers: list[Number] = []
    for monitor in monitors:
        key = str(monitor["id"])
        if not beats["heartbeatList"][key]:
            continue
        name = monitor_name(monitor["name"])
        uptime = beats["uptimeList"][f"{key}{DAY_SUFFIX}"] * PERCENT
        ping = beats["heartbeatList"][key][-1]["ping"]
        numbers.append(Number(f"uptime/{key}", f"{name} uptime", round(uptime, 2), "%", "last 24 hours"))
        numbers.append(Number(f"ping/{key}", f"{name} response", ping, "ms", "last check"))
    return numbers
