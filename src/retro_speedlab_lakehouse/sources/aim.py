from retro_speedlab_lakehouse.sources.http import client, get_json
from retro_speedlab_lakehouse.sources.number import Number
from retro_speedlab_lakehouse.sources.settings import AimSettings


def aim_numbers(settings: AimSettings) -> list[Number]:
    with client(settings.url, {}) as session:
        experiments = get_json(session, "/api/experiments/", {})
    runs = sum(experiment["run_count"] for experiment in experiments)
    return [Number("tracked_runs", "PPO runs and search turns", runs, "", "all time")]
