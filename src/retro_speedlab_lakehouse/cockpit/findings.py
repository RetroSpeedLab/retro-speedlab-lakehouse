from dataclasses import dataclass
from enum import StrEnum
from statistics import mean

from retro_speedlab_lakehouse.settings import FindingsSettings


class Verdict(StrEnum):
    ACT = "act"
    WATCH = "watch"
    GOOD = "good"


RANKS = {Verdict.ACT: 0, Verdict.WATCH: 1, Verdict.GOOD: 2}
CLAUDE = "claude"
FINDINGS = "findings"


@dataclass(frozen=True, slots=True)
class Finding:
    area: str
    subject: str
    verdict: Verdict
    statement: str
    value: float | None
    baseline: float | None
    action: str | None

    @property
    def rank(self) -> int:
        return RANKS[self.verdict]


@dataclass(frozen=True, slots=True)
class DayMetric:
    name: str
    area: str
    flow: bool
    values: dict[int, float | None]


@dataclass(frozen=True, slots=True)
class GameSearch:
    game: str
    hours_since_improvement: float | None


@dataclass(frozen=True, slots=True)
class Evidence:
    metrics: tuple[DayMetric, ...]
    searches: tuple[GameSearch, ...]
    failed_jobs: tuple[str, ...]
    failed_tables: tuple[str, ...]
    stream_lag_minutes: float | None
    disks: dict[str, float]
    claude_cost: dict[int, float]


def figure(value: float) -> str:
    return f"{value:,.0f}" if abs(value) >= 10 or value == round(value) else f"{value:,.1f}"


def graded(value: float, watch: float, act: float) -> Verdict:
    if value >= act:
        return Verdict.ACT
    if value >= watch:
        return Verdict.WATCH
    return Verdict.GOOD


def thin(area: str, subject: str, have: int, rules: FindingsSettings) -> Finding:
    statement = f"{subject}: not enough data yet, {have} of the {rules.min_baseline_days} days a baseline needs."
    return Finding(area, subject, Verdict.WATCH, statement, None, None, None)


def measured(metric: DayMetric, day: int) -> float | None:
    return metric.values[day] if day in metric.values else None


def history(metric: DayMetric, rules: FindingsSettings) -> list[float]:
    days = range(1, rules.baseline_days + 1)
    return [value for day in days if (value := measured(metric, day)) is not None]


def flow_finding(metric: DayMetric, rules: FindingsSettings) -> Finding:
    before = history(metric, rules)
    today = measured(metric, 0)
    if today is None or len(before) < rules.min_baseline_days:
        return thin(metric.area, metric.name, len(before), rules)
    baseline = mean(before)
    comparison = f"the {len(before)}-day average is {figure(baseline)}"
    if today <= baseline * (1 - rules.change):
        verdict, trend = Verdict.WATCH, "below"
    elif today >= baseline * (1 + rules.change):
        verdict, trend = Verdict.GOOD, "above"
    else:
        verdict, trend = Verdict.GOOD, "in line with"
    statement = f"{metric.name} in the last 24 h: {figure(today)}, {trend} the usual; {comparison}."
    return Finding(metric.area, metric.name, verdict, statement, today, baseline, None)


def level_finding(metric: DayMetric, rules: FindingsSettings) -> Finding:
    before = [(day, value) for day in range(rules.baseline_days, 0, -1) if (value := measured(metric, day)) is not None]
    today = measured(metric, 0)
    if today is None or not before:
        return thin(metric.area, metric.name, len(before), rules)
    days, then = before[0]
    change = today - then
    verdict = Verdict.GOOD if change >= 0 else Verdict.WATCH
    direction = "up" if change >= 0 else "down"
    statement = f"{metric.name}: {figure(today)}, {direction} {figure(abs(change))} in {days} days."
    return Finding(metric.area, metric.name, verdict, statement, today, then, None)


def waited(search: GameSearch, window: float) -> float:
    return window if search.hours_since_improvement is None else search.hours_since_improvement


def search_finding(searches: tuple[GameSearch, ...], rules: FindingsSettings) -> Finding:
    subject = "Search progress"
    if not searches:
        statement = "No game was searched in the last 24 h."
        return Finding("search", subject, Verdict.ACT, statement, None, None, rules.actions.search_idle)
    window = rules.baseline_days * 24
    gaps = {search.game: waited(search, window) for search in searches}
    game = max(gaps, key=lambda name: gaps[name])
    gap = gaps[game]
    verdict = graded(gap, rules.stuck_watch_hours, rules.stuck_act_hours)
    if verdict is Verdict.GOOD:
        statement = f"All {len(gaps)} searched games found a faster part within {figure(gap)} h."
        return Finding("search", subject, verdict, statement, gap, rules.stuck_watch_hours, None)
    span = f"for {figure(gap)} h" if gap < window else f"in {rules.baseline_days} days"
    statement = (
        f"Search on {game} is stuck: no faster part {span}, the longest of the {len(gaps)} searched games "
        f"(watch from {figure(rules.stuck_watch_hours)} h)."
    )
    action = rules.actions.search_stuck.format(game=game) if verdict is Verdict.ACT else None
    return Finding("search", subject, verdict, statement, gap, rules.stuck_watch_hours, action)


def jobs_finding(failed: tuple[str, ...], rules: FindingsSettings) -> Finding:
    subject = "Airflow jobs"
    verdict = graded(len(failed), 1, rules.failed_runs_act)
    if verdict is Verdict.GOOD:
        return Finding("platform", subject, verdict, "No Airflow job failed in the last 24 h.", 0, None, None)
    names = ", ".join(sorted(set(failed)))
    statement = f"{len(failed)} Airflow runs failed in the last 24 h ({names}); act from {rules.failed_runs_act}."
    action = rules.actions.failed_jobs if verdict is Verdict.ACT else None
    return Finding("platform", subject, verdict, statement, len(failed), rules.failed_runs_act, action)


def quality_finding(tables: tuple[str, ...], rules: FindingsSettings) -> Finding:
    subject = "Quality gates"
    if not tables:
        return Finding(
            "data quality", subject, Verdict.GOOD, "Every quality gate passed in the last 24 h.", 0, None, None
        )
    names = ", ".join(sorted(set(tables)))
    statement = f"{len(set(tables))} tables failed their quality gate in the last 24 h: {names}."
    return Finding("data quality", subject, Verdict.ACT, statement, len(set(tables)), 0, rules.actions.quality)


def stream_finding(lag: float | None, rules: FindingsSettings) -> Finding:
    subject = "Spark stream"
    if lag is None:
        return Finding(
            "platform", subject, Verdict.ACT, "The Spark stream reports no status.", None, None, rules.actions.stream
        )
    verdict = graded(lag, rules.stream_watch_minutes, rules.stream_act_minutes)
    statement = f"The Spark stream is {figure(lag)} min behind; watch from {figure(rules.stream_watch_minutes)} min."
    action = rules.actions.stream if verdict is Verdict.ACT else None
    return Finding("platform", subject, verdict, statement, lag, rules.stream_watch_minutes, action)


def disk_finding(disks: dict[str, float], rules: FindingsSettings) -> Finding:
    subject = "Disk space"
    if not disks:
        return Finding("platform", subject, Verdict.WATCH, "No server reported its disk use.", None, None, None)
    host = max(disks, key=lambda name: disks[name])
    verdict = graded(disks[host], rules.disk_watch_percent, rules.disk_act_percent)
    statement = (
        f"The fullest disk is on {host} at {figure(disks[host])} %; watch from {figure(rules.disk_watch_percent)} %."
    )
    action = rules.actions.disk.format(host=host) if verdict is Verdict.ACT else None
    return Finding("platform", subject, verdict, statement, disks[host], rules.disk_watch_percent, action)


def cost_finding(costs: dict[int, float], rules: FindingsSettings) -> Finding:
    subject = "Claude cost"
    before = [costs[day] for day in range(2, rules.baseline_days + 2) if day in costs]
    if 1 not in costs or len(before) < rules.min_baseline_days:
        return thin("cost", subject, len(before), rules)
    yesterday, baseline = costs[1], mean(before)
    verdict = Verdict.WATCH if yesterday >= baseline * (1 + rules.change) else Verdict.GOOD
    trend = "above" if verdict is Verdict.WATCH else "within"
    statement = (
        f"Claude cost yesterday: {figure(yesterday)} USD, {trend} the usual; "
        f"the {len(before)}-day average is {figure(baseline)} USD."
    )
    return Finding("cost", subject, verdict, statement, yesterday, baseline, None)


def findings(evidence: Evidence, rules: FindingsSettings) -> list[Finding]:
    measured = [
        flow_finding(metric, rules) if metric.flow else level_finding(metric, rules) for metric in evidence.metrics
    ]
    return [
        *measured,
        search_finding(evidence.searches, rules),
        jobs_finding(evidence.failed_jobs, rules),
        quality_finding(evidence.failed_tables, rules),
        stream_finding(evidence.stream_lag_minutes, rules),
        disk_finding(evidence.disks, rules),
        cost_finding(evidence.claude_cost, rules),
    ]
