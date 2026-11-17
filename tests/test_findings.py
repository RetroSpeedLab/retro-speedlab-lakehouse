from datetime import UTC, datetime

from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.cockpit.catalog import COCKPIT_FINDINGS
from retro_speedlab_lakehouse.cockpit.days import RECORDS_METRIC
from retro_speedlab_lakehouse.cockpit.findings import (
    DayMetric,
    Evidence,
    GameSearch,
    Verdict,
    cost_finding,
    disk_finding,
    findings,
    flow_finding,
    jobs_finding,
    level_finding,
    quality_finding,
    search_finding,
    stream_finding,
)
from retro_speedlab_lakehouse.cockpit.job import build_findings
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from tests.conftest import findings_settings

RULES = findings_settings()


def week(today: float, before: float) -> dict[int, float | None]:
    values: dict[int, float | None] = {day: before for day in range(1, RULES.baseline_days + 1)}
    values[0] = today
    return values


def test_a_flow_metric_is_judged_against_its_own_weekly_average() -> None:
    above = flow_finding(DayMetric("Records", "records", True, week(41, 18)), RULES)
    below = flow_finding(DayMetric("Records", "records", True, week(5, 18)), RULES)
    usual = flow_finding(DayMetric("Records", "records", True, week(19, 18)), RULES)
    assert (above.verdict, below.verdict, usual.verdict) == (Verdict.GOOD, Verdict.WATCH, Verdict.GOOD)
    assert above.statement == "Records in the last 24 h: 41, above the usual; the 7-day average is 18."
    assert (above.value, above.baseline, above.action) == (41, 18, None)


def test_a_metric_without_enough_history_says_so() -> None:
    young = flow_finding(DayMetric("Lab runs", "lab", True, {0: 3.0, 1: 2.0}), RULES)
    assert young.verdict is Verdict.WATCH
    assert (
        young.statement
        == f"Lab runs: not enough data yet; it needs {RULES.min_baseline_days} days with data and has 1."
    )


def test_a_level_metric_reports_its_change_since_the_oldest_day() -> None:
    finding = level_finding(DayMetric("Twitch followers", "audience", False, {0: 39.0, 3: 36.0, 6: None}), RULES)
    assert (finding.verdict, finding.statement) == (Verdict.GOOD, "Twitch followers: 39, up 3 in 3 days.")
    still = level_finding(DayMetric("Discord members", "audience", False, {0: 4.0, 1: 4.0}), RULES)
    assert still.statement == "Discord members: 4, unchanged in 1 day."


def test_the_longest_stuck_search_decides_and_names_its_game() -> None:
    searches = (GameSearch("floeporter", 9.0), GameSearch("comet_courier", 1.0), GameSearch("deepset", 2.0))
    finding = search_finding(searches, RULES)
    assert finding.verdict is Verdict.ACT
    assert finding.statement.startswith("Search on floeporter is stuck: no faster part for 9 h, the longest of the 3")
    assert finding.action is not None and "floeporter" in finding.action
    calm = search_finding((GameSearch("comet_courier", 1.0),), RULES)
    assert (calm.verdict, calm.action) == (Verdict.GOOD, None)
    never = search_finding((GameSearch("deepset", None),), RULES)
    assert "no faster part in 7 days" in never.statement


def test_platform_rules_grade_against_their_configured_thresholds() -> None:
    assert jobs_finding((), RULES).verdict is Verdict.GOOD
    assert jobs_finding(("gold",), RULES).verdict is Verdict.WATCH
    assert jobs_finding(("gold",) * RULES.failed_runs_act, RULES).verdict is Verdict.ACT
    assert stream_finding(RULES.stream_act_minutes, RULES).verdict is Verdict.ACT
    assert stream_finding(None, RULES).verdict is Verdict.ACT
    disk = disk_finding({"main": 70.0, "black-omen": RULES.disk_act_percent + 1}, RULES)
    assert disk.verdict is Verdict.ACT and disk.action is not None and "black-omen" in disk.action
    assert quality_finding(("gold.lab_roi", "gold.lab_roi"), RULES).value == 1


def test_claude_cost_compares_yesterday_with_the_week_before() -> None:
    costs = {1: 9.0} | {day: 4.0 for day in range(2, RULES.baseline_days + 2)}
    finding = cost_finding(costs, RULES)
    assert finding.verdict is Verdict.WATCH
    assert finding.statement == "Claude cost yesterday: 9 USD, above the usual; the 7-day average is 4 USD."
    assert cost_finding({1: 2.0}, RULES).statement.startswith("Claude cost: not enough data yet")


def evidence() -> Evidence:
    return Evidence(
        metrics=(DayMetric("Records", "records", True, week(41, 18)),),
        searches=(GameSearch("comet_courier", 1.0),),
        failed_jobs=("gold",),
        failed_tables=(),
        stream_lag_minutes=2.0,
        disks={"main": 50.0},
        claude_cost={},
    )


def test_every_rule_yields_one_finding_with_a_unique_subject() -> None:
    found = findings(evidence(), RULES)
    assert len({(finding.area, finding.subject) for finding in found}) == len(found) == 7
    assert all(finding.action for finding in found if finding.verdict is Verdict.ACT)


def test_the_findings_land_in_the_lake(spark: SparkSession, settings: Settings) -> None:
    now = datetime.now(UTC)
    days = spark.createDataFrame([("Records", 0, 41.0)], "metric string, days_ago int, value double")
    assert build_findings(spark, settings.lake, RULES, Gate("findings"), days, [RECORDS_METRIC], now) == 7
    rows = read(spark, COCKPIT_FINDINGS, settings.lake).collect()
    assert {row["subject"] for row in rows} >= {"Search progress", "Airflow jobs", "Spark stream"}
    assert all(row["verdict"] in {"act", "watch", "good"} for row in rows)
