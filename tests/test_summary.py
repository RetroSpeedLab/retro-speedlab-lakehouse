from datetime import UTC, datetime

import pytest
from pyspark.sql import Row, SparkSession

from retro_speedlab_lakehouse.cockpit.findings import CLAUDE, FINDINGS
from retro_speedlab_lakehouse.cockpit.summary import SummaryRefused, build_summary, checked, listed, summarize
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings

LIMIT = 400
ROWS = [
    Row(verdict="act", area="search", statement="Search on floeporter is stuck: no faster part for 9 h."),
    Row(verdict="watch", area="lab", statement="Lab runs in the last 24 h: 1, below the 7-day average of 3."),
    Row(verdict="good", area="records", statement="Records in the last 24 h: 41, above the usual."),
]


def test_the_guard_keeps_only_numbers_from_the_findings() -> None:
    known = "Search on floeporter is stuck: no faster part for 9 h."
    assert checked(" Floeporter has found nothing for 9 h. ", known, LIMIT) == "Floeporter has found nothing for 9 h."
    with pytest.raises(SummaryRefused):
        checked("Floeporter has found nothing for 12 h.", known, LIMIT)
    with pytest.raises(SummaryRefused):
        checked("Two" + chr(10) + "lines", known, LIMIT)


def test_a_rejected_summary_falls_back_to_the_findings_that_need_attention() -> None:
    kept = summarize(lambda known: "Fix the search stuck on floeporter for 9 h.", LIMIT, ROWS)
    assert kept.source == CLAUDE
    refused = summarize(lambda known: "Search is stuck for 10 h.", LIMIT, ROWS)
    assert refused.source == FINDINGS
    assert refused.text == f"Act: {ROWS[0]['statement']} Watch: {ROWS[1]['statement']}"
    assert listed(ROWS[2:]) == "All 1 findings are good."


def test_the_summary_fails_loudly_without_an_anthropic_key(
    spark: SparkSession, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        build_summary(spark, settings.lake, settings.summary, Gate("summary"), datetime.now(UTC))
