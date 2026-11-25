from datetime import UTC, datetime
from typing import Any

import pytest
import respx
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings
from retro_speedlab_lakehouse.sources.catalog import LAB_RUN_ROUNDS, LAB_RUN_TRACES, LAB_TRACE_DAILY
from retro_speedlab_lakehouse.sources.job import ingest_sources
from retro_speedlab_lakehouse.sources.rounds import LAB_EPOCH, TempoRounds, lab_round

from .conftest import fixture

NOW = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
AFTER_REAL_RUN = datetime(2026, 11, 22, 12, 0, tzinfo=UTC)
STARTED = 1763200800 * 1_000_000_000


def round_span(number: int, cost: float, model: str) -> dict[str, Any]:
    values = {"lab.game": "comet_courier", "lab.stamp": "2026-11-15T0935", "lab.outcome": "deployed"}
    counts = {"lab.round": number, "lab.turns": 10, "lab.tool_calls": 6, "lab.tool_errors": 1, "lab.subagents": 2}
    usage = {"lab.tokens": 900, "lab.usage.input": 100, "lab.usage.output": 200}
    cache = {"lab.usage.cache_read_input_tokens": 500, "lab.usage.cache_creation_input_tokens": 100}
    attributes = [{"key": key, "value": {"stringValue": value}} for key, value in values.items()]
    numbers = {**counts, **usage, **cache}
    attributes += [{"key": key, "value": {"intValue": str(value)}} for key, value in numbers.items()]
    attributes += [
        {"key": "lab.cost_usd", "value": {"doubleValue": cost}},
        {"key": "gen_ai.response.model", "value": {"stringValue": model}},
    ]
    start = STARTED + number * 600 * 1_000_000_000
    timing = {"startTimeUnixNano": str(start), "durationNanos": str(300 * 1_000_000_000)}
    return {"spanID": f"{number:016x}", **timing, "attributes": attributes}


@respx.mock
def test_each_round_in_tempo_adds_up_to_its_run_and_its_day(spark: SparkSession, settings: Settings) -> None:
    spans = [round_span(1, 1.5, "claude-opus-5-5"), round_span(2, 0.5, "claude-haiku-4-5")]
    route = respx.get("https://tempo.test/api/search").respond(json={"traces": [{"spanSets": [{"spans": spans}]}]})
    ingest_sources(
        spark,
        settings.lake,
        Gate("sources"),
        [TempoRounds(settings.sources.tempo)],
        [TempoRounds(settings.sources.tempo)],
        NOW,
    )

    [run] = read(spark, LAB_RUN_TRACES, settings.lake).collect()
    assert (run["rounds"], run["cost_usd"], run["minutes"], run["model"]) == (2, 2.0, 10.0, "claude-opus-5-5")
    assert (run["input_tokens"], run["output_tokens"], run["tool_calls"]) == (200, 400, 12)
    [day] = read(spark, LAB_TRACE_DAILY, settings.lake).collect()
    assert (day["game"], day["runs"], day["rounds"], day["tokens"]) == ("comet_courier", 1, 2, 1800)
    assert int(route.calls.last.request.url.params["start"]) == int(LAB_EPOCH.timestamp())


@respx.mock
def test_a_round_tempo_returns_without_a_duration_lasted_zero_minutes(spark: SparkSession, settings: Settings) -> None:
    respx.get("https://tempo.test/api/search").respond(json=fixture("tempo-rounds.json"))
    ingest_sources(
        spark,
        settings.lake,
        Gate("sources"),
        [TempoRounds(settings.sources.tempo)],
        [TempoRounds(settings.sources.tempo)],
        AFTER_REAL_RUN,
    )

    rounds = {row["round"]: row for row in read(spark, LAB_RUN_ROUNDS, settings.lake).collect()}
    assert (rounds[2]["minutes"], rounds[2]["model"], rounds[2]["tokens"]) == (0.0, None, 0)
    [run] = read(spark, LAB_RUN_TRACES, settings.lake).collect()
    assert (run["rounds"], run["model"], run["tokens"]) == (2, "claude-opus-5-5", 3214175)
    assert run["minutes"] == pytest.approx(1789.96 / 60)


def test_a_round_span_without_a_required_attribute_names_it() -> None:
    span = round_span(1, 1.5, "claude-opus-5-5")
    span["attributes"] = [found for found in span["attributes"] if found["key"] != "lab.cost_usd"]
    with pytest.raises(ValueError, match="lab.cost_usd"):
        lab_round(span)


@respx.mock
def test_a_window_without_lab_rounds_loads_nothing_since_tempo_leaves_out_empty_traces(settings: Settings) -> None:
    empty = {"metrics": fixture("tempo-rounds.json")["metrics"]}
    respx.get("https://tempo.test/api/search").respond(json=empty)
    page = TempoRounds(settings.sources.tempo).fetch(AFTER_REAL_RUN.isoformat(), AFTER_REAL_RUN)
    assert (page.records, page.watermark) == ([], AFTER_REAL_RUN.isoformat())
