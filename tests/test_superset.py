from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from bundle import GRID_COLUMNS, bundle, cockpit_charts, cockpit_datasets
from charts import Chart, Dataset
from dashboards import DASHBOARDS
from lake_datasets import LAYERS, lake_datasets
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.bandit.allocation import ALLOCATION
from retro_speedlab_lakehouse.bandit.holdout import RESULT as BANDIT
from retro_speedlab_lakehouse.batch.jobs import ALL_TABLES
from retro_speedlab_lakehouse.batch.platform import JOB_RUN, TABLE_STATS
from retro_speedlab_lakehouse.cockpit.days import SEARCH_METRICS, dense, search_days
from retro_speedlab_lakehouse.cockpit.hours import audience_hours, search_hours
from retro_speedlab_lakehouse.engagement.channel import channel_outcomes
from retro_speedlab_lakehouse.engagement.jobs import RESULT as ENGAGEMENT
from retro_speedlab_lakehouse.engagement.ranking import RANKING
from retro_speedlab_lakehouse.engagement.stream import chat_minutes, exposure_rows, twitch_minutes
from retro_speedlab_lakehouse.experiments.job import RESULT as EXPERIMENTS
from retro_speedlab_lakehouse.experiments.roi import ROI
from retro_speedlab_lakehouse.lake.catalog import Layer
from retro_speedlab_lakehouse.promotion.roi import PROMOTION
from retro_speedlab_lakehouse.quality.gate import REPORT
from retro_speedlab_lakehouse.sources.bronze import RECORD
from retro_speedlab_lakehouse.sources.silver import DAILY_COST, ITCH_DAY, NUMBER, daily, newest
from retro_speedlab_lakehouse.stream import rows, search_rows
from retro_speedlab_lakehouse.stream.status import STATUS

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)


def test_every_catalog_table_gets_one_described_dataset() -> None:
    listing = {layer.value: [table.name for table in ALL_TABLES if table.layer is layer] for layer in Layer}
    datasets = lake_datasets(listing)
    assert set(LAYERS) == {layer.value for layer in Layer}
    assert sorted(f"{dataset.schema}.{dataset.table}" for dataset in datasets) == sorted(
        table.full_name for table in ALL_TABLES
    )
    assert len({dataset.uuid for dataset in datasets}) == len(ALL_TABLES)
    assert all(f"{dataset.schema}.{dataset.table}" in dataset.description for dataset in datasets)


def test_the_bundle_holds_every_chart_dataset_and_dashboard() -> None:
    files = {path: yaml.safe_load(text) for path, text in bundle().items()}
    charts = [chart for dashboard in DASHBOARDS for chart in dashboard.charts]
    assert len(cockpit_charts()) == len(charts)
    assert {f"datasets/{chart.dataset.uuid}.yaml" for chart in charts} <= set(files)
    assert {f"charts/{chart.uuid}.yaml" for chart in charts} <= set(files)
    assert all(files[f"charts/{chart.uuid}.yaml"]["dataset_uuid"] == chart.dataset.uuid for chart in charts)
    for dashboard in DASHBOARDS:
        layout = files[f"dashboards/{dashboard.slug}.yaml"]["position"]
        assert {f"CHART-{chart.uuid}" for chart in dashboard.charts} <= set(layout)
        widths = [
            sum(layout[child]["meta"]["width"] for child in layout[row]["children"])
            for row in layout
            if row.startswith("ROW-")
        ]
        assert max(widths) <= GRID_COLUMNS


def test_every_chart_names_only_columns_of_its_dataset() -> None:
    for chart in cockpit_charts().values():
        assert set(chart.columns) <= chart.dataset.column_names, chart.name


def code_tables(spark: SparkSession) -> dict[str, DataFrame]:
    nothing = spark.createDataFrame([], rows.KAFKA_ROW)
    backend = rows.parsed(nothing)
    search = search_rows.bronze_rows(search_rows.parsed(nothing))
    records = spark.createDataFrame([], RECORD)
    return {
        "live.live_run": rows.live_run(backend),
        "live.search_pulse": search_rows.search_pulse(search, NOW),
        "live.policy_pulse": search_rows.policy_pulse(search, NOW),
        "live.stream_status": spark.createDataFrame([], STATUS),
        "live.host_metrics": rows.host_status(backend),
        "silver.host_metrics": rows.host_history(backend),
        "silver.lab_runs": rows.lab_runs(backend),
        "silver.langfuse_daily": daily(records, DAILY_COST),
        "silver.itch_daily": daily(records, ITCH_DAY),
        "gold.source_numbers": newest(records, "numbers", NUMBER).withColumn("computed_at", F.lit(NOW)),
        "gold.platform_jobs": spark.createDataFrame([], JOB_RUN),
        "gold.platform_tables": spark.createDataFrame([], TABLE_STATS),
        "gold.quality_reports": spark.createDataFrame([], REPORT),
        "gold.quality_failures": spark.createDataFrame([], REPORT),
        "gold.search_allocation": spark.createDataFrame([], ALLOCATION),
        "gold.search_bandit": spark.createDataFrame([], BANDIT),
        "gold.content_ranking": spark.createDataFrame([], RANKING),
        "gold.engagement_experiments": spark.createDataFrame([], ENGAGEMENT),
        "gold.twitch_channel_outcomes": channel_outcomes(
            exposure_rows(backend), twitch_minutes(search), chat_minutes(search)
        ),
        "gold.lab_experiments": spark.createDataFrame([], EXPERIMENTS),
        "gold.lab_roi": spark.createDataFrame([], ROI),
        "gold.promotion_roi": spark.createDataFrame([], PROMOTION),
        "gold.cockpit_days": dense(spark, [search_days(search, NOW)], list(SEARCH_METRICS), NOW),
        "gold.search_hours": search_hours(search, NOW),
        "gold.audience_hours": audience_hours(search, NOW),
    }


def declared(spark: SparkSession, dataset: Dataset) -> DataFrame:
    return spark.createDataFrame([], ", ".join(f"`{name}` {column_type}" for name, column_type in dataset.columns))


def columns_of(frame: DataFrame) -> dict[str, str]:
    return {field.name: field.dataType.simpleString().upper() for field in frame.schema.fields}


def physical(dataset: Dataset) -> str:
    return f"{dataset.schema}.{dataset.name}"


@pytest.fixture
def lake_tables(spark: SparkSession, tmp_path: Path) -> Iterator[dict[str, DataFrame]]:
    tables = code_tables(spark)
    for dataset in cockpit_datasets().values():
        if dataset.sql is None and physical(dataset) not in tables:
            tables[physical(dataset)] = declared(spark, dataset)
    for name, frame in tables.items():
        location = str(tmp_path / name)
        frame.write.format("delta").save(location)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {name.split('.')[0]}")
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"CREATE TABLE {name} USING DELTA LOCATION '{location}'")
    yield tables
    for name in tables:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def source(dataset: Dataset) -> str:
    return physical(dataset) if dataset.sql is None else f"({dataset.sql}) AS dataset"


def compile_chart(spark: SparkSession, chart: Chart) -> None:
    named = ", ".join(f"`{column}`" for column in chart.columns) or "1"
    metrics = ", ".join(chart.metrics) or "1"
    filters = " AND ".join(f"({expression})" for expression in chart.filters) or "true"
    for query in (
        f"SELECT {named} FROM {source(chart.dataset)}",
        f"SELECT {metrics} FROM {source(chart.dataset)} WHERE {filters}",
    ):
        try:
            spark.sql(f"{query} LIMIT 0").collect()
        except AnalysisException as error:
            raise AssertionError(f"{chart.name}: {query}") from error


def test_every_dataset_and_chart_runs_on_the_lake_schemas(
    spark: SparkSession, lake_tables: dict[str, DataFrame]
) -> None:
    for dataset in cockpit_datasets().values():
        if dataset.sql is not None:
            assert columns_of(spark.sql(dataset.sql)) == dict(dataset.columns), dataset.name
        else:
            assert dict(dataset.columns).items() <= columns_of(lake_tables[physical(dataset)]).items(), dataset.name
    for chart in cockpit_charts().values():
        compile_chart(spark, chart)
