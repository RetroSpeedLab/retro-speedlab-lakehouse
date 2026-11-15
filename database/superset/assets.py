import json
import uuid
from dataclasses import dataclass
from typing import Any

import yaml
from superset.app import create_app

NAMESPACE = uuid.UUID("5d0e4a8c-7f62-4c1b-9b8e-2f4a6d1c3e70")
VERSION = "1.0.0"
DATABASE = "Retro Speedlab Lake"
SQLALCHEMY_URI = "hive://retro-speedlab-thrift.thrift:10000/default?auth=NOSASL"
TEMPORAL_TYPES = ("TIMESTAMP", "DATE")
GRID_COLUMNS = 12
CHART_HEIGHT = 50
ROW_LIMIT = 10000
TIMESTAMP = "2026-11-15T00:00:00+00:00"
SAVED_SINCE_FIRST_CLEAR = "saved_since_first_clear_seconds"
COUNT = {"metric_name": "count", "verbose_name": "COUNT(*)", "metric_type": "count", "expression": "COUNT(*)"}


@dataclass(frozen=True, slots=True)
class Dataset:
    schema: str
    table: str
    columns: tuple[tuple[str, str], ...]

    @property
    def uuid(self) -> str:
        return stable_uuid(f"dataset/{self.schema}.{self.table}")


@dataclass(frozen=True, slots=True)
class Chart:
    name: str
    viz_type: str
    dataset: Dataset
    params: dict[str, Any]

    @property
    def uuid(self) -> str:
        return stable_uuid(f"chart/{self.name}")


@dataclass(frozen=True, slots=True)
class Dashboard:
    title: str
    slug: str
    rows: tuple[tuple[Chart, ...], ...]


def stable_uuid(name: str) -> str:
    return str(uuid.uuid5(NAMESPACE, name))


def sql_metric(expression: str, label: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def sql_filter(expression: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": expression, "clause": "WHERE"}


def all_time(column: str) -> dict[str, Any]:
    return {
        "expressionType": "SIMPLE",
        "subject": column,
        "operator": "TEMPORAL_RANGE",
        "comparator": "No filter",
        "clause": "WHERE",
    }


def table(dataset: Dataset, name: str, columns: list[str], order: str, where: list[dict[str, Any]]) -> Chart:
    params = {
        "query_mode": "raw",
        "all_columns": columns,
        "order_by_cols": [json.dumps([order, True])],
        "adhoc_filters": where,
        "row_limit": ROW_LIMIT,
        "server_page_length": 25,
        "include_search": True,
    }
    return Chart(name, "table", dataset, params)


def big_number(dataset: Dataset, name: str, expression: str) -> Chart:
    params = {"metric": sql_metric(expression, name), "adhoc_filters": [], "header_font_size": 0.4}
    return Chart(name, "big_number_total", dataset, params)


def line(dataset: Dataset, name: str, x_axis: str, grain: str, metric: dict[str, Any]) -> Chart:
    params = {
        "x_axis": x_axis,
        "time_grain_sqla": grain,
        "metrics": [metric],
        "groupby": ["game"],
        "adhoc_filters": [all_time(x_axis)],
        "row_limit": ROW_LIMIT,
        "show_legend": True,
        "markerEnabled": True,
    }
    return Chart(name, "echarts_timeseries_line", dataset, params)


def bar(dataset: Dataset, name: str, x_axis: str, metric: dict[str, Any]) -> Chart:
    params = {
        "x_axis": x_axis,
        "metrics": [metric],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": ROW_LIMIT,
        "orientation": "vertical",
        "show_value": True,
    }
    return Chart(name, "echarts_timeseries_bar", dataset, params)


GAME_SUMMARY = Dataset(
    "gold",
    "game_summary",
    (
        ("game", "STRING"),
        ("title", "STRING"),
        ("won", "BOOLEAN"),
        ("run_seconds", "DOUBLE"),
        ("record_seconds", "DOUBLE"),
        ("frames", "BIGINT"),
        ("levels", "INT"),
        ("run_updated_at", "TIMESTAMP"),
        ("levels_cleared", "BIGINT"),
        (SAVED_SINCE_FIRST_CLEAR, "DOUBLE"),
        ("costliest_level", "STRING"),
        ("costliest_frames", "BIGINT"),
        ("costliest_share", "DOUBLE"),
        ("first_at", "TIMESTAMP"),
        ("last_change_at", "TIMESTAMP"),
        ("history_points", "BIGINT"),
        ("lab_runs", "BIGINT"),
        ("lab_deployed", "BIGINT"),
        ("lab_minutes", "DOUBLE"),
        ("hours_searched", "DOUBLE"),
        ("saved_since_yesterday_seconds", "DOUBLE"),
        ("computed_at", "TIMESTAMP"),
    ),
)
RECORDS_DAILY = Dataset(
    "gold",
    "records_daily",
    (
        ("game", "STRING"),
        ("date", "DATE"),
        ("best_seconds", "DOUBLE"),
        ("record_seconds", "DOUBLE"),
        ("saved_seconds", "DOUBLE"),
    ),
)
IMPROVEMENT_CURVE = Dataset(
    "gold", "improvement_curve", (("game", "STRING"), ("hour", "TIMESTAMP"), ("seconds", "DOUBLE"))
)
LEVEL_COSTS = Dataset(
    "gold",
    "level_costs",
    (
        ("game", "STRING"),
        ("level", "STRING"),
        ("position", "INT"),
        ("frames", "BIGINT"),
        ("level_seconds", "DOUBLE"),
        ("first_seconds", "DOUBLE"),
        ("saved_seconds", "DOUBLE"),
        ("share", "DOUBLE"),
        ("costliest", "BOOLEAN"),
    ),
)
LAB_OUTCOMES = Dataset(
    "gold",
    "lab_outcomes",
    (
        ("game", "STRING"),
        ("outcome", "STRING"),
        ("runs", "BIGINT"),
        ("minutes", "DOUBLE"),
        ("cost_usd", "DOUBLE"),
        ("commits", "BIGINT"),
    ),
)
PLATFORM_TABLES = Dataset(
    "gold",
    "platform_tables",
    (
        ("table", "STRING"),
        ("layer", "STRING"),
        ("store", "STRING"),
        ("rows", "BIGINT"),
        ("bytes", "BIGINT"),
        ("files", "BIGINT"),
        ("version", "BIGINT"),
        ("modified_at", "TIMESTAMP"),
        ("measured_at", "TIMESTAMP"),
    ),
)
PLATFORM_JOBS = Dataset(
    "gold",
    "platform_jobs",
    (
        ("dag", "STRING"),
        ("run_id", "STRING"),
        ("job", "STRING"),
        ("started_at", "TIMESTAMP"),
        ("finished_at", "TIMESTAMP"),
        ("status", "STRING"),
        ("rows", "BIGINT"),
        ("error", "STRING"),
    ),
)
JOB_MINUTES = "(unix_timestamp(finished_at) - unix_timestamp(started_at)) / 60"

DASHBOARDS = (
    Dashboard(
        "Speedrun records",
        "speedrun-records",
        (
            (
                big_number(GAME_SUMMARY, "Games with a record", "COUNT(record_seconds)"),
                big_number(GAME_SUMMARY, "Seconds saved since the first clears", f"SUM({SAVED_SINCE_FIRST_CLEAR})"),
                big_number(GAME_SUMMARY, "Lab runs", "SUM(lab_runs)"),
            ),
            (
                table(
                    GAME_SUMMARY,
                    "Records",
                    [
                        "game",
                        "title",
                        "record_seconds",
                        "levels_cleared",
                        "levels",
                        SAVED_SINCE_FIRST_CLEAR,
                        "saved_since_yesterday_seconds",
                        "costliest_level",
                        "lab_runs",
                        "hours_searched",
                    ],
                    "game",
                    [],
                ),
            ),
            (
                line(RECORDS_DAILY, "Record per day", "date", "P1D", sql_metric("MIN(record_seconds)", "Record (s)")),
                line(IMPROVEMENT_CURVE, "Improvement curve", "hour", "PT1H", sql_metric("MIN(seconds)", "Run (s)")),
            ),
            (
                table(
                    LEVEL_COSTS,
                    "Costliest level per game",
                    ["game", "level", "frames", "level_seconds", "share"],
                    "game",
                    [sql_filter("costliest")],
                ),
                bar(LAB_OUTCOMES, "Lab outcomes", "outcome", sql_metric("SUM(runs)", "Lab runs")),
            ),
        ),
    ),
    Dashboard(
        "Lakehouse platform",
        "lakehouse-platform",
        (
            (
                table(
                    PLATFORM_TABLES,
                    "Tables",
                    ["table", "layer", "store", "rows", "bytes", "files", "version", "modified_at"],
                    "table",
                    [],
                ),
            ),
            (
                bar(PLATFORM_JOBS, "Average job minutes", "job", sql_metric(f"AVG({JOB_MINUTES})", "Minutes")),
                table(
                    PLATFORM_JOBS,
                    "Failed job runs",
                    ["started_at", "dag", "job", "error"],
                    "started_at",
                    [sql_filter("status = 'failed'")],
                ),
            ),
        ),
    ),
)


def database_config() -> dict[str, Any]:
    return {
        "database_name": DATABASE,
        "sqlalchemy_uri": SQLALCHEMY_URI,
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_csv_upload": False,
        "impersonate_user": False,
        "extra": {"allows_virtual_table_explore": True},
        "uuid": stable_uuid("database"),
        "version": VERSION,
    }


def column_config(name: str, column_type: str) -> dict[str, Any]:
    return {
        "column_name": name,
        "type": column_type,
        "is_dttm": column_type in TEMPORAL_TYPES,
        "is_active": True,
        "groupby": True,
        "filterable": True,
    }


def dataset_config(dataset: Dataset) -> dict[str, Any]:
    temporal = [name for name, column_type in dataset.columns if column_type in TEMPORAL_TYPES]
    return {
        "table_name": dataset.table,
        "schema": dataset.schema,
        "main_dttm_col": temporal[0] if temporal else None,
        "sql": None,
        "params": None,
        "filter_select_enabled": True,
        "uuid": dataset.uuid,
        "columns": [column_config(name, column_type) for name, column_type in dataset.columns],
        "metrics": [COUNT],
        "version": VERSION,
        "database_uuid": stable_uuid("database"),
    }


def chart_config(chart: Chart) -> dict[str, Any]:
    return {
        "slice_name": chart.name,
        "viz_type": chart.viz_type,
        "params": chart.params | {"viz_type": chart.viz_type},
        "query_context": None,
        "cache_timeout": None,
        "uuid": chart.uuid,
        "version": VERSION,
        "dataset_uuid": chart.dataset.uuid,
    }


def position(dashboard: Dashboard) -> dict[str, Any]:
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": dashboard.title}},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    for number, row in enumerate(dashboard.rows):
        row_id = f"ROW-{number}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [f"CHART-{chart.uuid}" for chart in row],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart in row:
            layout[f"CHART-{chart.uuid}"] = {
                "type": "CHART",
                "id": f"CHART-{chart.uuid}",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "width": GRID_COLUMNS // len(row),
                    "height": CHART_HEIGHT,
                    "chartId": 0,
                    "uuid": chart.uuid,
                    "sliceName": chart.name,
                },
            }
    return layout


def dashboard_config(dashboard: Dashboard) -> dict[str, Any]:
    return {
        "dashboard_title": dashboard.title,
        "slug": dashboard.slug,
        "uuid": stable_uuid(f"dashboard/{dashboard.slug}"),
        "position": position(dashboard),
        "metadata": {"native_filter_configuration": [], "cross_filters_enabled": True, "refresh_frequency": 0},
        "published": True,
        "version": VERSION,
    }


def bundle() -> dict[str, str]:
    charts = {chart.uuid: chart for dashboard in DASHBOARDS for row in dashboard.rows for chart in row}
    datasets = {chart.dataset.uuid: chart.dataset for chart in charts.values()}
    configs: dict[str, dict[str, Any]] = {
        "metadata.yaml": {"version": VERSION, "type": "Dashboard", "timestamp": TIMESTAMP},
        "databases/lake.yaml": database_config(),
    }
    configs |= {f"datasets/lake/{dataset.table}.yaml": dataset_config(dataset) for dataset in datasets.values()}
    configs |= {f"charts/{chart.uuid}.yaml": chart_config(chart) for chart in charts.values()}
    configs |= {f"dashboards/{dashboard.slug}.yaml": dashboard_config(dashboard) for dashboard in DASHBOARDS}
    return {path: yaml.safe_dump(config) for path, config in configs.items()}


def main() -> None:
    with create_app().app_context():
        from superset.commands.importers.v1.examples import ImportExamplesCommand

        ImportExamplesCommand(bundle(), overwrite=True).run()


if __name__ == "__main__":
    main()
