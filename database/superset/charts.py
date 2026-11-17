import json
from dataclasses import dataclass
from typing import Any

from lake_datasets import LakeDataset, stable_uuid

ROW_LIMIT = 10000
RANKED_LIMIT = 25
PAGE_LENGTH = 25
COLOR_SCHEME = "retroSpeedlab"
VIRTUAL_SCHEMA = "main"
COMPARED = "vs the 24 h before"
NO_FILTER = "No filter"
FINDING_COLUMNS = ("rank", "area", "verdict", "statement", "action")
FINDING_TEMPLATE = (
    '<div class="findings">{{#each data}}<div class="finding {{verdict}}">'
    '<span class="verdict">{{verdict}}</span><span class="area">{{area}}</span>{{statement}}'
    '{{#if action}}<div class="action">Next step: {{action}}</div>{{/if}}</div>{{/each}}</div>'
)
FINDING_STYLE = """
.findings { font-size: 14px; line-height: 1.45; }
.finding { border-left: 6px solid; border-radius: 4px; padding: 6px 10px; margin: 0 0 6px; color: #1f1f1f; }
.finding.act { border-color: #d03b3b; background: #fdecec; }
.finding.watch { border-color: #d08700; background: #fff4dc; }
.finding.good { border-color: #0ca30c; background: #e9f6e9; }
.finding .verdict { display: inline-block; min-width: 4.5em; font-weight: 700; text-transform: uppercase; }
.finding.act .verdict { color: #a11d1d; }
.finding.watch .verdict { color: #7a4f00; }
.finding.good .verdict { color: #086b08; }
.finding .area { display: inline-block; min-width: 7.5em; color: #555; }
.finding .action { margin: 2px 0 0 12em; font-weight: 600; }
"""

Columns = tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class Dataset:
    name: str
    schema: str
    sql: str | None
    columns: Columns
    description: str
    uuid: str

    @property
    def column_names(self) -> set[str]:
        return {name for name, _ in self.columns}


def lake_table(schema: str, name: str, columns: Columns) -> Dataset:
    lake = LakeDataset(schema, name)
    return Dataset(name, schema, None, columns, lake.description, lake.uuid)


def virtual(name: str, sql: str, columns: Columns, description: str) -> Dataset:
    return Dataset(name, VIRTUAL_SCHEMA, sql, columns, description, stable_uuid(f"dataset/virtual/{name}"))


@dataclass(frozen=True, slots=True)
class Chart:
    name: str
    viz_type: str
    dataset: Dataset
    columns: tuple[str, ...]
    metrics: tuple[str, ...]
    filters: tuple[str, ...]
    params: dict[str, Any]

    @property
    def uuid(self) -> str:
        return stable_uuid(f"chart/{self.name}")


def sql_metric(expression: str, label: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def sql_filter(expression: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": expression, "clause": "WHERE"}


def all_time(column: str) -> dict[str, Any]:
    return {
        "expressionType": "SIMPLE",
        "subject": column,
        "operator": "TEMPORAL_RANGE",
        "comparator": NO_FILTER,
        "clause": "WHERE",
    }


def where(filters: tuple[str, ...]) -> list[dict[str, Any]]:
    return [sql_filter(expression) for expression in filters]


def table(dataset: Dataset, name: str, columns: tuple[str, ...], order: str, filters: tuple[str, ...]) -> Chart:
    descending = order.startswith("-")
    ordered = order.removeprefix("-")
    params = {
        "query_mode": "raw",
        "all_columns": list(columns),
        "order_by_cols": [json.dumps([ordered, not descending])],
        "adhoc_filters": where(filters),
        "row_limit": ROW_LIMIT,
        "server_page_length": PAGE_LENGTH,
        "include_search": True,
    }
    return Chart(name, "table", dataset, (*columns, ordered), (), filters, params)


def findings(dataset: Dataset, name: str, filters: tuple[str, ...]) -> Chart:
    params = {
        "query_mode": "raw",
        "all_columns": list(FINDING_COLUMNS),
        "order_by_cols": [json.dumps(["rank", True]), json.dumps(["area", True])],
        "adhoc_filters": where(filters),
        "row_limit": ROW_LIMIT,
        "handlebarsTemplate": FINDING_TEMPLATE,
        "styleTemplate": FINDING_STYLE,
    }
    return Chart(name, "handlebars", dataset, FINDING_COLUMNS, (), filters, params)


def number(dataset: Dataset, name: str, expression: str, filters: tuple[str, ...]) -> Chart:
    params = {
        "metric": sql_metric(expression, name),
        "adhoc_filters": where(filters),
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
    }
    return Chart(name, "big_number_total", dataset, (), (expression,), filters, params)


def kpi(dataset: Dataset, name: str, x_axis: str, expression: str, filters: tuple[str, ...]) -> Chart:
    params = {
        "x_axis": x_axis,
        "time_grain_sqla": "P1D",
        "metric": sql_metric(expression, name),
        "adhoc_filters": [all_time(x_axis), *where(filters)],
        "compare_lag": 1,
        "compare_suffix": COMPARED,
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "show_timestamp": False,
        "rolling_type": "None",
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "color_picker": {"r": 42, "g": 120, "b": 214, "a": 1},
    }
    return Chart(name, "big_number", dataset, (x_axis,), (expression,), filters, params)


def over_time(name: str, x_axis: str, grain: str, metric: str, groupby: tuple[str, ...]) -> dict[str, Any]:
    return {
        "x_axis": x_axis,
        "time_grain_sqla": grain,
        "metrics": [sql_metric(metric, name)],
        "groupby": list(groupby),
        "row_limit": ROW_LIMIT,
        "show_legend": bool(groupby),
        "rich_tooltip": True,
        "color_scheme": COLOR_SCHEME,
    }


def trend(
    dataset: Dataset,
    name: str,
    x_axis: str,
    grain: str,
    metric: str,
    groupby: tuple[str, ...],
    filters: tuple[str, ...],
) -> Chart:
    params = over_time(name, x_axis, grain, metric, groupby)
    params |= {"adhoc_filters": [all_time(x_axis), *where(filters)], "markerEnabled": False}
    return Chart(name, "echarts_timeseries_line", dataset, (x_axis, *groupby), (metric,), filters, params)


def columns_over_time(
    dataset: Dataset,
    name: str,
    x_axis: str,
    grain: str,
    metric: str,
    groupby: tuple[str, ...],
    filters: tuple[str, ...],
) -> Chart:
    params = over_time(name, x_axis, grain, metric, groupby)
    params |= {"adhoc_filters": [all_time(x_axis), *where(filters)], "stack": "Stack" if groupby else None}
    return Chart(name, "echarts_timeseries_bar", dataset, (x_axis, *groupby), (metric,), filters, params)


def ranked(
    dataset: Dataset, name: str, dimension: str, metric: str, groupby: tuple[str, ...], filters: tuple[str, ...]
) -> Chart:
    params = {
        "x_axis": dimension,
        "metrics": [sql_metric(metric, name)],
        "groupby": list(groupby),
        "adhoc_filters": where(filters),
        "row_limit": RANKED_LIMIT,
        "orientation": "horizontal",
        "x_axis_sort": name,
        "x_axis_sort_asc": False,
        "x_axis_sort_series": "sum",
        "stack": "Stack" if groupby else None,
        "show_legend": bool(groupby),
        "show_value": not groupby,
        "color_scheme": COLOR_SCHEME,
    }
    return Chart(name, "echarts_timeseries_bar", dataset, (dimension, *groupby), (metric,), filters, params)
