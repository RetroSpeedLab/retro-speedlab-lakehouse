from typing import Any

import yaml
from charts import COLOR_SCHEME, Chart, Dataset
from dashboards import DASHBOARDS, Dashboard, Row
from lake_datasets import stable_uuid

VERSION = "1.0.0"
DATABASE = "Retro Speedlab Lake"
SQLALCHEMY_URI = "hive://retro-speedlab-thrift:10000/default?auth=NOSASL"
TEMPORAL_TYPES = ("TIMESTAMP", "DATE")
GRID_COLUMNS = 12
TIMESTAMP = "2026-11-16T00:00:00+00:00"
COUNT = {"metric_name": "count", "verbose_name": "COUNT(*)", "metric_type": "count", "expression": "COUNT(*)"}
STATUS_COLORS = {"success": "#0ca30c", "failed": "#d03b3b"}
TRANSPARENT = "BACKGROUND_TRANSPARENT"


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
        "table_name": dataset.name,
        "schema": dataset.schema,
        "description": dataset.description,
        "main_dttm_col": temporal[0] if temporal else None,
        "sql": dataset.sql,
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


def chart_layout(chart: Chart, row: Row, parents: list[str]) -> dict[str, Any]:
    return {
        "type": "CHART",
        "id": f"CHART-{chart.uuid}",
        "children": [],
        "parents": parents,
        "meta": {
            "width": GRID_COLUMNS // len(row.charts),
            "height": row.height,
            "chartId": 0,
            "uuid": chart.uuid,
            "sliceName": chart.name,
        },
    }


def position(dashboard: Dashboard) -> dict[str, Any]:
    grid: list[str] = []
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": dashboard.title}},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": grid, "parents": ["ROOT_ID"]},
    }
    for section_number, section in enumerate(dashboard.sections):
        header_id = f"HEADER-{section_number}"
        grid.append(header_id)
        meta = {"text": section.title, "headerSize": "MEDIUM_HEADER", "background": TRANSPARENT}
        layout[header_id] = {"type": "HEADER", "id": header_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"]}
        layout[header_id]["meta"] = meta
        for row_number, row in enumerate(section.rows):
            row_id = f"ROW-{section_number}-{row_number}"
            grid.append(row_id)
            children = [f"CHART-{chart.uuid}" for chart in row.charts]
            layout[row_id] = {"type": "ROW", "id": row_id, "children": children, "parents": ["ROOT_ID", "GRID_ID"]}
            layout[row_id]["meta"] = {"background": TRANSPARENT}
            for chart in row.charts:
                layout[f"CHART-{chart.uuid}"] = chart_layout(chart, row, ["ROOT_ID", "GRID_ID", row_id])
    return layout


def dashboard_config(dashboard: Dashboard) -> dict[str, Any]:
    metadata = {
        "native_filter_configuration": [],
        "cross_filters_enabled": True,
        "refresh_frequency": dashboard.refresh_seconds,
        "color_scheme": COLOR_SCHEME,
        "label_colors": STATUS_COLORS,
    }
    return {
        "dashboard_title": dashboard.title,
        "slug": dashboard.slug,
        "uuid": stable_uuid(f"dashboard/{dashboard.slug}"),
        "position": position(dashboard),
        "metadata": metadata,
        "published": True,
        "version": VERSION,
    }


def cockpit_charts() -> dict[str, Chart]:
    return {chart.uuid: chart for dashboard in DASHBOARDS for chart in dashboard.charts}


def cockpit_datasets() -> dict[str, Dataset]:
    return {chart.dataset.uuid: chart.dataset for chart in cockpit_charts().values()}


def bundle() -> dict[str, str]:
    configs: dict[str, dict[str, Any]] = {
        "metadata.yaml": {"version": VERSION, "type": "Dashboard", "timestamp": TIMESTAMP},
        "databases/lake.yaml": database_config(),
    }
    configs |= {f"datasets/{key}.yaml": dataset_config(dataset) for key, dataset in cockpit_datasets().items()}
    configs |= {f"charts/{chart.uuid}.yaml": chart_config(chart) for chart in cockpit_charts().values()}
    configs |= {f"dashboards/{dashboard.slug}.yaml": dashboard_config(dashboard) for dashboard in DASHBOARDS}
    return {path: yaml.safe_dump(config) for path, config in configs.items()}
