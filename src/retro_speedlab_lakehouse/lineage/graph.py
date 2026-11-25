from datetime import datetime

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.lake.catalog import LINEAGE_EDGES, LINEAGE_EVENTS, LINEAGE_NODES
from retro_speedlab_lakehouse.lake.tables import read
from retro_speedlab_lakehouse.lake.write import merge, replace_counted
from retro_speedlab_lakehouse.lineage import observed
from retro_speedlab_lakehouse.lineage.events import COMPLETE, FAIL, LINEAGE_EVENT, RUNNING, LineageEvent
from retro_speedlab_lakehouse.lineage.topology import PROCESSES_DAG, externals, processes
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import Settings

NODE = StructType([StructField("node", StringType(), False), StructField("kind", StringType(), False)])
EDGE = StructType(
    [
        StructField("source", StringType(), False),
        StructField("target", StringType(), False),
        StructField("job", StringType(), False),
        StructField("last_run_at", TimestampType(), False),
    ]
)
UNOBSERVED_JOBS = ("platform", "maintenance")


def kind_of(node: str) -> str:
    if ":" in node:
        return node.partition(":")[0]
    return node.partition(".")[0]


def record_events(spark: SparkSession, settings: Settings, gate: Gate, events: list[LineageEvent]) -> None:
    merge(LINEAGE_EVENTS, settings.lake, spark.createDataFrame([event.row for event in events], LINEAGE_EVENT), gate)


def record_job_lineage(
    spark: SparkSession, settings: Settings, gate: Gate, dag: str, run_id: str, job: str, failed: bool, now: datetime
) -> None:
    if job in UNOBSERVED_JOBS:
        return
    outside_inputs, outside_outputs = externals(settings).get(job, ((), ()))
    event_type = FAIL if failed else COMPLETE
    inputs = (*outside_inputs, *observed.inputs())
    outputs = (*observed.outputs(), *outside_outputs)
    record_events(spark, settings, gate, [LineageEvent(dag, run_id, job, event_type, now, inputs, outputs)])


def process_events(settings: Settings, now: datetime) -> list[LineageEvent]:
    return [
        LineageEvent(PROCESSES_DAG, process.name, process.name, RUNNING, now, process.inputs, process.outputs)
        for process in processes(settings)
    ]


def newest_by_job(events: list[Row]) -> list[Row]:
    ordered = sorted((event for event in events if event["event_type"] != FAIL), key=lambda event: event["event_time"])
    return list({event["job"]: event for event in ordered}.values())


def edges_of(events: list[Row]) -> dict[tuple[str, str, str], datetime]:
    return {
        (source, target, event["job"]): event["event_time"]
        for event in newest_by_job(events)
        for source in event["inputs"]
        for target in event["outputs"]
    }


def build_lineage(spark: SparkSession, settings: Settings, gate: Gate, now: datetime) -> int:
    record_events(spark, settings, gate, process_events(settings, now))
    edges = edges_of(read(spark, LINEAGE_EVENTS, settings.lake).collect())
    nodes = {node for source, target, _ in edges for node in (source, target)}
    node_rows = spark.createDataFrame([(node, kind_of(node)) for node in sorted(nodes)], NODE)
    edge_rows = spark.createDataFrame([(*edge, at) for edge, at in sorted(edges.items())], EDGE)
    return replace_counted(LINEAGE_NODES, settings.lake, node_rows, gate) + replace_counted(
        LINEAGE_EDGES, settings.lake, edge_rows, gate
    )
