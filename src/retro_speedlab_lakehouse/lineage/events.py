import json
from dataclasses import dataclass
from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType

PRODUCER = "https://github.com/RetroSpeedLab/retro-speedlab-lakehouse"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
BRONZE_NAMESPACE = "black-omen"
LAKE_NAMESPACE = "lake"
COMPLETE = "COMPLETE"
FAIL = "FAIL"
RUNNING = "RUNNING"
LINEAGE_EVENT = StructType(
    [
        StructField("run", StringType(), False),
        StructField("dag", StringType(), False),
        StructField("job", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("inputs", ArrayType(StringType()), False),
        StructField("outputs", ArrayType(StringType()), False),
        StructField("event", StringType(), False),
    ]
)


def dataset(node: str) -> dict[str, str]:
    kind, separator, name = node.partition(":")
    if separator:
        return {"namespace": kind, "name": name}
    return {"namespace": BRONZE_NAMESPACE if node.startswith("bronze.") else LAKE_NAMESPACE, "name": node}


@dataclass(frozen=True, slots=True)
class LineageEvent:
    dag: str
    run_id: str
    job: str
    event_type: str
    event_time: datetime
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]

    @property
    def run(self) -> str:
        return str(uuid5(NAMESPACE_URL, f"{self.dag}/{self.run_id}/{self.job}"))

    @property
    def open_lineage(self) -> str:
        return json.dumps(
            {
                "eventType": self.event_type,
                "eventTime": self.event_time.isoformat(),
                "producer": PRODUCER,
                "schemaURL": SCHEMA_URL,
                "run": {"runId": self.run},
                "job": {"namespace": self.dag, "name": self.job},
                "inputs": [dataset(node) for node in self.inputs],
                "outputs": [dataset(node) for node in self.outputs],
            }
        )

    @property
    def row(self) -> tuple[object, ...]:
        return (
            self.run,
            self.dag,
            self.job,
            self.event_type,
            self.event_time,
            list(self.inputs),
            list(self.outputs),
            self.open_lineage,
        )
