from datetime import UTC, datetime

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType
from spark_data_quality import CheckStatus, SuiteReport

from retro_speedlab_lakehouse.lake.catalog import QUALITY_REPORTS, Table
from retro_speedlab_lakehouse.lake.tables import append_rows
from retro_speedlab_lakehouse.quality.suites import suite_of
from retro_speedlab_lakehouse.settings import LakeSettings

REPORT = StructType(
    [
        StructField("table", StringType(), False),
        StructField("run", StringType(), False),
        StructField("status", StringType(), False),
        StructField("checks", IntegerType(), False),
        StructField("failed", IntegerType(), False),
        StructField("rows", LongType(), True),
        StructField("checked_at", TimestampType(), False),
        StructField("report", StringType(), False),
    ]
)


class QualityFailure(RuntimeError):
    pass


def rows_of(report: SuiteReport) -> int | None:
    counted = [result.total_rows for result in report.results if result.total_rows is not None]
    return counted[0] if counted else None


class Gate:
    def __init__(self, run: str) -> None:
        self.run = run
        self.reports: list[tuple[str, SuiteReport, datetime]] = []

    def check(self, table: Table, rows: DataFrame) -> None:
        suite = suite_of(table)
        if suite is None:
            return
        report = suite.validate(rows)
        self.reports.append((table.full_name, report, datetime.now(UTC)))
        if report.status is not CheckStatus.PASS:
            failed = [result.description for result in report.results if result.status is not CheckStatus.PASS]
            raise QualityFailure(f"{table.full_name} failed its quality gate: {'; '.join(failed)}")

    def flush(self, spark: SparkSession, lake: LakeSettings) -> None:
        if not self.reports:
            return
        rows = [
            (
                name,
                self.run,
                str(report.status),
                len(report.results),
                sum(result.status is not CheckStatus.PASS for result in report.results),
                rows_of(report),
                checked_at,
                report.model_dump_json(),
            )
            for name, report, checked_at in self.reports
        ]
        append_rows(QUALITY_REPORTS, lake, spark.createDataFrame(rows, REPORT))
        logger.info(f"Kept {len(rows)} quality reports of {self.run}")
        self.reports = []
