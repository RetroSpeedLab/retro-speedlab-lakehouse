import signal
import threading
from types import FrameType
from typing import Any

from loguru import logger
from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.batch.jobs import ALL_TABLES
from retro_speedlab_lakehouse.database.register import register
from retro_speedlab_lakehouse.settings import DatabaseSettings, ThriftSettings
from retro_speedlab_lakehouse.spark.session import session_builder

ALL_INTERFACES = "0.0.0.0"
EXIT_ON_ERROR = True
DERBY_LOG = "derby.log"


def hive_options(thrift: ThriftSettings) -> dict[str, str]:
    if thrift.spark.jars_dir is None:
        raise ValueError("The Thrift Server needs spark.jars_dir: its sessions load Delta from the driver class path")
    return {
        "spark.driver.extraClassPath": str(thrift.spark.jars_dir / "*"),
        "spark.driver.extraJavaOptions": f"-Dderby.stream.error.file={thrift.metastore.parent / DERBY_LOG}",
        "spark.sql.warehouse.dir": str(thrift.warehouse),
        "spark.hadoop.javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={thrift.metastore};create=true",
        "spark.hadoop.hive.server2.thrift.port": str(thrift.port),
        "spark.hadoop.hive.server2.thrift.bind.host": ALL_INTERFACES,
        "spark.hadoop.hive.server2.authentication": "NOSASL",
        "spark.hadoop.hive.server2.enable.doAs": "false",
    }


def thrift_session(settings: DatabaseSettings) -> SparkSession:
    builder = session_builder("lakehouse-thrift", settings.thrift.spark, settings.lake).enableHiveSupport()
    for key, value in hive_options(settings.thrift).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def start_thrift_server(spark: SparkSession) -> None:
    jvm: Any = spark.sparkContext._jvm  # pyright: ignore[reportPrivateUsage]
    if jvm is None:
        raise RuntimeError("The Spark session has no JVM")
    server = jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
    server.startWithSparkSession(spark._jsparkSession, EXIT_ON_ERROR)  # pyright: ignore[reportPrivateUsage]


def serve(settings: DatabaseSettings) -> None:
    spark = thrift_session(settings)
    stopped = threading.Event()

    def stop(signum: int, frame: FrameType | None) -> None:
        logger.info(f"Signal {signum}: the Thrift Server stops")
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    register(spark, ALL_TABLES, settings.lake)
    start_thrift_server(spark)
    logger.info(f"The Thrift Server listens on port {settings.thrift.port}")
    while not stopped.wait(settings.thrift.register_seconds):
        register(spark, ALL_TABLES, settings.lake)
    spark.stop()
