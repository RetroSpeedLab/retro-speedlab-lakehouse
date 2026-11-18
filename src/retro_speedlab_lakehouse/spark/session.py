from pyspark.sql import SparkSession

from retro_speedlab_lakehouse.settings import LakeSettings, SparkSettings

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
S3A_FILESYSTEM = "org.apache.hadoop.fs.s3a.S3AFileSystem"
MULTIPART_BYTES = "16M"
LOOPBACK = "127.0.0.1"
IDLE_HEAP_RETURN = "-XX:G1PeriodicGCInterval=60000"
SNAPSHOT_STALENESS = "60s"
PARQUET_BATCH_ROWS = "512"


def lake_options(lake: LakeSettings) -> dict[str, str]:
    return {
        "spark.hadoop.fs.s3a.impl": S3A_FILESYSTEM,
        "spark.hadoop.fs.s3a.endpoint": lake.endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": "garage",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": lake.access_key,
        "spark.hadoop.fs.s3a.secret.key": lake.secret_key,
        "spark.hadoop.fs.s3a.multipart.size": MULTIPART_BYTES,
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.change.detection.mode": "none",
        "spark.delta.logStore.s3a.impl": "io.delta.storage.S3SingleDriverLogStore",
    }


def session_builder(name: str, spark: SparkSettings, lake: LakeSettings) -> SparkSession.Builder:
    builder = (
        SparkSession.builder.appName(name)  # type: ignore[attr-defined]
        .master(spark.master)
        .config("spark.driver.memory", spark.driver_memory)
        .config("spark.driver.extraJavaOptions", IDLE_HEAP_RETURN)
        .config("spark.sql.shuffle.partitions", str(spark.shuffle_partitions))
        .config("spark.sql.parquet.columnarReaderBatchSize", PARQUET_BATCH_ROWS)
        .config("spark.databricks.delta.snapshotPartitions", str(spark.shuffle_partitions))
        .config("spark.databricks.delta.stalenessLimit", SNAPSHOT_STALENESS)
        .config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", LOOPBACK)
        .config("spark.driver.bindAddress", LOOPBACK)
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
    )
    if spark.jars_dir is not None:
        builder = builder.config("spark.jars", ",".join(str(jar) for jar in sorted(spark.jars_dir.glob("*.jar"))))
    if spark.packages:
        builder = builder.config("spark.jars.packages", ",".join(spark.packages))
    for key, value in lake_options(lake).items():
        builder = builder.config(key, value)
    return builder


def spark_session(name: str, spark: SparkSettings, lake: LakeSettings) -> SparkSession:
    return session_builder(name, spark, lake).getOrCreate()
