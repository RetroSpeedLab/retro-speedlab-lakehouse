import signal
import threading
from pathlib import Path
from types import FrameType
from typing import Annotated

import typer
from loguru import logger

from retro_speedlab_lakehouse.batch.jobs import run_job
from retro_speedlab_lakehouse.relay.producer import relay as run_relay
from retro_speedlab_lakehouse.relay.topics import create_topics
from retro_speedlab_lakehouse.settings import load_settings
from retro_speedlab_lakehouse.spark.session import spark_session
from retro_speedlab_lakehouse.stream.job import start

app = typer.Typer(no_args_is_help=True)


@app.command()
def relay(config: Path) -> None:
    settings = load_settings(config)
    stopped = threading.Event()

    def stop(signum: int, frame: FrameType | None) -> None:
        logger.info(f"Signal {signum}: the relay stops")
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    run_relay(settings, stopped)


@app.command()
def topics(config: Path) -> None:
    create_topics(load_settings(config).kafka)


@app.command()
def stream(config: Path) -> None:
    settings = load_settings(config)
    spark = spark_session("lakehouse-stream", settings.spark, settings.lake)
    start(spark, settings)
    spark.streams.awaitAnyTermination()


@app.command()
def job(
    name: str,
    config: Path,
    dag: Annotated[str, typer.Option()],
    run_id: Annotated[str, typer.Option()],
) -> None:
    settings = load_settings(config)
    spark = spark_session(f"lakehouse-{name}", settings.spark, settings.lake)
    try:
        run_job(spark, settings, name, dag, run_id)
    finally:
        spark.stop()
