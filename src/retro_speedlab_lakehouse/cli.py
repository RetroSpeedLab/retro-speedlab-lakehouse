import signal
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Annotated

import typer
from loguru import logger

from retro_speedlab_lakehouse.batch.jobs import run_job
from retro_speedlab_lakehouse.batch.night import NIGHT_END, NIGHT_START, outside_the_night
from retro_speedlab_lakehouse.chat.watch import watch_chat
from retro_speedlab_lakehouse.database.thrift import serve
from retro_speedlab_lakehouse.relay.producer import relay as run_relay
from retro_speedlab_lakehouse.relay.topics import create_topics
from retro_speedlab_lakehouse.settings import load_database_settings, load_settings
from retro_speedlab_lakehouse.spark.session import spark_session
from retro_speedlab_lakehouse.stream.job import start

app = typer.Typer(no_args_is_help=True)


def stopping(name: str) -> threading.Event:
    stopped = threading.Event()

    def stop(signum: int, frame: FrameType | None) -> None:
        logger.info(f"Signal {signum}: the {name} stops")
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    return stopped


@app.command()
def relay(config: Path) -> None:
    run_relay(load_settings(config), stopping("relay"))


@app.command()
def chat(config: Path) -> None:
    watch_chat(load_settings(config), stopping("chat"))


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
def thrift(config: Path) -> None:
    serve(load_database_settings(config))


@app.command()
def job(
    name: str,
    config: Path,
    dag: Annotated[str, typer.Option()],
    run_id: Annotated[str, typer.Option()],
) -> None:
    if outside_the_night(name, datetime.now(UTC)):
        logger.warning(f"{name} runs only between {NIGHT_START} and {NIGHT_END} UTC; it is skipped now")
        return
    settings = load_settings(config)
    spark = spark_session(f"lakehouse-{name}", settings.spark, settings.lake)
    try:
        run_job(spark, settings, name, dag, run_id)
    finally:
        spark.stop()
