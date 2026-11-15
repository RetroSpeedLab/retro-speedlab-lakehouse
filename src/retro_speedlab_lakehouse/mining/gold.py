from pyspark.sql import Column, DataFrame, Window, WindowSpec
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from retro_speedlab_lakehouse.stream.rows import RUN_TOPICS, field

CELL_PIXELS = 16
RAM_BINS = 32
SPLIT = StructType(
    [
        StructField("level", StringType(), False),
        StructField("level_seconds", DoubleType(), False),
        StructField("first_seconds", DoubleType(), False),
    ]
)
LEVEL = StructType([StructField("name", StringType(), False)])


def first_by(keys: list[str], *order: Column | str) -> WindowSpec:
    return Window.partitionBy(*keys).orderBy(*order)


def latest_pieces(pieces: DataFrame) -> DataFrame:
    newest = F.dense_rank().over(first_by(["game"], F.col("mined_at").desc()))
    return pieces.withColumn("newest", newest).where("newest = 1").drop("newest")


def level_segments(latest: DataFrame) -> DataFrame:
    return latest.select(
        "game",
        "record",
        "segment",
        F.coalesce("level", "entered").alias("level"),
        "entered",
        "reached",
        "won",
        "first_frame",
        "frames",
        "identical",
        (F.col("frames") / F.sum("frames").over(Window.partitionBy("game"))).alias("share"),
    )


def costliest(rows: DataFrame, keys: list[str], prefix: str) -> DataFrame:
    ranked = rows.withColumn("rank", F.row_number().over(first_by(["game"], F.col("frames").desc(), *keys)))
    return ranked.where("rank = 1").select(
        "game", *[F.col(key).alias(f"{prefix}_{key}") for key in keys], F.col("frames").alias(f"{prefix}_frames")
    )


def mining_summary(latest: DataFrame, segments: DataFrame) -> DataFrame:
    totals = latest.groupBy("game", "record", "version", "cartridge_version").agg(
        F.first("labels").alias("labels"),
        F.sum("frames").alias("frames"),
        F.count("*").alias("pieces"),
        F.sum(F.col("identical").cast("int")).alias("identical_pieces"),
        F.max("mined_at").alias("mined_at"),
    )
    levels = segments.groupBy("game", "level").agg(F.sum("frames").alias("frames"))
    return (
        totals.withColumn("replays_identically", F.col("identical_pieces") == F.col("pieces"))
        .join(costliest(levels, ["level"], "costliest"), "game")
        .join(costliest(segments, ["segment", "level"], "costliest_piece"), "game")
        .withColumn("costliest_share", F.col("costliest_frames") / F.col("frames"))
    )


def kept_frames(frames: DataFrame) -> DataFrame:
    return frames.where(F.col("identical") & F.col("level").isNotNull())


def position_heatmap(frames: DataFrame) -> DataFrame:
    placed = kept_frames(frames).where(F.col("x").isNotNull() & F.col("y").isNotNull())
    cells = placed.select(
        "game",
        "level",
        F.floor(F.col("x") / CELL_PIXELS).cast("int").alias("cell_x"),
        F.floor(F.col("y") / CELL_PIXELS).cast("int").alias("cell_y"),
    )
    counted = cells.groupBy("game", "level", "cell_x", "cell_y").agg(F.count("*").alias("frames"))
    return counted.withColumn("cell_pixels", F.lit(CELL_PIXELS))


def ram_heatmap(frames: DataFrame) -> DataFrame:
    values = kept_frames(frames).select("game", "level", F.explode("labels").alias("label", "value"))
    shown = values.where(F.col("value").isNotNull())
    spans = shown.groupBy("game", "label").agg(F.min("value").alias("lowest"), F.max("value").alias("highest"))
    width = F.greatest(F.lit(1), F.ceil((F.col("highest") - F.col("lowest") + 1) / RAM_BINS)).cast("int")
    binned = shown.join(spans.withColumn("width", width), ["game", "label"]).withColumn(
        "low", F.col("lowest") + F.floor((F.col("value") - F.col("lowest")) / F.col("width")) * F.col("width")
    )
    counted = binned.groupBy("game", "level", "label", "low", "width").agg(F.count("*").alias("frames"))
    return counted.select("game", "level", "label", F.col("low").cast("int").alias("value"), "width", "frames")


def won_runs(events: DataFrame) -> DataFrame:
    runs = events.where(F.col("topic").isin(*RUN_TOPICS) & (field("won") == "true"))
    return runs.select(
        F.col("key").alias("game"),
        F.col("event_at").alias("at"),
        field("frames").cast("long").alias("frames"),
        field("frame_rate").cast("double").alias("frame_rate"),
        F.from_json(field("splits"), ArrayType(SPLIT)).alias("splits"),
        F.from_json(field("levels"), ArrayType(LEVEL)).alias("levels"),
    )


def changed(rows: DataFrame, keys: list[str], value: str) -> DataFrame:
    marked = rows.withColumn("before", F.lag(value).over(first_by(keys, "at")))
    return marked.where(F.col("before").isNull() | (F.col("before") != F.col(value))).drop("before")


def level_history(events: DataFrame) -> DataFrame:
    runs = won_runs(events)
    splits = runs.select("game", "at", "frame_rate", "levels", F.explode("splits").alias("split"))
    levels = splits.where(F.exists("levels", lambda level: level["name"] == F.col("split.level")))
    rows = levels.select(
        "game",
        F.col("split.level").alias("level"),
        F.array_position(F.transform("levels", lambda level: level["name"]), F.col("split.level")).alias("position"),
        "at",
        F.round(F.col("split.level_seconds") * F.col("frame_rate")).cast("long").alias("frames"),
        F.round(F.col("split.first_seconds") * F.col("frame_rate")).cast("long").alias("first_frames"),
    )
    return changed(rows.dropDuplicates(["game", "level", "at"]), ["game", "level"], "frames")
