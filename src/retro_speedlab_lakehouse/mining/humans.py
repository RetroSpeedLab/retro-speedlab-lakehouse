from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.batch.major import board_line, current_line
from retro_speedlab_lakehouse.mining.verify import AI, AI_NAME, HUMAN, NO_INPUTS, REJECTED, UNSERVED, VERIFIED
from retro_speedlab_lakehouse.stream.rows import HIGHSCORES_TOPIC, field

FRAME_RATE = 60.0
PENDING = "pending"
MEDIAN = 0.5


def improvements(rows: DataFrame, keys: list[Column]) -> DataFrame:
    earlier = Window.partitionBy(*keys).orderBy("at").rowsBetween(Window.unboundedPreceding, -1)
    marked = rows.withColumn("best_before", F.min("frames").over(earlier))
    return marked.where(F.col("best_before").isNull() | (F.col("frames") < F.col("best_before"))).drop("best_before")


def board_history(events: DataFrame, won: DataFrame) -> DataFrame:
    humans = events.where(F.col("topic") == HIGHSCORES_TOPIC).select(
        field("game").alias("game"),
        field("version").alias("version"),
        F.lit(HUMAN).alias("side"),
        field("name").alias("holder"),
        F.to_timestamp(field("set_at")).alias("at"),
        field("frames").cast("long").alias("frames"),
    )
    ai = won.select(
        "game",
        "version",
        F.lit(AI).alias("side"),
        F.lit(AI_NAME).alias("holder"),
        "at",
        "frames",
    )
    board = [F.col("game"), board_line(F.col("version"))]
    return improvements(humans.dropDuplicates(["game", "version", "at"]), board).unionByName(
        improvements(ai.dropDuplicates(["game", "at"]), [F.col("game")])
    )


def entry_of(scores: DataFrame) -> DataFrame:
    return scores.withColumn("entry", F.concat_ws(":", F.col("id").cast("string"), F.col("frames").cast("string")))


def humans_vs_ai(highscores: DataFrame, games: DataFrame, runs: DataFrame, replays: DataFrame) -> DataFrame:
    current = current_line(highscores, "version", games)
    statuses = replays.where(F.col("side") == HUMAN).select("entry", "status")
    ai = runs.where("won").select("game", F.col("frames").alias("ai_frames"))
    place = F.rank().over(Window.partitionBy("game").orderBy("frames", "set_at"))
    return (
        entry_of(current)
        .join(statuses, "entry", "left")
        .join(ai, "game", "left")
        .select(
            "game",
            "version",
            "id",
            "entry",
            "name",
            "frames",
            (F.col("frames") / FRAME_RATE).alias("seconds"),
            "set_at",
            place.alias("place"),
            "ai_frames",
            (F.col("frames") - F.col("ai_frames")).alias("gap_frames"),
            F.coalesce("status", F.lit(PENDING)).alias("status"),
        )
    )


def verified_splits(splits: DataFrame, replays: DataFrame) -> DataFrame:
    verified = replays.where(F.col("status") == VERIFIED).select("side", "entry")
    return splits.join(verified, ["side", "entry"])


def level_names(ai: DataFrame, segments: DataFrame) -> DataFrame:
    ends = segments.select(
        "game", F.col("record").alias("entry"), "level", (F.col("first_frame") + F.col("frames")).alias("ends_at")
    )
    splits = ai.select("game", "entry", "level_index", (F.col("entered_at") + F.col("frames")).alias("ends_at"))
    return splits.join(ends, ["game", "entry", "ends_at"]).select("game", "level_index", "level").distinct()


def split_comparison(kept: DataFrame, segments: DataFrame, humans: DataFrame) -> DataFrame:
    ai_runs = segments.select("game", F.col("record").alias("entry")).distinct()
    ai = kept.where(F.col("side") == AI).join(ai_runs, ["game", "entry"])
    ai_levels = ai.groupBy("game", "level_index").agg(F.sum("frames").alias("ai_frames"))
    human = kept.where(F.col("side") == HUMAN).join(humans.select("game", "entry"), ["game", "entry"])
    per_run = human.groupBy("game", "entry", "level_index").agg(F.sum("frames").alias("frames"))
    human_levels = per_run.groupBy("game", "level_index").agg(
        F.min("frames").alias("human_best_frames"),
        F.percentile_approx("frames", MEDIAN).alias("human_median_frames"),
        F.count("*").alias("humans"),
    )
    compared = ai_levels.join(human_levels, ["game", "level_index"], "full")
    return compared.join(level_names(ai, segments), ["game", "level_index"], "left").fillna(0, ["humans"])


def counted(status: str) -> Column:
    return F.sum((F.col("status") == status).cast("int")).alias(status)


def verification_stats(replays: DataFrame) -> DataFrame:
    return replays.groupBy("game", "side").agg(
        F.count("*").alias("replays"),
        *[counted(status) for status in (VERIFIED, REJECTED, UNSERVED, NO_INPUTS)],
        F.max("replayed_at").alias("last_replayed_at"),
    )
