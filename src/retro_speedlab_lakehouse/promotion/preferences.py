from datetime import datetime

from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.engagement.posterior import BETA, Posterior, Totals
from retro_speedlab_lakehouse.lake.catalog import EVENTS, PROMOTION_DECISIONS, PROMOTION_PREFERENCES
from retro_speedlab_lakehouse.lake.tables import exists, read
from retro_speedlab_lakehouse.lake.write import replace
from retro_speedlab_lakehouse.quality.gate import Gate
from retro_speedlab_lakehouse.settings import LakeSettings
from retro_speedlab_lakehouse.stream.rows import field, newest

DRAFTS = "promotion.drafts"
REJECTED = "rejected"
NUMBER = r"\d+(?:[.:,]\d+)*"
FIRST_SENTENCE = r"^.*?[.?!](?=\s|$)"
SHORT_POST = 120
LONG_POST = 200
MANY_NUMBERS = 3
FEATURES = ("kind", "variant", "opening", "tone", "length", "numbers")
ALL = "all"
PREFERENCES = StructType(
    [
        StructField("channel", StringType(), False),
        StructField("feature", StringType(), False),
        StructField("value", StringType(), False),
        StructField("decisions", LongType(), False),
        StructField("approved", LongType(), False),
        StructField("rejected", LongType(), False),
        StructField("approval_rate", DoubleType(), False),
        StructField("lower", DoubleType(), False),
        StructField("upper", DoubleType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


def opening(text: Column) -> Column:
    first = F.regexp_extract(text, FIRST_SENTENCE, 0)
    return (
        F.when(text.rlike(r"^\d"), F.lit("number"))
        .when(first.endswith("?"), F.lit("question"))
        .otherwise(F.lit("statement"))
    )


def length_band(length: Column) -> Column:
    return (
        F.when(length < SHORT_POST, F.lit(f"under {SHORT_POST}"))
        .when(length < LONG_POST, F.lit(f"{SHORT_POST}-{LONG_POST - 1}"))
        .otherwise(F.lit(f"{LONG_POST}+"))
    )


def numbers_band(numbers: Column) -> Column:
    return F.when(numbers >= MANY_NUMBERS, F.lit(f"{MANY_NUMBERS}+")).otherwise(numbers.cast("string"))


def text_features(decisions: DataFrame) -> DataFrame:
    text = F.col("text")
    return decisions.withColumns(
        {
            "length": F.length(text),
            "numbers": F.regexp_count(text, F.lit(NUMBER)).cast("int"),
            "opening": opening(text),
            "tone": F.when(text.contains("!"), F.lit("exclaims")).otherwise(F.lit("plain")),
        }
    )


def promotion_decisions(events: DataFrame) -> DataFrame:
    drafts = newest(events.where(F.col("topic") == DRAFTS), ["key"])
    decided = drafts.where(field("decided").isNotNull()).select(
        F.col("key").alias("draft"),
        field("event").alias("event"),
        field("kind").alias("kind"),
        field("game").alias("game"),
        field("channel").alias("channel"),
        field("variant").cast("int").alias("variant"),
        F.coalesce(field("copy.text"), field("copy.title")).alias("text"),
        (field("status") != REJECTED).alias("approved"),
        F.to_timestamp(field("decided")).alias("decided_at"),
    )
    return text_features(decided)


def pair(name: str) -> Column:
    return F.struct(F.lit(name).alias("feature"), F.col(name).cast("string").alias("value"))


def feature_values(decisions: DataFrame) -> DataFrame:
    bands = decisions.withColumns(
        {"length": length_band(F.col("length")), "numbers": numbers_band(F.col("numbers")), ALL: F.lit(ALL)}
    )
    pairs = F.array(*(pair(name) for name in (ALL, *FEATURES)))
    return bands.select("channel", "approved", F.explode(pairs).alias("pair")).select(
        "channel", "pair.feature", "pair.value", "approved"
    )


def preference_row(row: Row, now: datetime) -> tuple[object, ...]:
    approved, decisions = row["approved"], row["decisions"]
    rejected = decisions - approved
    posterior = Posterior(Totals(row["value"], decisions, approved), BETA, 1.0 + approved, 1.0 + rejected)
    lower, upper = posterior.interval()
    rate = approved / decisions
    return (row["channel"], row["feature"], row["value"], decisions, approved, rejected, rate, lower, upper, now)


def preference_rows(decisions: DataFrame, now: datetime) -> list[tuple[object, ...]]:
    counted = (
        feature_values(decisions)
        .groupBy("channel", "feature", "value")
        .agg(F.count("*").alias("decisions"), F.sum(F.col("approved").cast("long")).alias("approved"))
    )
    return [preference_row(row, now) for row in counted.collect()]


def build_promotion_preferences(spark: SparkSession, lake: LakeSettings, gate: Gate, now: datetime) -> int:
    if not exists(spark, EVENTS, lake):
        return 0
    replace(PROMOTION_DECISIONS, lake, promotion_decisions(read(spark, EVENTS, lake)), gate)
    rows = preference_rows(read(spark, PROMOTION_DECISIONS, lake), now)
    replace(PROMOTION_PREFERENCES, lake, spark.createDataFrame(rows, PREFERENCES), gate)
    return len(rows)
