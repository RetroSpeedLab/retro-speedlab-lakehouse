from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retro_speedlab_lakehouse.bandit.comparison import BANDIT, HOLDOUT, METRIC, SECONDS_PER_HOUR
from retro_speedlab_lakehouse.bandit.holdout import finished_turns, turn_savings
from retro_speedlab_lakehouse.growthbook.units import Experiment, Family, measured

SEARCH_BANDIT = "search_bandit"
HOLDOUT_SHARE = 0.2

SEARCH = Experiment(
    SEARCH_BANDIT,
    "Search: bandit worker split",
    "The bandit's split of workers saves more frames per hour than the static split",
    (HOLDOUT, BANDIT),
    (HOLDOUT_SHARE, 1 - HOLDOUT_SHARE),
    (METRIC,),
)


def search_bandit(events: DataFrame, now: datetime) -> Family:
    turns = turn_savings(events, finished_turns(events, now)).select(
        F.lit(SEARCH_BANDIT).alias("experiment"),
        F.col("arm").alias("variation"),
        F.concat_ws(":", F.lit(SEARCH_BANDIT), "game", "turn").alias("unit"),
        F.col("started_at").alias("assigned_at"),
        (F.col("saved") / (F.col("seconds") / SECONDS_PER_HOUR)).alias(METRIC),
    )
    return Family([SEARCH], measured(turns, (METRIC,)))
