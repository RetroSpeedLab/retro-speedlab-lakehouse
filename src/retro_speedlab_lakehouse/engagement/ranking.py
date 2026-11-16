from datetime import UTC, datetime, timedelta

import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from retro_speedlab_lakehouse.engagement.posterior import Ranked, Totals, posteriors, ranked
from retro_speedlab_lakehouse.settings import EngagementSettings

HOUR = timedelta(hours=1)
RANKING = StructType(
    [
        StructField("surface", StringType(), False),
        StructField("item", StringType(), False),
        StructField("family", StringType(), False),
        StructField("exposures", DoubleType(), False),
        StructField("outcomes", DoubleType(), False),
        StructField("alpha", DoubleType(), False),
        StructField("beta", DoubleType(), False),
        StructField("mean", DoubleType(), False),
        StructField("lower", DoubleType(), False),
        StructField("upper", DoubleType(), False),
        StructField("lead_probability", DoubleType(), False),
        StructField("share", DoubleType(), False),
        StructField("rank", LongType(), False),
        StructField("sampled_rank", LongType(), False),
        StructField("period", TimestampType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


def next_period(now: datetime) -> datetime:
    return now.astimezone(UTC).replace(minute=0, second=0, microsecond=0) + HOUR


def surface_totals(events: DataFrame, catalog: list[str], settings: EngagementSettings) -> dict[str, list[Totals]]:
    summed = events.groupBy("surface", "item").agg(
        F.sum("exposures").alias("exposures"), F.sum("outcomes").alias("outcomes")
    )
    found: dict[str, dict[str, Totals]] = {surface: {} for surface in settings.surfaces}
    for row in summed.collect():
        if row["surface"] in found:
            found[row["surface"]][row["item"]] = Totals(row["item"], row["exposures"], row["outcomes"])
    for surface, items in found.items():
        if settings.surfaces[surface].catalog:
            items.update({game: Totals(game, 0.0, 0.0) for game in catalog if game not in items})
    return {surface: sorted(items.values(), key=lambda total: total.item) for surface, items in found.items()}


def ranking_row(surface: str, item: Ranked, period: datetime, now: datetime) -> tuple[object, ...]:
    posterior = item.posterior
    lower, upper = posterior.interval()
    totals = posterior.totals
    return (
        surface,
        totals.item,
        posterior.family,
        totals.exposures,
        totals.outcomes,
        posterior.alpha,
        posterior.beta,
        posterior.mean,
        lower,
        upper,
        item.lead_probability,
        item.share,
        item.rank,
        item.sampled_rank,
        period,
        now,
    )


def ranking_rows(
    totals: dict[str, list[Totals]], settings: EngagementSettings, now: datetime
) -> list[tuple[object, ...]]:
    period = next_period(now)
    rng = np.random.default_rng()
    rows: list[tuple[object, ...]] = []
    for surface, items in totals.items():
        found = posteriors(items, settings.surfaces[surface].family, settings.prior_weight)
        rows.extend(
            ranking_row(surface, item, period, now)
            for item in ranked(found, settings.min_visibility, settings.draws, rng)
        )
    return rows
