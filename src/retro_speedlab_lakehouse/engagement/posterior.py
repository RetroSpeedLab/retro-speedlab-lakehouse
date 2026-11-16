from dataclasses import dataclass

import numpy as np
from scipy import stats

BETA = "beta"
GAMMA = "gamma"
INTERVAL = (0.05, 0.95)


@dataclass(frozen=True, slots=True)
class Totals:
    item: str
    exposures: float
    outcomes: float


@dataclass(frozen=True, slots=True)
class Posterior:
    totals: Totals
    family: str
    alpha: float
    beta: float

    def draws(self, size: int, rng: np.random.Generator) -> np.ndarray:
        if self.family == BETA:
            return rng.beta(self.alpha, self.beta, size)
        return rng.gamma(self.alpha, 1.0 / self.beta, size)

    def interval(self) -> tuple[float, float]:
        if self.family == BETA:
            lower, upper = stats.beta.ppf(INTERVAL, self.alpha, self.beta)
        else:
            lower, upper = stats.gamma.ppf(INTERVAL, self.alpha, scale=1.0 / self.beta)
        return float(lower), float(upper)

    @property
    def mean(self) -> float:
        return self.alpha / (self.alpha + self.beta) if self.family == BETA else self.alpha / self.beta


@dataclass(frozen=True, slots=True)
class Ranked:
    posterior: Posterior
    lead_probability: float
    share: float
    rank: int
    sampled_rank: int


def posteriors(totals: list[Totals], family: str, prior_weight: float) -> list[Posterior]:
    exposures = sum(total.exposures for total in totals)
    pooled = sum(total.outcomes for total in totals) / exposures if exposures > 0 else 0.0
    if family == BETA:
        rate = min(pooled, 1.0)
        return [
            Posterior(
                total,
                BETA,
                1.0 + prior_weight * rate + total.outcomes,
                1.0 + prior_weight * (1.0 - rate) + max(total.exposures - total.outcomes, 0.0),
            )
            for total in totals
        ]
    return [
        Posterior(total, GAMMA, 1.0 + prior_weight * pooled + total.outcomes, prior_weight + total.exposures)
        for total in totals
    ]


def lead_probabilities(found: list[Posterior], draws: int, rng: np.random.Generator) -> np.ndarray:
    sampled = np.vstack([posterior.draws(draws, rng) for posterior in found])
    return np.bincount(sampled.argmax(axis=0), minlength=len(found)) / draws


def sampled_order(found: list[Posterior], shares: np.ndarray, rng: np.random.Generator) -> list[int]:
    leader = int(rng.choice(len(found), p=shares / shares.sum()))
    thompson = [float(posterior.draws(1, rng)[0]) for posterior in found]
    others = sorted((index for index in range(len(found)) if index != leader), key=lambda index: -thompson[index])
    return [leader, *others]


def ranked(found: list[Posterior], min_visibility: float, draws: int, rng: np.random.Generator) -> list[Ranked]:
    if not found:
        return []
    lead = lead_probabilities(found, draws, rng)
    shares = (1.0 - min_visibility) * lead + min_visibility / len(found)
    by_mean = sorted(range(len(found)), key=lambda index: -found[index].mean)
    rank = {index: position + 1 for position, index in enumerate(by_mean)}
    sampled = {index: position + 1 for position, index in enumerate(sampled_order(found, shares, rng))}
    return [
        Ranked(posterior, float(lead[index]), float(shares[index]), rank[index], sampled[index])
        for index, posterior in enumerate(found)
    ]
