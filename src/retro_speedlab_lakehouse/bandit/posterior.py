from dataclasses import dataclass

import numpy as np

PRIOR_WORKER_SECONDS = 3600.0
PRIOR_SHAPE = 1.0
INTERVAL = (2.5, 97.5)


@dataclass(frozen=True, slots=True)
class Effort:
    role: str
    saved: int
    improvements: int
    worker_seconds: float


@dataclass(frozen=True, slots=True)
class Allocation:
    effort: Effort
    share: float
    best: float
    rate_mean: float
    rate_lower: float
    rate_upper: float


def dispersion(efforts: list[Effort]) -> float:
    improvements = sum(effort.improvements for effort in efforts)
    return max(1.0, sum(effort.saved for effort in efforts) / improvements) if improvements else 1.0


def pooled_rate(efforts: list[Effort]) -> float:
    return sum(effort.saved for effort in efforts) / sum(effort.worker_seconds for effort in efforts)


def rate_draws(efforts: list[Effort], draws: int, rng: np.random.Generator) -> np.ndarray:
    clump, pooled = dispersion(efforts), pooled_rate(efforts)
    shape = np.array([PRIOR_SHAPE + (pooled * PRIOR_WORKER_SECONDS + effort.saved) / clump for effort in efforts])
    rate = np.array([(PRIOR_WORKER_SECONDS + effort.worker_seconds) / clump for effort in efforts])
    return rng.gamma(shape[:, None], 1.0 / rate[:, None], size=(len(efforts), draws))


def allocations(efforts: list[Effort], min_share: float, draws: int, rng: np.random.Generator) -> list[Allocation]:
    if min_share * len(efforts) > 1:
        raise ValueError(f"A floor of {min_share} for each of {len(efforts)} worker kinds exceeds all workers")
    sampled = rate_draws(efforts, draws, rng)
    best = np.bincount(sampled.argmax(axis=0), minlength=len(efforts)) / draws
    lower, upper = np.percentile(sampled, INTERVAL, axis=1)
    spread = 1.0 - min_share * len(efforts)
    return [
        Allocation(
            effort,
            min_share + spread * float(best[index]),
            float(best[index]),
            float(sampled[index].mean()),
            float(lower[index]),
            float(upper[index]),
        )
        for index, effort in enumerate(efforts)
    ]
