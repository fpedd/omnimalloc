#
# SPDX-License-Identifier: Apache-2.0
#

import math
import random
from enum import Enum
from typing import Final


class SizeDistribution(str, Enum):
    """Size distribution families, ordered from flattest to most skewed.

    Real workloads sit at the skewed end: a compiler's scope is one dominant
    buffer plus hundreds of small ones, the shape that breaks greedy-by-size.
    """

    UNIFORM = "uniform"
    LOG_UNIFORM = "log_uniform"
    ZIPF = "zipf"
    BIMODAL = "bimodal"
    DOMINANT = "dominant"

    __str__ = str.__str__


SIZE_DISTRIBUTIONS: Final[tuple[SizeDistribution, ...]] = tuple(SizeDistribution)

# `bimodal` splits this fraction of the allocations into the small mode
_SMALL_FRACTION: Final[float] = 0.9

# `dominant`'s single large buffer, as a fraction of the range
_DOMINANT_FRACTION: Final[float] = 0.9


def sample_sizes(
    rng: random.Random,
    count: int,
    distribution: SizeDistribution | str,
    size_min: int,
    size_max: int,
) -> list[int]:
    """Draw `count` sizes in [size_min, size_max] from the named distribution.

    `uniform` is flat, `log_uniform` flat in the exponent, `zipf` heavy-tailed,
    `bimodal` the 90/10 accelerator mix, `dominant` one buffer at 90%.
    """
    distribution = SizeDistribution(distribution)
    if size_min <= 0:
        raise ValueError("size_min must be positive")
    if size_max < size_min:
        raise ValueError("size_max must be >= size_min")
    if count <= 0:
        return []

    if distribution == SizeDistribution.UNIFORM:
        return [rng.randint(size_min, size_max) for _ in range(count)]
    if distribution == SizeDistribution.LOG_UNIFORM:
        return [_log_uniform(rng, size_min, size_max) for _ in range(count)]
    if distribution == SizeDistribution.ZIPF:
        return [_zipf(rng, size_min, size_max) for _ in range(count)]
    if distribution == SizeDistribution.BIMODAL:
        return _bimodal(rng, count, size_min, size_max)
    return _dominant(rng, count, size_min, size_max)


def _clamp(value: int, size_min: int, size_max: int) -> int:
    return max(size_min, min(size_max, value))


def _log_uniform(rng: random.Random, size_min: int, size_max: int) -> int:
    exponent = rng.uniform(math.log2(size_min), math.log2(size_max))
    return _clamp(int(2.0**exponent), size_min, size_max)


def _zipf(rng: random.Random, size_min: int, size_max: int) -> int:
    # Inverse-transform sample of a Pareto tail anchored at size_min; the
    # exponent is fixed so one knob (the range) controls the spread.
    tail = rng.random() or 1e-12
    return _clamp(int(size_min / tail**0.8), size_min, size_max)


def _bimodal(rng: random.Random, count: int, size_min: int, size_max: int) -> list[int]:
    boundary = max(size_min, math.isqrt(size_min * size_max))
    sizes = []
    for _ in range(count):
        if rng.random() < _SMALL_FRACTION:
            sizes.append(rng.randint(size_min, boundary))
        else:
            sizes.append(rng.randint(boundary, size_max))
    return sizes


def _dominant(
    rng: random.Random, count: int, size_min: int, size_max: int
) -> list[int]:
    large = _clamp(int(size_max * _DOMINANT_FRACTION), size_min, size_max)
    small_max = max(size_min, large // count)
    sizes = [rng.randint(size_min, small_max) for _ in range(count)]
    # The dominant buffer lands at a drawn position, not always first, so
    # input order alone never tells an allocator where it is
    sizes[rng.randrange(count)] = large
    return sizes
