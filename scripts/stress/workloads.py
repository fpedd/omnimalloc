#
# SPDX-License-Identifier: Apache-2.0
#
"""Catalog of diverse workloads for the stress harness.

Wraps the benchmark sources plus shapes no generator produces, and records a
``known_optimum`` only where the instance is reverse-constructed from one.
"""

from collections.abc import Callable
from dataclasses import dataclass, field

from omnimalloc.benchmark.sources import (
    ConcurrentTilingSource,
    HighContentionSource,
    MinimallocSource,
    PinwheelSource,
    PowerOf2Source,
    RandomSource,
    SequentialSource,
    SkewedSource,
    SyncPatternSource,
    TilingSource,
    TwoPlusTwoSource,
    UniformSource,
)
from omnimalloc.benchmark.sources.sizes import SIZE_DISTRIBUTIONS
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS
from omnimalloc.common.constants import KB, MB
from omnimalloc.primitives import Allocation

# The tiling sources cut a `capacity x makespan` rectangle into leaves, so the
# capacity is exactly the optimal peak.
TILING_CAPACITY = MB


@dataclass(frozen=True)
class Workload:
    """One named, seedable instance generator plus what is known about it."""

    name: str
    family: str
    dim: int
    build: Callable[[int, int], tuple[Allocation, ...]]
    known_optimum: int | None = None
    tags: frozenset[str] = field(default_factory=frozenset)

    def allocations(self, size: int, seed: int) -> tuple[Allocation, ...]:
        return self.build(size, seed)


def _generators() -> list[Workload]:
    return [
        Workload(
            "random",
            "generator",
            1,
            lambda n, s: RandomSource(num_allocations=n, seed=s).get_allocations(),
        ),
        Workload(
            "random_wide_sizes",
            "generator",
            1,
            lambda n, s: RandomSource(
                num_allocations=n, size_min=1, size_max=64 * MB, seed=s
            ).get_allocations(),
        ),
        Workload(
            "random_long_lived",
            "generator",
            1,
            lambda n, s: RandomSource(
                num_allocations=n,
                time_max=1000,
                duration_min=400,
                duration_max=1000,
                seed=s,
            ).get_allocations(),
        ),
        Workload(
            "random_short_lived",
            "generator",
            1,
            lambda n, s: RandomSource(
                num_allocations=n,
                time_max=100_000,
                duration_min=1,
                duration_max=2,
                seed=s,
            ).get_allocations(),
            tags=frozenset({"sparse"}),
        ),
        Workload(
            "uniform",
            "generator",
            1,
            lambda n, s: UniformSource(num_allocations=n, seed=s).get_allocations(),
        ),
        Workload(
            "power_of2",
            "generator",
            1,
            lambda n, s: PowerOf2Source(num_allocations=n, seed=s).get_allocations(),
        ),
        Workload(
            "high_contention",
            "generator",
            1,
            lambda n, s: HighContentionSource(
                num_allocations=n, seed=s
            ).get_allocations(),
            tags=frozenset({"dense"}),
        ),
        Workload(
            "sequential",
            "generator",
            1,
            lambda n, s: SequentialSource(num_allocations=n, seed=s).get_allocations(),
        ),
    ]


def _skewed() -> list[Workload]:
    return [
        Workload(
            f"skewed[{distribution}]",
            "adversarial",
            1,
            lambda n, s, d=distribution: SkewedSource(
                num_allocations=n, distribution=d, seed=s
            ).get_allocations(),
            tags=frozenset({"adversarial"}),
        )
        for distribution in SIZE_DISTRIBUTIONS
    ]


def _tilings() -> list[Workload]:
    entries = [
        Workload(
            "tiling",
            "tiling",
            1,
            lambda n, s: TilingSource(num_allocations=n, seed=s).get_allocations(),
            known_optimum=TILING_CAPACITY,
            tags=frozenset({"known_optimum"}),
        ),
        Workload(
            "tiling_mem_biased",
            "tiling",
            1,
            lambda n, s: TilingSource(
                num_allocations=n, mem_cut_prob=0.9, seed=s
            ).get_allocations(),
            known_optimum=TILING_CAPACITY,
            tags=frozenset({"known_optimum"}),
        ),
        Workload(
            "pinwheel",
            "tiling",
            1,
            # Pinwheel splits five-way, so only counts of 1 mod 4 are reachable.
            lambda n, s: PinwheelSource(
                num_allocations=n + (1 - n) % 4, seed=s
            ).get_allocations(),
            known_optimum=TILING_CAPACITY,
            tags=frozenset({"known_optimum", "adversarial"}),
        ),
    ]
    entries += [
        Workload(
            f"concurrent_tiling[t={threads}]",
            "tiling",
            threads,
            lambda n, s, t=threads: ConcurrentTilingSource(
                num_allocations=max(n, t), num_threads=t, seed=s
            ).get_allocations(),
            known_optimum=TILING_CAPACITY,
            tags=frozenset({"known_optimum", "vector"}),
        )
        for threads in (2, 4, 8)
    ]
    return entries


def _sync_patterns() -> list[Workload]:
    entries = [
        Workload(
            f"sync[{pattern},t=4]",
            "sync",
            4,
            lambda n, s, p=pattern: SyncPatternSource(
                num_allocations=n, num_threads=4, pattern=p, seed=s
            ).get_allocations(),
            tags=frozenset({"vector"}),
        )
        for pattern in SYNC_PATTERNS
    ]
    entries += [
        Workload(
            f"sync[{pattern},t={threads}]",
            "sync",
            threads,
            lambda n, s, t=threads, p=pattern: SyncPatternSource(
                num_allocations=n, num_threads=t, pattern=p, seed=s
            ).get_allocations(),
            tags=frozenset({"vector"}),
        )
        for pattern, threads in (
            ("dense", 2),
            ("dense", 8),
            ("dense", 16),
            ("dense", 32),
            ("dense", 64),
            ("sparse", 8),
            ("sparse", 32),
            ("sparse", 64),
        )
    ]
    entries.append(
        Workload(
            "sync[independent,t=16,skew]",
            "sync",
            16,
            lambda n, s: SyncPatternSource(
                num_allocations=n,
                num_threads=16,
                pattern="independent",
                speed_skew=7,
                size_distribution="dominant",
                seed=s,
            ).get_allocations(),
            tags=frozenset({"vector", "adversarial"}),
        )
    )
    return entries


def _non_interval() -> list[Workload]:
    return [
        Workload(
            f"two_plus_two[noise={noise}]",
            "adversarial",
            2,
            lambda n, s, x=noise: TwoPlusTwoSource(
                num_allocations=max(n, 4), noise=x, seed=s
            ).get_allocations(),
            tags=frozenset({"vector", "adversarial", "non_interval"}),
        )
        for noise in (0.0, 0.5)
    ]


def _identical(n: int, _seed: int) -> tuple[Allocation, ...]:
    return tuple(Allocation(id=i, size=KB, start=0, end=1) for i in range(n))


def _disjoint(n: int, _seed: int) -> tuple[Allocation, ...]:
    return tuple(Allocation(id=i, size=KB, start=i, end=i + 1) for i in range(n))


def _nested(n: int, _seed: int) -> tuple[Allocation, ...]:
    return tuple(Allocation(id=i, size=KB, start=i, end=2 * n - i) for i in range(n))


def _staircase(n: int, _seed: int) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(id=i, size=(i + 1) * KB, start=i, end=i + 2) for i in range(n)
    )


def _one_giant(n: int, _seed: int) -> tuple[Allocation, ...]:
    giant = Allocation(id=0, size=n * MB, start=0, end=2 * n)
    rest = (Allocation(id=i, size=KB, start=i, end=i + 2) for i in range(1, n))
    return (giant, *rest)


def _huge_sizes(n: int, _seed: int) -> tuple[Allocation, ...]:
    """Sizes near 2**40 exercise the 64-bit accumulators in the C++ sweeps."""
    return tuple(
        Allocation(id=i, size=(1 << 40) + i, start=i % 7, end=i % 7 + 3)
        for i in range(n)
    )


def _huge_times(n: int, _seed: int) -> tuple[Allocation, ...]:
    base = 1 << 50
    return tuple(
        Allocation(id=i, size=KB, start=base + i, end=base + i + 3) for i in range(n)
    )


def _string_ids(n: int, _seed: int) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(id=f"buf/{i:06d}", size=(i % 17 + 1) * KB, start=i, end=i + 5)
        for i in range(n)
    )


def _vector_identical(n: int, _seed: int) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(id=i, size=KB, start=(0, 0, 0, 0), end=(1, 1, 1, 1))
        for i in range(n)
    )


def _vector_chain(n: int, _seed: int) -> tuple[Allocation, ...]:
    """A totally ordered chain, so no two allocations conflict at all."""
    return tuple(
        Allocation(id=i, size=KB, start=(i, i), end=(i + 1, i + 1)) for i in range(n)
    )


def _degenerate() -> list[Workload]:
    return [
        Workload("degenerate_identical", "degenerate", 1, _identical),
        Workload("degenerate_disjoint", "degenerate", 1, _disjoint),
        Workload("degenerate_nested", "degenerate", 1, _nested),
        Workload("degenerate_staircase", "degenerate", 1, _staircase),
        Workload("degenerate_one_giant", "degenerate", 1, _one_giant),
        Workload(
            "degenerate_huge_sizes",
            "degenerate",
            1,
            _huge_sizes,
            tags=frozenset({"overflow"}),
        ),
        Workload(
            "degenerate_huge_times",
            "degenerate",
            1,
            _huge_times,
            tags=frozenset({"overflow"}),
        ),
        Workload("degenerate_string_ids", "degenerate", 1, _string_ids),
        Workload(
            "degenerate_vector_identical",
            "degenerate",
            4,
            _vector_identical,
            tags=frozenset({"vector"}),
        ),
        Workload(
            "degenerate_vector_chain",
            "degenerate",
            2,
            _vector_chain,
            tags=frozenset({"vector"}),
        ),
    ]


def _real() -> list[Workload]:
    """The bundled Minimalloc CSVs, absent from wheel installs."""
    entries = []
    for subset in ("examples", "small", "challenging"):
        source = MinimallocSource(subset=subset)
        if not source.get_available_variants():
            continue
        entries.append(
            Workload(
                f"minimalloc[{subset}]",
                "real",
                1,
                lambda n, _s, src=source: src.get_allocations(num_allocations=n),
                tags=frozenset({"real", "fixed"}),
            )
        )
    return entries


def catalog() -> list[Workload]:
    """Every workload the harness sweeps, in a stable order."""
    return (
        _generators()
        + _skewed()
        + _tilings()
        + _sync_patterns()
        + _non_interval()
        + _degenerate()
        + _real()
    )


def by_name() -> dict[str, Workload]:
    return {workload.name: workload for workload in catalog()}
