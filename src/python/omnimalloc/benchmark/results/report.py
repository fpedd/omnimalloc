#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass
from statistics import mean, median, stdev

from omnimalloc.allocators import BaseAllocator
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.primitives import IdType
from omnimalloc.primitives.utils import ensure_unique_ids

from .result import BenchmarkResult
from .utils import source_label


@dataclass(frozen=True)
class BenchmarkReport:
    """Aggregates results (iterations) of one allocator/source/variant combination.

    The iterations re-run one fixed problem instance, so their dispersion
    measures timing jitter, not instance-to-instance variance.
    """

    id: IdType
    results: tuple[BenchmarkResult, ...]
    allocator: BaseAllocator | str | None = None
    source: BaseSource | str | None = None
    variant_id: IdType | None = None
    known_optimum: int | None = None

    def __post_init__(self) -> None:
        if not self.results:
            raise ValueError("BenchmarkReport must contain at least one result")

        ensure_unique_ids(self.results, "result")

        num_allocs = {r.num_allocations for r in self.results}
        if len(num_allocs) > 1:
            raise ValueError("results in report must have same number of allocations")

        if self.allocator is not None:
            alloc_names = {r.allocator_name for r in self.results}
            if len(alloc_names) > 1 or self.allocator_name not in alloc_names:
                raise ValueError("Allocator mismatch between report and results")

        if self.source is not None:
            source_names = {r.source_name for r in self.results}
            if len(source_names) > 1 or self.source_name not in source_names:
                raise ValueError("Source mismatch between report and results")

    @property
    def allocator_name(self) -> str:
        if self.allocator is None:
            return self.results[0].allocator_name
        return str(self.allocator)

    @property
    def source_name(self) -> str:
        if self.source is None:
            return self.results[0].source_name
        return source_label(self.source)

    @property
    def variant_label(self) -> str:
        """Human-readable label for this variant."""
        if self.variant_id is None:
            return f"{self.num_allocations}"
        if isinstance(self.variant_id, str):
            return self.variant_id
        return f"{self.variant_id}"

    @property
    def is_categorical(self) -> bool:
        """Whether the variant_id is categorical (str) or numerical (int)."""
        return isinstance(self.variant_id, str)

    @property
    def num_allocations(self) -> int:
        return self.results[0].num_allocations

    @property
    def total_num_allocations(self) -> int:
        return sum(r.num_allocations for r in self.results)

    @property
    def num_results(self) -> int:
        return len(self.results)

    @property
    def mean_seconds(self) -> float:
        return mean(r.duration for r in self.results)

    @property
    def median_seconds(self) -> float:
        return median(r.duration for r in self.results)

    @property
    def stdev_seconds(self) -> float | None:
        """Timing jitter across iterations; None below two iterations."""
        if len(self.results) < 2:
            return None
        return stdev(r.duration for r in self.results)

    @property
    def min_seconds(self) -> float:
        return min(r.duration for r in self.results)

    @property
    def max_seconds(self) -> float:
        return max(r.duration for r in self.results)

    @property
    def mean_allocation_efficiency(self) -> float | None:
        """Mean over the iterations whose pressure could be measured."""
        measured = [
            efficiency
            for efficiency in (r.allocation_efficiency for r in self.results)
            if efficiency is not None
        ]
        return mean(measured) if measured else None

    @property
    def mean_peak_size(self) -> float:
        return mean(r.peak_size for r in self.results)

    @property
    def lower_bound(self) -> int | None:
        """Peak pressure of the instance, identical across iterations."""
        return self.results[0].lower_bound

    @property
    def optimum_ratio(self) -> float | None:
        """Achieved peak over the source's known optimum; 1.0 is optimal."""
        if not self.known_optimum:
            return None
        return self.mean_peak_size / self.known_optimum

    def with_results(self, results: tuple[BenchmarkResult, ...]) -> "BenchmarkReport":
        return BenchmarkReport(
            id=self.id,
            allocator=self.allocator,
            source=self.source,
            variant_id=self.variant_id,
            known_optimum=self.known_optimum,
            results=self.results + results,
        )
