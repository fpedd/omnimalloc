#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass

import numpy as np

from omnimalloc.allocators import BaseAllocator
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.primitives import IdType

from .result import BenchmarkResult


@dataclass(frozen=True)
class BenchmarkReport:
    """A collection of benchmark results with aggregate statistics.

    A report aggregates multiple results (iterations) for the same
    allocator/source/variant combination.
    """

    id: IdType
    results: tuple[BenchmarkResult, ...]
    allocator: BaseAllocator | type[BaseAllocator] | str | None = None
    source: BaseSource | type[BaseSource] | str | None = None
    variant_id: IdType | None = None

    def __post_init__(self) -> None:
        if not self.results:
            raise ValueError("BenchmarkReport must contain at least one result")

        if len({r.id for r in self.results}) != len(self.results):
            raise ValueError("result ids must be unique")

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
        return str(self.source)

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
        num_allocs = {r.num_allocations for r in self.results}
        if len(num_allocs) != 1:
            raise ValueError("results in report have different number of allocations")
        return num_allocs.pop()

    @property
    def total_num_allocations(self) -> int:
        return sum(r.num_allocations for r in self.results)

    @property
    def num_results(self) -> int:
        return len(self.results)

    @property
    def mean_seconds(self) -> float:
        return float(np.mean([r.duration for r in self.results]))

    @property
    def average_seconds(self) -> float:
        return self.mean_seconds

    @property
    def median_seconds(self) -> float:
        return float(np.median([r.duration for r in self.results]))

    @property
    def mean_allocation_efficiency(self) -> float:
        return float(np.mean([r.allocation_efficiency for r in self.results]))

    @property
    def average_allocation_efficiency(self) -> float:
        return self.mean_allocation_efficiency

    @property
    def median_allocation_efficiency(self) -> float:
        return float(np.median([r.allocation_efficiency for r in self.results]))

    def with_results(self, results: tuple[BenchmarkResult, ...]) -> "BenchmarkReport":
        return BenchmarkReport(
            id=self.id,
            allocator=self.allocator,
            source=self.source,
            variant_id=self.variant_id,
            results=self.results + results,
        )
