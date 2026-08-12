#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass
from pathlib import Path

from omnimalloc.allocators import BaseAllocator
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.primitives import IdType, Pool
from omnimalloc.visualize import plot_allocation

from .utils import source_label

# Pressure analysis is budgeted and gives up on wide vector clocks. A timing
# is still good data there, so the quantities derived from pressure become
# unknown rather than fatal.


@dataclass(frozen=True)
class BenchmarkResult:
    """A single benchmark execution result."""

    id: IdType
    allocator: BaseAllocator | str
    source: BaseSource | str
    entity: Pool  # TODO(fpedd): Add support for Memory and System
    duration: float

    def __post_init__(self) -> None:
        if not self.entity.is_allocated:
            raise ValueError(f"entity {self.entity} is not allocated")
        if self.duration < 0:
            raise ValueError(f"duration must be non-negative, got {self.duration}")

    @property
    def allocator_name(self) -> str:
        return str(self.allocator)

    @property
    def source_name(self) -> str:
        return source_label(self.source)

    @property
    def allocation_efficiency(self) -> float | None:
        """Efficiency, or None where the pressure is out of analysis reach."""
        try:
            return self.entity.efficiency
        except RuntimeError:
            return None

    @property
    def peak_size(self) -> int:
        """Memory the placement actually needs."""
        return self.entity.size

    @property
    def lower_bound(self) -> int | None:
        """Peak pressure of the instance: no placement can go below it."""
        try:
            return self.entity.pressure
        except RuntimeError:
            return None

    @property
    def num_allocations(self) -> int:
        return len(self.entity.allocations)

    def visualize(self, path: Path | str | None = None) -> None:
        plot_allocation(self.entity, path)
