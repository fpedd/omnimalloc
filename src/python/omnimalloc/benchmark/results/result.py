#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass
from pathlib import Path

from omnimalloc.allocators import BaseAllocator
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.primitives import IdType, Pool
from omnimalloc.visualize import plot_allocation


@dataclass(frozen=True)
class BenchmarkResult:
    """A single benchmark execution result."""

    id: IdType
    allocator: BaseAllocator | type[BaseAllocator] | str
    source: BaseSource | type[BaseSource] | str
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
        return str(self.source)

    @property
    def allocation_efficiency(self) -> float:
        return self.entity.efficiency

    @property
    def num_allocations(self) -> int:
        return len(self.entity.allocations)

    def visualize(
        self, file_path: Path | str | None = None, show_inline: bool = False
    ) -> None:
        plot_allocation(self.entity, file_path=file_path, show_inline=show_inline)
