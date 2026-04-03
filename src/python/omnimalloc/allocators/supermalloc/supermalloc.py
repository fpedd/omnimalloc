#
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from omnimalloc.allocators.base import BaseAllocator

from .analyzers import get_partitions
from .solver import build_solver_problem, solve, static_preorder

if TYPE_CHECKING:
    from omnimalloc.primitives import Allocation
    from omnimalloc.primitives.allocation import IdType


def _auto_budget(n: int) -> int:
    if n <= 20:
        return 100_000
    if n <= 100:
        return n * n * 100
    return 1_000_000


@dataclass(frozen=True)
class SuperMallocConfig:
    budget_s: int | None = None

    @property
    def budget_ns(self) -> int | None:
        if self.budget_s is None:
            return None
        return int(self.budget_s * 1e9)


class SuperMallocAllocator(BaseAllocator):
    def __init__(self, config: SuperMallocConfig) -> None:
        super().__init__()
        self._config = config
        self._start_ns: int | None = None

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations

        n = len(allocations)
        budget = self._budget if self._budget is not None else _auto_budget(n)

        partitions = get_partitions(allocations)

        result_map: dict[IdType, int] = {}

        for partition in partitions:
            part_allocs = list(partition.allocations)
            if not part_allocs:
                continue

            # Distribute budget proportionally, with a minimum
            part_budget = max(budget * len(part_allocs) // n, len(part_allocs) * 10)

            # Build solver problem with static preordering
            # First build with arbitrary order to get the problem structure,
            # then reorder using static_preorder
            initial_problem = build_solver_problem(partition, part_allocs)
            preorder = static_preorder(initial_problem)
            ordered_allocs = [part_allocs[i] for i in preorder]

            problem = build_solver_problem(partition, ordered_allocs)
            offsets = solve(problem, part_budget)

            for i, alloc in enumerate(problem.allocations):
                result_map[alloc.id] = offsets[i]

        return tuple(
            alloc.with_offset(result_map.get(alloc.id, 0)) for alloc in allocations
        )
