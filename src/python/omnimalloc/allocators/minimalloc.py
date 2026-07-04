#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Any, cast

from omnimalloc.common.optional import OptionalDependencyError
from omnimalloc.common.units import TB
from omnimalloc.primitives import Allocation

from .base import BaseAllocator, require_unique_ids

try:
    import minimalloc as mm  # type: ignore

    HAS_MINIMALLOC = True
except ImportError:
    HAS_MINIMALLOC = False
    mm = cast("Any", None)


def _to_buffer(allocation: Allocation) -> "mm.Buffer":
    return mm.Buffer(
        id=str(allocation.id),
        size=allocation.size,
        lifespan=mm.Lifespan(lower=allocation.start, upper=allocation.end),
    )


class MinimallocAllocator(BaseAllocator):
    """Wrapper for Google's minimalloc constraint-based allocator."""

    def __init__(self, timeout: int = 10, max_capacity: int = 1 * TB) -> None:
        if not HAS_MINIMALLOC:
            # TODO(fpedd): Make minimalloc more easily installable via PyPI
            raise OptionalDependencyError(
                "The MinimallocAllocator feature requires 'minimalloc' which is not "
                "installed.\nInstall manually: pip install git+https://github.com/google/minimalloc.git"
            )

        self._timeout = timeout
        self._max_capacity = max_capacity

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations

        require_unique_ids(allocations)

        problem = mm.Problem(buffers=[_to_buffer(a) for a in allocations])
        problem.capacity = self._max_capacity

        params = mm.SolverParams()
        params.timeout = self._timeout
        params.minimize_capacity = True

        solution = mm.Solver(params).solve(problem)
        if solution is None:
            raise RuntimeError("Minimalloc failed to find a solution")
        if len(solution.offsets) != len(allocations):
            raise ValueError(
                f"Num offsets {len(solution.offsets)} != "
                f"num allocations {len(allocations)}"
            )

        return tuple(
            allocation.with_offset(offset)
            for allocation, offset in zip(allocations, solution.offsets, strict=False)
        )
