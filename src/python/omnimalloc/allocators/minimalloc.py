#
# SPDX-License-Identifier: Apache-2.0
#


from omnimalloc.common.optional import OptionalDependencyError
from omnimalloc.common.units import TB
from omnimalloc.primitives import Allocation, Pool

from .base import BaseAllocator

try:
    import minimalloc as mm  # type: ignore

    HAS_MINIMALLOC = True
except ImportError:
    from types import SimpleNamespace
    from typing import Any

    HAS_MINIMALLOC = False
    mm: Any = SimpleNamespace(
        Buffer=None,
        Lifespan=None,
        Problem=None,
        Solution=None,
        Solver=None,
        SolverParams=None,
    )


def _om_allocation_to_mm_buffer(allocation: Allocation) -> "mm.Buffer":
    return mm.Buffer(
        id=str(allocation.id),
        size=allocation.size,
        lifespan=mm.Lifespan(lower=allocation.start, upper=allocation.end),
    )


def _om_pool_to_mm_problem(pool: Pool) -> "mm.Problem":
    buffers = [
        _om_allocation_to_mm_buffer(allocation) for allocation in pool.allocations
    ]
    return mm.Problem(buffers=buffers)


def _mm_problem_to_om_pool(
    problem: "mm.Problem | None", solution: "mm.Solution | None" = None
) -> Pool | None:
    if problem is None or solution is None:
        return None

    buffers = problem.buffers
    offsets = solution.offsets

    if len(offsets) != len(buffers):
        raise ValueError(f"Num offsets {len(offsets)} != num buffers {len(buffers)}")

    for buffer, offset in zip(buffers, offsets, strict=False):
        buffer.offset = offset

    allocations = tuple(
        Allocation(
            id=buffer.id,
            size=buffer.size,
            start=buffer.lifespan.lower,
            end=buffer.lifespan.upper,
            offset=buffer.offset,
        )
        for buffer in problem.buffers
    )

    return Pool(id=0, allocations=allocations)


def _run_solver(
    problem: "mm.Problem", timeout: int, capacity: int, minimize: bool = True
) -> "mm.Solution | None":
    params = mm.SolverParams()
    params.timeout = timeout
    params.minimize_capacity = minimize
    problem.capacity = capacity

    solver = mm.Solver(params)
    solution = solver.solve(problem)

    return solution


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
        problem = _om_pool_to_mm_problem(Pool(id=0, allocations=allocations))
        solution = _run_solver(problem, self._timeout, self._max_capacity)
        pool = _mm_problem_to_om_pool(problem, solution)
        if pool is None:
            raise RuntimeError("Minimalloc failed to find a solution")
        return tuple(pool.allocations)
