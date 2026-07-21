#
# SPDX-License-Identifier: Apache-2.0
#

import math
from typing import Any, cast

from omnimalloc.common.constants import DEFAULT_TIMEOUT, TB
from omnimalloc.common.deadline import ensure_valid_timeout
from omnimalloc.common.validation import ensure_positive
from omnimalloc.primitives import Allocation

from .base import BaseAllocator

try:
    import minimalloc as mm  # type: ignore
except ImportError:
    mm = cast("Any", None)

HAS_MINIMALLOC = mm is not None


def _require_minimalloc() -> None:
    """Re-check availability at use time so installs after package import work."""
    global mm, HAS_MINIMALLOC
    if mm is not None:
        return
    try:
        import minimalloc  # type: ignore
    except ImportError:
        # TODO(fpedd): Make minimalloc more easily installable via PyPI
        raise ImportError(
            "The MinimallocAllocator feature requires 'minimalloc' which is not "
            "installed.\nInstall manually: pip install git+https://github.com/google/minimalloc.git"
        ) from None
    mm, HAS_MINIMALLOC = minimalloc, True


def _to_buffer(allocation: Allocation) -> "mm.Buffer":
    return mm.Buffer(
        id=str(allocation.id),
        size=allocation.size,
        lifespan=mm.Lifespan(lower=allocation.start, upper=allocation.end),
    )


class MinimallocAllocator(BaseAllocator):
    """Wrapper for Google's minimalloc constraint-based allocator."""

    # mm.Lifespan is inherently an interval on one timeline
    supports_vector_time = False

    def __init__(
        self, timeout: float | None = DEFAULT_TIMEOUT, capacity: int = 1 * TB
    ) -> None:
        _require_minimalloc()
        ensure_valid_timeout(timeout)
        ensure_positive(capacity, "capacity")
        self._timeout = timeout
        self._capacity = capacity

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        problem = mm.Problem(buffers=[_to_buffer(a) for a in allocations])
        problem.capacity = self._capacity

        params = mm.SolverParams()
        # minimalloc's own default timeout is infinite, matching None here;
        # its solver takes whole seconds, so round up to never shorten the budget
        if self._timeout is not None:
            params.timeout = math.ceil(self._timeout)
        params.minimize_capacity = True

        solution = mm.Solver(params).solve(problem)
        if solution is None:
            raise RuntimeError("Minimalloc failed to find a solution")

        return tuple(
            allocation.with_offset(offset)
            for allocation, offset in zip(allocations, solution.offsets, strict=True)
        )
