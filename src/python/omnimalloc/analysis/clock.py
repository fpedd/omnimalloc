#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.primitives.allocation import Allocation, TimePoint, VectorClock


def time_components(time_point: TimePoint) -> VectorClock:
    """Return the time point's clock components (scalars become 1-tuples)."""
    if isinstance(time_point, tuple):
        return time_point
    return (time_point,)


def ensure_uniform_dim(allocations: tuple[Allocation, ...]) -> int:
    """Return the shared clock dimension (1 if empty); raise on mixed dims."""
    dims = {alloc.dim for alloc in allocations}
    if len(dims) > 1:
        raise ValueError(
            f"allocations must share one clock dimension, got {sorted(dims)}"
        )
    return dims.pop() if dims else 1
