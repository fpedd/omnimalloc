#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Iterator

from .allocation import Allocation, TimePoint, VectorClock
from .flow import FlowNetwork


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


def happens_before(end: VectorClock, start: VectorClock) -> bool:
    """Componentwise `end <= start`; mirrors C++ `happens_before` in allocation.cpp."""
    return all(e <= s for e, s in zip(end, start, strict=True))


def happens_before_pairs(
    allocations: tuple[Allocation, ...],
) -> Iterator[tuple[int, int]]:
    """Yield index pairs (i, j) where i's free happens-before j's alloc."""
    starts = [time_components(alloc.start) for alloc in allocations]
    ends = [time_components(alloc.end) for alloc in allocations]
    for i, end in enumerate(ends):
        for j, start in enumerate(starts):
            if i != j and happens_before(end, start):
                yield i, j


def vector_pressure(allocations: tuple[Allocation, ...]) -> int:
    """Max-weight antichain of the happens-before order via min flow.

    Weighted Dilworth dual: the minimum feasible flow where each allocation's
    node must carry at least its size equals the max-weight set of
    pairwise-concurrent allocations.
    """
    n = len(allocations)
    weights = [alloc.size for alloc in allocations]
    total = sum(weights)

    # Nodes: in_i = 2i, out_i = 2i + 1, then source, sink, and the
    # super-source/super-sink of the lower-bound feasibility transform.
    source, sink, feas_source, feas_sink = 2 * n, 2 * n + 1, 2 * n + 2, 2 * n + 3
    network = FlowNetwork(2 * n + 4)
    for i in range(n):
        network.add_edge(source, 2 * i, total)
        network.add_edge(2 * i + 1, sink, total)
        # Lower bound w_i on (in_i, out_i): unbounded residual arc plus the
        # standard super-source/super-sink demand arcs.
        network.add_edge(2 * i, 2 * i + 1, total)
        network.add_edge(feas_source, 2 * i + 1, weights[i])
        network.add_edge(2 * i, feas_sink, weights[i])
    for i, j in happens_before_pairs(allocations):
        network.add_edge(2 * i + 1, 2 * j, total)
    circulation = network.add_edge(sink, source, total)

    if network.max_flow(feas_source, feas_sink) != total:
        raise RuntimeError("lower-bound feasibility flow must saturate")
    feasible = total - circulation[1]
    circulation[1] = 0
    network.reverse(circulation)[1] = 0
    return feasible - network.max_flow(sink, source)
