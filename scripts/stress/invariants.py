#
# SPDX-License-Identifier: Apache-2.0
#
"""Invariant checks for the public API, returning failure strings.

The reference implementations here are deliberately naive Python, never the
C++ kernels, so a kernel bug cannot hide behind itself.
"""

import pickle
import random
from collections.abc import Sequence

from omnimalloc import validate_allocation
from omnimalloc._cpp import ConflictGraph
from omnimalloc.analysis import (
    antichain_pressure,
    antichain_pressure_per_allocation,
    closure_pressure,
    closure_pressure_per_allocation,
    conflict_degrees,
    conflict_graph,
    conflicts,
    placement_pressure,
    placement_pressure_per_allocation,
    try_linearize,
)
from omnimalloc.analysis._clock import time_components
from omnimalloc.primitives import Allocation, IdType

Allocs = Sequence[Allocation]

# Above this the exact clique search stops being worth its runtime
CLIQUE_LIMIT = 26


def brute_conflict_pairs(allocations: Allocs) -> set[tuple[int, int]]:
    """Every conflicting index pair, straight off ``Allocation.conflicts_with``."""
    return {
        (i, j)
        for i in range(len(allocations))
        for j in range(i + 1, len(allocations))
        if allocations[i].conflicts_with(allocations[j])
    }


def brute_scalar_pressure(allocations: Allocs) -> int:
    """Max total size live at one instant; scalar lifetimes only.

    Every cut that can be maximal starts at some allocation, so the starts are
    the only instants worth probing.
    """
    spans = [
        (time_components(a.start)[0], time_components(a.end)[0], a.size)
        for a in allocations
    ]
    peak = 0
    for cut in {start for start, _, _ in spans}:
        live = sum(size for start, end, size in spans if start <= cut < end)
        peak = max(peak, live)
    return peak


def brute_collisions(allocations: Allocs) -> list[tuple[int, int]]:
    """Index pairs overlapping in both time and address space."""
    bad = []
    for i, first in enumerate(allocations):
        if first.offset is None:
            continue
        low, high = first.offset, first.offset + first.size
        for j in range(i + 1, len(allocations)):
            second = allocations[j]
            if second.offset is None:
                continue
            other_low, other_high = second.offset, second.offset + second.size
            if low < other_high and other_low < high:
                if first.conflicts_with(second):
                    bad.append((i, j))
    return bad


def brute_max_clique_weight(allocations: Allocs) -> int | None:
    """Exact max-weight clique of the conflict graph, or None when too large.

    A clique must occupy disjoint addresses, so its weight lower-bounds any
    placement's peak, and the maximum is exactly ``antichain_pressure``.
    """
    n = len(allocations)
    if n > CLIQUE_LIMIT:
        return None
    adjacency = [0] * n
    for i, j in brute_conflict_pairs(allocations):
        adjacency[i] |= 1 << j
        adjacency[j] |= 1 << i
    sizes = [a.size for a in allocations]
    best = 0

    def expand(candidates: int, weight: int) -> None:
        nonlocal best
        best = max(best, weight)
        reachable = weight + sum(sizes[k] for k in range(n) if candidates >> k & 1)
        if reachable <= best:
            return
        remaining = candidates
        while remaining:
            pivot = (remaining & -remaining).bit_length() - 1
            remaining &= ~(1 << pivot)
            above_pivot = ~((1 << (pivot + 1)) - 1)
            expand(candidates & adjacency[pivot] & above_pivot, weight + sizes[pivot])

    expand((1 << n) - 1, 0)
    return best


def peak_of(placed: Allocs) -> int:
    """Highest occupied address, ignoring anything still unplaced."""
    heights = [a.height for a in placed if a.height is not None]
    return max(heights, default=0)


def closure_or_none(allocations: Allocs) -> int | None:
    """The default cap refuses wide vector clocks, and that is an answer."""
    try:
        return closure_pressure(allocations)
    except RuntimeError:
        return None


def check_conflicts(allocations: Allocs, brute: bool) -> list[str]:
    """The three conflict entry points must describe one identical relation."""
    fails = []
    n = len(allocations)
    degrees = conflict_degrees(allocations)
    graph = conflict_graph(allocations)

    if len(degrees) != n:
        fails.append(f"conflict_degrees length {len(degrees)} != {n}")
    if len(graph) != n:
        fails.append(f"len(conflict_graph) {len(graph)} != {n}")
    if [graph.degree(i) for i in range(len(graph))] != degrees:
        fails.append("conflict_graph.degree disagrees with conflict_degrees")

    neighbor_sets = [set(graph.neighbors(i)) for i in range(len(graph))]
    fails += _check_rows(graph, neighbor_sets, degrees)

    total = sum(degrees)
    if total % 2:
        fails.append(f"degree sum {total} is odd")
    if graph.pair_count != total // 2:
        fails.append(f"pair_count {graph.pair_count} != half the degree sum")

    fails += _check_conflict_map(allocations, neighbor_sets)
    if brute:
        graph_pairs = {
            (min(i, j), max(i, j))
            for i, neighbors in enumerate(neighbor_sets)
            for j in neighbors
        }
        expected = brute_conflict_pairs(allocations)
        if graph_pairs != expected:
            fails.append(
                f"conflict_graph vs conflicts_with: {len(expected - graph_pairs)} "
                f"missing, {len(graph_pairs - expected)} extra"
            )
    return fails


def _check_rows(
    graph: ConflictGraph, neighbor_sets: list[set[int]], degrees: list[int]
) -> list[str]:
    """Each row is duplicate-free, irreflexive, symmetric, and the right length."""
    for i, neighbors in enumerate(neighbor_sets):
        if len(neighbors) != len(graph.neighbors(i)):
            return [f"conflict_graph.neighbors({i}) repeats a neighbor"]
        if i in neighbors:
            return [f"conflict_graph is reflexive at {i}"]
        if len(neighbors) != degrees[i]:
            return [f"degree {degrees[i]} disagrees with |neighbors| at {i}"]
        if any(i not in neighbor_sets[j] for j in neighbors):
            return [f"conflict_graph is asymmetric at {i}"]
    return []


def _check_conflict_map(
    allocations: Allocs, neighbor_sets: list[set[int]]
) -> list[str]:
    ids = [a.id for a in allocations]
    if len(set(ids)) != len(ids):
        return []
    mapping = conflicts(allocations)
    if set(mapping) != set(ids):
        return ["conflicts() keys differ from the allocation ids"]
    for i, alloc in enumerate(allocations):
        if mapping[alloc.id] != {ids[j] for j in neighbor_sets[i]}:
            return [f"conflicts()[{alloc.id!r}] differs from the graph"]
    return []


def check_pressure(allocations: Allocs, brute: bool) -> list[str]:
    """The bound order, the per-allocation split, and the exact references."""
    antichain = antichain_pressure(allocations)
    closure = closure_or_none(allocations)
    fails = _check_bound_range(allocations, antichain, closure)
    fails += _check_per_allocation(allocations, antichain, closure)

    if allocations and allocations[0].dim == 1:
        reference = brute_scalar_pressure(allocations)
        if antichain != reference:
            fails.append(f"scalar antichain {antichain} != sweep {reference}")
        if closure != reference:
            fails.append(f"scalar closure {closure} != sweep {reference}")
    if brute:
        clique = brute_max_clique_weight(allocations)
        if clique is not None and clique != antichain:
            fails.append(f"antichain {antichain} != exact max clique {clique}")
    return fails


def _check_bound_range(
    allocations: Allocs, antichain: int, closure: int | None
) -> list[str]:
    """Both bounds sit between the largest single size and the total."""
    largest = max((a.size for a in allocations), default=0)
    fails = []
    if antichain < 0:
        fails.append(f"negative antichain_pressure {antichain}")
    if allocations and antichain < largest:
        fails.append(f"antichain_pressure {antichain} below largest size {largest}")
    if antichain > sum(a.size for a in allocations):
        fails.append(f"antichain_pressure {antichain} above the total size")
    if closure is None:
        return fails
    if closure > antichain:
        fails.append(f"closure_pressure {closure} above antichain {antichain}")
    if allocations and closure < largest:
        fails.append(f"closure_pressure {closure} below largest size {largest}")
    return fails


def _check_per_allocation(
    allocations: Allocs, antichain: int, closure: int | None
) -> list[str]:
    """Each per-allocation map peaks at its aggregate and covers its own size."""
    ids = [a.id for a in allocations]
    if len(set(ids)) != len(ids):
        return []
    fails = []
    per_antichain = antichain_pressure_per_allocation(allocations)
    if max(per_antichain.values(), default=0) != antichain:
        fails.append("max antichain_pressure_per_allocation != antichain_pressure")
    for alloc in allocations:
        if per_antichain[alloc.id] < alloc.size:
            fails.append(f"antichain per-allocation below own size at {alloc.id!r}")
            break
    if closure is None:
        return fails

    per_closure = closure_pressure_per_allocation(allocations)
    if max(per_closure.values(), default=0) != closure:
        fails.append("max closure_pressure_per_allocation != closure_pressure")
    for alloc in allocations:
        if per_closure[alloc.id] < alloc.size:
            fails.append(f"closure per-allocation below own size at {alloc.id!r}")
            break
        if per_closure[alloc.id] > per_antichain[alloc.id]:
            fails.append(f"per-allocation closure above antichain at {alloc.id!r}")
            break
    return fails


def check_order_invariance(allocations: Allocs, rng: random.Random) -> list[str]:
    """Analysis reads a set, so a shuffled input must answer identically."""
    order = list(range(len(allocations)))
    rng.shuffle(order)
    shuffled = tuple(allocations[i] for i in order)
    fails = []

    if antichain_pressure(shuffled) != antichain_pressure(allocations):
        fails.append("antichain_pressure depends on input order")
    if closure_or_none(shuffled) != closure_or_none(allocations):
        fails.append("closure_pressure depends on input order")

    shuffled_degrees = conflict_degrees(shuffled)
    restored = [shuffled_degrees[order.index(i)] for i in range(len(order))]
    if restored != conflict_degrees(allocations):
        fails.append("conflict_degrees is not permutation-equivariant")

    ids = [a.id for a in allocations]
    if len(set(ids)) == len(ids):
        if antichain_pressure_per_allocation(
            shuffled
        ) != antichain_pressure_per_allocation(allocations):
            fails.append("antichain_pressure_per_allocation depends on input order")
    return fails


def check_lane_invariance(allocations: Allocs, rng: random.Random) -> list[str]:
    """Clock lanes are unordered, so permuting them must change nothing."""
    dim = allocations[0].dim if allocations else 1
    if dim < 2:
        return []
    lanes = list(range(dim))
    rng.shuffle(lanes)
    permuted = tuple(
        Allocation(
            id=a.id,
            size=a.size,
            start=tuple(time_components(a.start)[k] for k in lanes),
            end=tuple(time_components(a.end)[k] for k in lanes),
            offset=a.offset,
            kind=a.kind,
        )
        for a in allocations
    )
    fails = []
    if antichain_pressure(permuted) != antichain_pressure(allocations):
        fails.append("antichain_pressure is not lane-permutation invariant")
    if closure_or_none(permuted) != closure_or_none(allocations):
        fails.append("closure_pressure is not lane-permutation invariant")
    if conflict_degrees(permuted) != conflict_degrees(allocations):
        fails.append("conflict_degrees is not lane-permutation invariant")
    return fails


def check_linearize(allocations: Allocs) -> list[str]:
    """A linearization must preserve ids, sizes, and the conflict relation."""
    linearized = try_linearize(allocations)
    if linearized is None:
        return []

    if len(linearized) != len(allocations):
        return [f"try_linearize returned {len(linearized)} of {len(allocations)}"]
    if [a.id for a in linearized] != [a.id for a in allocations]:
        return ["try_linearize reordered or renamed the allocations"]

    fails = []
    if any(a.dim != 1 for a in linearized):
        fails.append("try_linearize returned non-scalar lifetimes")
    if [a.size for a in linearized] != [a.size for a in allocations]:
        fails.append("try_linearize changed a size")
    if brute_conflict_pairs(linearized) != brute_conflict_pairs(allocations):
        fails.append("try_linearize changed the conflict relation")
    if antichain_pressure(linearized) != antichain_pressure(allocations):
        fails.append("try_linearize changed antichain_pressure")
    return fails


def check_placement(
    allocations: Allocs, placed: Allocs, label: str, pins: dict[IdType, int]
) -> list[str]:
    """The allocator exit contract, re-derived without trusting the allocator."""
    if len(placed) != len(allocations):
        return [f"{label}: returned {len(placed)} of {len(allocations)}"]
    original = {a.id: a for a in allocations}
    if {a.id for a in placed} != set(original):
        return [f"{label}: returned a different id set"]

    fails = _check_preserved(placed, original, label)
    for alloc in placed:
        pinned_at = pins.get(alloc.id)
        if pinned_at is not None and alloc.offset != pinned_at:
            fails.append(f"{label}: pin {alloc.id!r} moved to {alloc.offset}")
            break
    if any(a.offset is None for a in placed):
        return fails

    collisions = brute_collisions(placed)
    if collisions:
        first, second = collisions[0]
        fails.append(
            f"{label}: {len(collisions)} collisions, first {placed[first].id!r} "
            f"against {placed[second].id!r}"
        )
    try:
        validate_allocation(placed)
    except ValueError as e:
        fails.append(f"{label}: validate_allocation rejected the result, {e}")

    peak = peak_of(placed)
    if placement_pressure(placed) != peak:
        fails.append(f"{label}: placement_pressure disagrees with the max height")
    fails += _check_placement_peaks(placed, peak, label)
    fails += _check_above_bounds(allocations, peak, label)
    return fails


def _check_above_bounds(allocations: Allocs, peak: int, label: str) -> list[str]:
    """No placement can beat a bound that every placement must respect."""
    fails = []
    bound = antichain_pressure(allocations)
    if peak < bound:
        fails.append(f"{label}: peak {peak} below the exact lower bound {bound}")
    closure = closure_or_none(allocations)
    if closure is not None and peak < closure:
        fails.append(f"{label}: peak {peak} below the closure bound {closure}")
    return fails


def _check_preserved(
    placed: Allocs, original: dict[IdType, Allocation], label: str
) -> list[str]:
    """An allocator assigns offsets and touches nothing else."""
    for alloc in placed:
        source = original[alloc.id]
        if alloc.offset is None:
            return [f"{label}: left {alloc.id!r} unplaced"]
        if alloc.offset < 0:
            return [f"{label}: gave {alloc.id!r} a negative offset {alloc.offset}"]
        if alloc.size != source.size:
            return [f"{label}: changed the size of {alloc.id!r}"]
        if alloc.start != source.start or alloc.end != source.end:
            return [f"{label}: changed the lifetime of {alloc.id!r}"]
        if alloc.kind != source.kind:
            return [f"{label}: changed the kind of {alloc.id!r}"]
    return []


def _check_placement_peaks(placed: Allocs, peak: int, label: str) -> list[str]:
    ids = [a.id for a in placed]
    if len(set(ids)) != len(ids):
        return []
    per_alloc = placement_pressure_per_allocation(placed)
    if max(per_alloc.values(), default=0) != peak:
        return [f"{label}: max placement_pressure_per_allocation != peak"]
    for alloc in placed:
        if per_alloc[alloc.id] < (alloc.height or 0):
            return [f"{label}: per-allocation peak below own height"]
    return []


def check_pickle(allocations: Allocs) -> list[str]:
    """Allocations cross a process boundary in the parallel allocators."""
    restored = pickle.loads(pickle.dumps(tuple(allocations)))  # noqa: S301
    if list(restored) != list(allocations):
        return ["pickle round-trip changed the allocations"]
    if [a.dim for a in restored] != [a.dim for a in allocations]:
        return ["pickle round-trip changed the clock dimensions"]
    return []
