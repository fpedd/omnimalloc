#
# SPDX-License-Identifier: Apache-2.0
#

from enum import IntEnum
from itertools import accumulate

from omnimalloc._cpp import (
    GreedyOrder,
    compute_allocation_peaks,
    compute_temporal_overlaps,
)

from .allocation import Allocation, IdType, VectorClock
from .utils import ensure_unique_ids
from .vector_clock import (
    ensure_uniform_dim,
    happens_before,
    time_components,
    vector_pressure,
)

DEFAULT_CLOSURE_CAP = 1 << 12


class Guarantee(IntEnum):
    """Minimum guarantee on each reported per-allocation peak; higher is tighter."""

    BOUND = 1  # sound upper bound: allocation-derived via a greedy placement
    ANTICHAIN = 2  # exact pinned antichain, tightest order-determined bound
    EXACT = 3  # exact realizable peak


def get_conflicts(allocations: tuple[Allocation, ...]) -> dict[IdType, set[IdType]]:
    """Map each allocation id to the ids of temporally conflicting allocations."""
    ensure_unique_ids(allocations)
    ensure_uniform_dim(allocations)
    conflicts: dict[IdType, set[IdType]] = {alloc.id: set() for alloc in allocations}
    conflicts.update(compute_temporal_overlaps(allocations))
    return conflicts


def get_pressure(
    allocations: tuple[Allocation, ...],
    guarantee: Guarantee = Guarantee.BOUND,
    closure_cap: int = DEFAULT_CLOSURE_CAP,
    placer: GreedyOrder = GreedyOrder.ALL,
) -> int:
    """Peak memory pressure across all allocation lifetimes."""
    if ensure_uniform_dim(allocations) == 1:
        return _scalar_pressure(allocations)
    if guarantee == Guarantee.BOUND:
        return max(compute_allocation_peaks(list(allocations), placer))
    if guarantee == Guarantee.ANTICHAIN:
        return vector_pressure(allocations)
    return _exact_pressure(allocations, closure_cap)


def get_per_allocation_pressure(
    allocations: tuple[Allocation, ...],
    guarantee: Guarantee = Guarantee.BOUND,
    closure_cap: int = DEFAULT_CLOSURE_CAP,
    placer: GreedyOrder = GreedyOrder.ALL,
) -> dict[IdType, int]:
    """Peak pressure over each allocation's own lifetime, per allocation id."""
    ensure_unique_ids(allocations)
    if not allocations:
        return {}
    ids = [alloc.id for alloc in allocations]
    if ensure_uniform_dim(allocations) == 1:
        return dict(zip(ids, _scalar_peaks(allocations), strict=True))
    if guarantee == Guarantee.BOUND:
        peaks = compute_allocation_peaks(list(allocations), placer)
        return dict(zip(ids, peaks, strict=True))
    neighbors = _neighbor_indices(allocations, ids)
    births = [time_components(alloc.start) for alloc in allocations]
    deaths = [time_components(alloc.end) for alloc in allocations]
    weights = [alloc.size for alloc in allocations]
    peaks = _closure_peaks(births, deaths, weights, closure_cap)
    if peaks is None:
        peaks = _certified_peaks(
            allocations, births, deaths, weights, neighbors, guarantee
        )
    return dict(zip(ids, peaks, strict=True))


def _scalar_pressure(allocations: tuple[Allocation, ...]) -> int:
    """Sweep-line peak of concurrently live sizes on a scalar timeline."""
    events = [(alloc.start, alloc.size) for alloc in allocations]
    events.extend((alloc.end, -alloc.size) for alloc in allocations)
    events.sort()

    max_pressure = current = 0
    for _, delta in events:
        current += delta
        max_pressure = max(max_pressure, current)

    return max_pressure


def _scalar_peaks(allocations: tuple[Allocation, ...]) -> list[int]:
    """Sweep peaks: each allocation's max pressure over its own [start, end)."""
    bounds = sorted({t for alloc in allocations for t in (alloc.start, alloc.end)})
    index = {t: k for k, t in enumerate(bounds)}
    deltas = [0] * len(bounds)
    for alloc in allocations:
        deltas[index[alloc.start]] += alloc.size
        deltas[index[alloc.end]] -= alloc.size
    pressure = list(accumulate(deltas))
    return [
        max(pressure[index[alloc.start] : index[alloc.end]]) for alloc in allocations
    ]


def _neighbor_indices(
    allocations: tuple[Allocation, ...], ids: list[IdType]
) -> list[list[int]]:
    index = {alloc_id: i for i, alloc_id in enumerate(ids)}
    conflicts = compute_temporal_overlaps(allocations)
    return [
        sorted(index[other] for other in conflicts.get(alloc_id, ()))
        for alloc_id in ids
    ]


def _exact_pressure(allocations: tuple[Allocation, ...], closure_cap: int) -> int:
    """Realizable peak via join closure, else antichain certified by residency."""
    births = [time_components(alloc.start) for alloc in allocations]
    deaths = [time_components(alloc.end) for alloc in allocations]
    weights = [alloc.size for alloc in allocations]
    peaks = _closure_peaks(births, deaths, weights, closure_cap)
    if peaks is not None:
        return max(peaks)
    antichain = vector_pressure(allocations)
    neighbors = _neighbor_indices(allocations, [alloc.id for alloc in allocations])
    resident = max(
        _grow_resident(i, births, deaths, weights, neighbors[i])[1]
        for i in range(len(allocations))
    )
    if resident == antichain:
        return antichain
    raise RuntimeError(
        "Exact pressure unresolved: join-closure cap exceeded and the best "
        "residency certificate is below the antichain bound; raise closure_cap "
        "or request Guarantee.ANTICHAIN"
    )


def _join(a: VectorClock, b: VectorClock) -> VectorClock:
    return tuple(max(x, y) for x, y in zip(a, b, strict=True))


def _resident(cut: VectorClock, birth: VectorClock, death: VectorClock) -> bool:
    return happens_before(birth, cut) and not happens_before(death, cut)


def _join_closure(births: list[VectorClock], cap: int) -> set[VectorClock] | None:
    """Join-closure of the birth frontiers, or None once it exceeds cap."""
    cuts = set(births)
    if len(cuts) > cap:
        return None
    frontier = list(cuts)
    while frontier:
        cut = frontier.pop()
        for birth in births:
            joined = _join(cut, birth)
            if joined not in cuts:
                if len(cuts) >= cap:
                    return None
                cuts.add(joined)
                frontier.append(joined)
    return cuts


def _closure_peaks(
    births: list[VectorClock], deaths: list[VectorClock], weights: list[int], cap: int
) -> list[int] | None:
    """Exact peaks by scoring every join-closure cut, or None if capped."""
    cuts = _join_closure(births, cap)
    if cuts is None:
        return None
    peaks = list(weights)
    lifetimes = list(zip(births, deaths, strict=True))
    for cut in cuts:
        live = [
            i
            for i, (birth, death) in enumerate(lifetimes)
            if _resident(cut, birth, death)
        ]
        weight = sum(weights[i] for i in live)
        for i in live:
            peaks[i] = max(peaks[i], weight)
    return peaks


def _grow_resident(
    seed: int,
    births: list[VectorClock],
    deaths: list[VectorClock],
    weights: list[int],
    neighbors: list[int],
) -> tuple[list[int], int]:
    """Greedy jointly-resident set containing seed: a lower bound on its peak."""
    members = [seed]
    cut = births[seed]
    for j in sorted(neighbors, key=lambda j: -weights[j]):
        joined = _join(cut, births[j])
        if all(_resident(joined, births[m], deaths[m]) for m in (*members, j)):
            members.append(j)
            cut = joined
    return members, sum(weights[m] for m in members)


def _certified_peaks(
    allocations: tuple[Allocation, ...],
    births: list[VectorClock],
    deaths: list[VectorClock],
    weights: list[int],
    neighbors: list[list[int]],
    guarantee: Guarantee,
) -> list[int]:
    """Pinned antichain bounds, certified exact where a resident set matches."""
    n = len(allocations)
    ubs = [
        weights[i]
        + get_pressure(tuple(allocations[j] for j in neighbors[i]), Guarantee.ANTICHAIN)
        for i in range(n)
    ]
    lbs = list(weights)
    for i in range(n):
        members, weight = _grow_resident(i, births, deaths, weights, neighbors[i])
        for m in members:
            lbs[m] = max(lbs[m], weight)
    if guarantee == Guarantee.EXACT:
        unresolved = [allocations[i].id for i in range(n) if lbs[i] < ubs[i]]
        if unresolved:
            raise RuntimeError(
                f"Exact peaks unresolved for {unresolved!r}: join-closure cap "
                "exceeded and residency certificates left gaps; raise "
                "closure_cap or request Guarantee.ANTICHAIN"
            )
    return ubs
