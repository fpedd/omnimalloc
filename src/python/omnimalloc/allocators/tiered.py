#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Mapping
from dataclasses import dataclass

from omnimalloc._cpp import compute_temporal_overlaps, first_fit_place
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.allocation import IdType

from .base import BaseAllocator
from .greedy_base import (
    order_by_area,
    order_by_conflict,
    order_by_conflict_size,
    order_by_duration,
    order_by_size,
    order_by_start,
)

# Buffer priority for the fast tier: the bytes most expensive to spill are
# considered first, so largest size is the sensible default.
_ORDERS = {
    "size": order_by_size,
    "duration": order_by_duration,
    "area": order_by_area,
    "conflict": order_by_conflict,
    "conflict_size": order_by_conflict_size,
    "start": order_by_start,
}


@dataclass(frozen=True)
class TieredConfig:
    """Configuration for TieredAllocator."""

    capacity: int | None = None
    order: str = "size"

    def __post_init__(self) -> None:
        if self.capacity is not None and self.capacity <= 0:
            raise ValueError(f"capacity must be positive, got {self.capacity}")
        if self.order not in _ORDERS:
            raise ValueError(
                f"unknown order {self.order!r}; choose from {sorted(_ORDERS)}"
            )


class TieredAllocator(BaseAllocator):
    """Pack buffers into a fixed-capacity fast memory and spill the rest above it.

    Models the on-chip/off-chip decision at the heart of ML accelerators. The
    fast memory occupies offsets ``[0, capacity)``; every buffer first-fit can
    place under that ceiling stays on-chip, and the rest are packed contiguously
    above ``capacity`` (the spill region). The result is a single valid
    allocation: a buffer is on-chip iff its ``height`` (offset + size) is at most
    ``capacity``. With ``capacity=None`` the fast memory is unbounded, so nothing
    spills and the allocator reduces to first-fit in ``order``.

    Buffers are considered for the fast tier in ``order`` (default largest size
    first, so the bytes most expensive to spill are kept on-chip).
    """

    def __init__(self, config: TieredConfig | None = None) -> None:
        self._config = config or TieredConfig()

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations
        overlaps = compute_temporal_overlaps(allocations)
        ordered = _ORDERS[self._config.order](allocations)
        placed, spilled = self._pack_fast(ordered, overlaps)
        if spilled:
            placed.update(self._pack_spill(spilled))
        return tuple(a.with_offset(placed[a.id]) for a in allocations)

    def _pack_fast(
        self,
        ordered: tuple[Allocation, ...],
        overlaps: Mapping[IdType, set[IdType]],
    ) -> tuple[dict[IdType, int], list[Allocation]]:
        """Place each buffer at its lowest under-ceiling gap; spill when none fits."""
        capacity = self._config.capacity
        sizes = {a.id: a.size for a in ordered}
        placed: dict[IdType, int] = {}
        spilled: list[Allocation] = []
        for alloc in ordered:
            neighbors = overlaps.get(alloc.id) or ()
            occupied = sorted((placed[n], sizes[n]) for n in neighbors if n in placed)
            cursor = 0
            fits = False
            for offset, size in occupied:
                if offset - cursor >= alloc.size:
                    fits = True
                    break
                cursor = max(cursor, offset + size)
            if not fits and (capacity is None or cursor + alloc.size <= capacity):
                fits = True
            if fits:
                placed[alloc.id] = cursor
            else:
                spilled.append(alloc)
        return placed, spilled

    def _pack_spill(self, spilled: list[Allocation]) -> dict[IdType, int]:
        """Pack the spilled buffers with first-fit, offset above the fast region."""
        base = self._config.capacity or 0
        ordered = order_by_size(tuple(spilled))
        overlaps = compute_temporal_overlaps(ordered)
        return {
            a.id: base + (a.offset or 0) for a in first_fit_place(ordered, overlaps)
        }
