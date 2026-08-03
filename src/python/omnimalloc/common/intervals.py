#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence
from itertools import accumulate
from math import inf


class FreeGaps:
    """Maximal free ranges above a set of occupied ones, claimed first-fit.

    A claim takes space at a range's low end, so it only ever shrinks that range
    and never splits one: the range count is fixed at what the occupied ranges
    leave. That lets a max tree over the capacities answer each first fit in
    O(log n), where rescanning the occupied ranges costs the whole set per claim.
    """

    def __init__(self, occupied: Sequence[tuple[int, int]]) -> None:
        self._offsets = []
        capacities: list[float] = []
        cursor = 0
        for start, end in sorted(occupied):
            if start > cursor:
                self._offsets.append(cursor)
                capacities.append(start - cursor)
            cursor = max(cursor, end)
        # The range above every occupied one is unbounded, so a claim that fits
        # nowhere below still lands, and the tree root always admits a descent
        self._offsets.append(cursor)
        capacities.append(inf)
        self._build(capacities)

    def _build(self, capacities: list[float]) -> None:
        self._leaves = 1 << (len(capacities) - 1).bit_length()
        self._tree: list[float] = [-inf] * (2 * self._leaves)
        self._tree[self._leaves : self._leaves + len(capacities)] = capacities
        for node in range(self._leaves - 1, 0, -1):
            self._tree[node] = max(self._tree[2 * node], self._tree[2 * node + 1])

    def claim(self, size: int) -> int:
        """Lowest offset where `size` fits, marking that space occupied."""
        if size == 0:
            return 0  # reserves nothing, and an empty range collides with nothing
        node = 1
        while node < self._leaves:
            node *= 2
            if self._tree[node] < size:
                node += 1
        index = node - self._leaves
        offset = self._offsets[index]
        self._offsets[index] = offset + size
        self._tree[node] -= size
        node //= 2
        while node >= 1:
            self._tree[node] = max(self._tree[2 * node], self._tree[2 * node + 1])
            node //= 2
        return offset


def stack_around_pins(sizes: Sequence[int], offsets: Sequence[int | None]) -> list[int]:
    """Offset per item in input order: pinned ones keep theirs, the rest stack."""
    occupied = [
        (offset, offset + size)
        for size, offset in zip(sizes, offsets, strict=True)
        if offset is not None
    ]
    if not occupied:
        # Sizes are non-negative, so no gap ever opens below the top and every
        # claim returns the running total. Take it directly: the baseline
        # allocators run this path over millions of allocations.
        return list(accumulate(sizes, initial=0))[:-1]

    gaps = FreeGaps(occupied)
    return [
        offset if offset is not None else gaps.claim(size)
        for size, offset in zip(sizes, offsets, strict=True)
    ]
