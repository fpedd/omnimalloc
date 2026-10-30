#
# SPDX-License-Identifier: Apache-2.0
#

from bisect import insort
from collections.abc import Sequence
from itertools import accumulate


def lowest_gap(occupied: list[tuple[int, int]], size: int, alignment: int = 1) -> int:
    """Lowest `alignment` multiple where `size` fits between the ascending ranges."""
    cursor = 0
    for start, end in occupied:
        if start - cursor >= size:
            break
        cursor = max(cursor, -(-end // alignment) * alignment)
    return cursor


def stack_around_pins(
    sizes: Sequence[int],
    offsets: Sequence[int | None],
    alignments: Sequence[int] | None = None,
) -> list[int]:
    """Offset per item in input order: pinned ones keep theirs, the rest stack."""
    occupied = sorted(
        (offset, offset + size)
        for size, offset in zip(sizes, offsets, strict=True)
        if offset is not None
    )
    if alignments is None:
        alignments = [1] * len(sizes)
    if not occupied:
        # Sizes are positive, so no gap ever opens below the top and the scan
        # returns the running total every time. Take it directly: the scan is
        # quadratic and this is the hot path for the baseline allocators.
        if max(alignments, default=1) == 1:
            return list(accumulate(sizes, initial=0))[:-1]
        resolved, top = [], 0
        for size, alignment in zip(sizes, alignments, strict=True):
            resolved.append(-(-top // alignment) * alignment)
            top = resolved[-1] + size
        return resolved

    resolved = []
    for size, offset, alignment in zip(sizes, offsets, alignments, strict=True):
        if offset is not None:
            resolved.append(offset)
            continue
        gap = lowest_gap(occupied, size, alignment)
        resolved.append(gap)
        insort(occupied, (gap, gap + size))
    return resolved
