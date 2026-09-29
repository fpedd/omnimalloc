#
# SPDX-License-Identifier: Apache-2.0
#

import math
from collections.abc import Sequence
from itertools import accumulate


def stack_around_pins(sizes: Sequence[int], offsets: Sequence[int | None]) -> list[int]:
    """Offset per item in input order: pinned ones keep theirs, the rest stack."""
    if all(offset is None for offset in offsets):
        return list(accumulate(sizes, initial=0))[:-1]
    # Gap i spans [los[i], his[i]); free items fill each gap from below
    los: list[int] = []
    his: list[float] = []
    lo = 0
    for start, end in sorted(
        (offset, offset + size)
        for size, offset in zip(sizes, offsets, strict=True)
        if offset is not None and size > 0
    ):
        if start > lo:
            los.append(lo)
            his.append(start)
        lo = max(lo, end)
    los.append(lo)
    his.append(math.inf)

    # Max tree over gap widths: leftmost fitting gap in O(log gaps), not quadratic
    leaves = 1 << (len(los) - 1).bit_length()
    free = [0.0] * (2 * leaves)
    widths = [hi - lo for lo, hi in zip(los, his, strict=True)]
    free[leaves : leaves + len(widths)] = widths
    for node in range(leaves - 1, 0, -1):
        free[node] = max(free[2 * node], free[2 * node + 1])

    resolved = []
    for size, offset in zip(sizes, offsets, strict=True):
        if offset is None:
            node = 1
            while node < leaves:
                node = 2 * node if free[2 * node] >= size else 2 * node + 1
            gap = node - leaves
            offset = los[gap]
            los[gap] += size
            free[node] -= size
            while node > 1:
                node //= 2
                free[node] = max(free[2 * node], free[2 * node + 1])
        resolved.append(offset)
    return resolved


def first_overlap(ranges: Sequence[tuple[int, int]]) -> tuple[int, int] | None:
    """Indices of two half-open ranges that intersect, or None; empty ones never do."""
    last = None
    for index in sorted(range(len(ranges)), key=ranges.__getitem__):
        lo, hi = ranges[index]
        if lo < hi:
            if last is not None and lo < ranges[last][1]:
                return last, index
            last = index
    return None
