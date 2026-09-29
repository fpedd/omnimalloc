#
# SPDX-License-Identifier: Apache-2.0
#

import math
from collections.abc import Sequence


def stack_around_pins(sizes: Sequence[int], offsets: Sequence[int | None]) -> list[int]:
    """Offset per item in input order: pinned ones keep theirs, the rest stack.

    Each free item takes the lowest gap between the pins that fits it (first
    fit); the space above the highest pin is unbounded.
    """
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

    resolved = []
    for size, offset in zip(sizes, offsets, strict=True):
        if offset is not None:
            resolved.append(offset)
            continue
        gap = 0
        while his[gap] - los[gap] < size:
            gap += 1
        resolved.append(los[gap])
        los[gap] += size
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
