#
# SPDX-License-Identifier: Apache-2.0
#

from .allocation import Allocation


def get_pressure(allocations: tuple[Allocation, ...]) -> int:
    """Calculate maximum memory pressure across all allocation intervals."""
    events = [(alloc.start, alloc.size) for alloc in allocations]
    events.extend((alloc.end, -alloc.size) for alloc in allocations)
    events.sort()

    max_pressure = current = 0
    for _, delta in events:
        current += delta
        max_pressure = max(max_pressure, current)

    return max_pressure
