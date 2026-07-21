#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from .allocation import Allocation


def ensure_unique_ids(allocations: Sequence[Allocation]) -> None:
    """Raise if any allocation id repeats; id-keyed placement assumes uniqueness."""
    if len({alloc.id for alloc in allocations}) != len(allocations):
        raise ValueError("allocation ids must be unique")


def ensure_allocations(allocations: object) -> tuple[Allocation, ...]:
    """Coerce a raw sequence to a tuple, requiring every element be an Allocation."""
    if isinstance(allocations, str | bytes) or not isinstance(allocations, Sequence):
        raise TypeError(f"Unsupported entity type: {type(allocations)!r}")
    checked: list[Allocation] = []
    for alloc in allocations:
        if not isinstance(alloc, Allocation):
            raise TypeError(f"Expected Allocation, got {type(alloc)!r}")
        checked.append(alloc)
    return tuple(checked)
