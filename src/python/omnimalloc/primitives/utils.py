#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence
from typing import Any

from .allocation import Allocation


def ensure_unique_ids(entities: Sequence[Any], kind: str) -> None:
    """Raise if any id repeats; id-keyed placement assumes uniqueness."""
    seen: dict[Any, int] = {}
    for index, entity in enumerate(entities):
        if not hasattr(entity, "id"):
            raise TypeError(f"Expected an entity with an id, got {type(entity)!r}")
        if entity.id in seen:
            raise ValueError(
                f"{kind} ids must be unique: duplicate id {entity.id!r} "
                f"at indices {seen[entity.id]} and {index}"
            )
        seen[entity.id] = index


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
