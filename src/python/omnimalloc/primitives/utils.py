#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence
from typing import Protocol

from .allocation import Allocation, IdType


class HasId(Protocol):
    @property
    def id(self) -> IdType: ...


def ensure_unique_ids(entities: Sequence[HasId], kind: str) -> None:
    """Raise if any id repeats; id-keyed placement assumes uniqueness."""
    seen: dict[IdType, int] = {}
    for index, entity in enumerate(entities):
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
