#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence
from typing import Any, TypeVar

from .allocation import Allocation

T = TypeVar("T")


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


def ensure_items(items: object, item_type: type[T], label: str) -> tuple[T, ...]:
    """Coerce a raw sequence to a tuple, requiring every element be `item_type`."""
    if isinstance(items, str | bytes) or not isinstance(items, Sequence):
        raise TypeError(f"Unsupported {label} type: {type(items)!r}")
    checked: list[T] = []
    for item in items:
        if not isinstance(item, item_type):
            raise TypeError(f"Expected {item_type.__name__}, got {type(item)!r}")
        checked.append(item)
    return tuple(checked)


def ensure_allocations(allocations: object) -> tuple[Allocation, ...]:
    """Coerce a raw sequence to a tuple, requiring every element be an Allocation."""
    return ensure_items(allocations, Allocation, "entity")
