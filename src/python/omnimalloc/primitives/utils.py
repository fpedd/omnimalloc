#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence
from typing import Any, TypeVar, cast

from .allocation import Allocation

T = TypeVar("T")


def ensure_unique_ids(entities: Sequence[Any], kind: str) -> None:
    """Raise if any id repeats; id-keyed placement assumes uniqueness."""
    if len({entity.id for entity in entities}) == len(entities):
        return
    seen: dict[Any, int] = {}
    for index, entity in enumerate(entities):
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
    checked = tuple(items)
    for item in checked:
        if not isinstance(item, item_type):
            raise TypeError(f"Expected {item_type.__name__}, got {type(item)!r}")
    return cast("tuple[T, ...]", checked)


def ensure_allocations(allocations: object) -> tuple[Allocation, ...]:
    """Coerce a raw sequence to a tuple, requiring every element be an Allocation."""
    return ensure_items(allocations, Allocation, "entity")
