#
# SPDX-License-Identifier: Apache-2.0
#


from typing import TypeVar, cast

from .allocators import DEFAULT_ALLOCATOR, BaseAllocator
from .primitives import Memory, Pool, System
from .validate import validate_allocation

T = TypeVar("T", System, Memory, Pool)


def allocate(
    entity: T,
    allocator: BaseAllocator | type[BaseAllocator] | str | None = None,
    *,
    validate: bool = False,
) -> T:
    """Return the entity (System, Memory, or Pool) with offsets assigned.

    `allocator` accepts an instance, a class, a registry name, or `None`
    (the default allocator); `validate=True` additionally runs
    `validate_allocation` on the result.
    """

    if allocator is None:
        allocator = DEFAULT_ALLOCATOR

    resolved = BaseAllocator.resolve(allocator)

    # ty doesn't understand that TypeVar T (System|Memory|Pool) all have allocate method
    allocated = entity.allocate(resolved)  # type: ignore[invalid-argument-type]

    if validate:
        validate_allocation(allocated)

    return cast("T", allocated)
