#
# SPDX-License-Identifier: Apache-2.0
#


from typing import TypeVar, cast

from .allocators import BaseAllocator, get_default_allocator
from .primitives import Memory, Pool, System
from .validate import validate_allocation

T = TypeVar("T", System, Memory, Pool)


def run_allocation(
    entity: T,
    allocator: BaseAllocator | type[BaseAllocator] | str | None = None,
    validate: bool = False,
) -> T:
    """Run allocation on the given entity using the provided allocator.

    Args:
        entity: The entity to allocate (System, Memory, or Pool).
        allocator: The allocator to use (instance, class, or name).
        validate: Whether to validate the allocated entity.

    Returns:
        The allocated entity with the same type as the input.
    """

    if allocator is None:
        allocator = get_default_allocator()

    if isinstance(allocator, str):
        allocator = BaseAllocator.get(allocator)

    if isinstance(allocator, type):
        allocator = allocator()

    # At this point, allocator is guaranteed to be a BaseAllocator instance
    assert isinstance(allocator, BaseAllocator)

    # ty doesn't understand that TypeVar T (System|Memory|Pool) all have allocate method
    allocated = entity.allocate(allocator)  # type: ignore[invalid-argument-type]

    if validate:
        validate_allocation(allocated)

    return cast("T", allocated)
