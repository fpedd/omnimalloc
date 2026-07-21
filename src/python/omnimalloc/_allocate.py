#
# SPDX-License-Identifier: Apache-2.0
#


from collections.abc import Sequence
from typing import TypeAlias, TypeVar, cast, overload

from .allocators import DEFAULT_ALLOCATOR, BaseAllocator
from .primitives import Allocation, Memory, Pool, System
from .validate import validate_allocation

AllocatorLike: TypeAlias = BaseAllocator | type[BaseAllocator] | str | None

T = TypeVar("T", System, Memory, Pool)


@overload
def allocate(
    entity: T,
    allocator: AllocatorLike = None,
    validate: bool = False,
) -> T: ...


@overload
def allocate(
    entity: Sequence[Allocation],
    allocator: AllocatorLike = None,
    validate: bool = False,
) -> tuple[Allocation, ...]: ...


def allocate(
    entity: System | Memory | Pool | Sequence[Allocation],
    allocator: AllocatorLike = None,
    validate: bool = False,
) -> System | Memory | Pool | tuple[Allocation, ...]:
    """Return the entity with offsets assigned.

    Accepts a System, Memory, or Pool (returned as the same type) or a raw
    sequence of Allocations (returned as a tuple in input order). `allocator`
    accepts an instance, a class, a registry name, or `None` (the default
    allocator); `validate=True` additionally runs `validate_allocation` on
    the result.
    """

    if allocator is None:
        allocator = DEFAULT_ALLOCATOR

    resolved = cast("BaseAllocator", BaseAllocator.resolve(allocator))

    if isinstance(entity, System | Memory | Pool):
        allocated = entity.allocate(resolved)
    else:
        allocated = Pool.from_allocations(entity).allocate(resolved).allocations

    if validate:
        validate_allocation(allocated)

    return allocated
