#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from omnimalloc._cpp import find_collision as _find_collision

from .analysis.clock import uniform_dim
from .primitives import Allocation, IdType, Memory, Pool, System
from .primitives.utils import ensure_allocations, ensure_unique_ids


def _check_ids_across_pools(pools: tuple[Pool, ...]) -> None:
    """Allocation ids key the placement, so they stay unique per address space."""
    owner: dict[IdType, IdType] = {}
    for pool in pools:
        for alloc in pool.allocations:
            if alloc.id in owner:
                raise ValueError(
                    f"duplicate allocation id {alloc.id!r} in pools "
                    f"{owner[alloc.id]!r} and {pool.id!r}"
                )
            owner[alloc.id] = pool.id


def _check_alignment(
    allocations: tuple[Allocation, ...], alignment: int, base: int
) -> None:
    # Allocation offsets are pool-relative, so alignment is a property of
    # `base + offset`: a pool sitting at an unaligned base misaligns every
    # allocation in it, however well-aligned each offset looks on its own.
    for alloc in allocations:
        if alloc.offset is None:
            continue
        address = base + alloc.offset
        if address % alignment != 0:
            raise ValueError(
                f"allocation {alloc.id!r} at address {address} is not "
                f"{alignment}-byte aligned"
            )


def _check_collisions(allocations: tuple[Allocation, ...]) -> None:
    for alloc in allocations:
        if not alloc.is_allocated:
            raise ValueError(f"allocation {alloc.id!r} is not allocated")
    collision = _find_collision(allocations)
    if collision is not None:
        first, second = collision
        raise ValueError(
            f"allocation {allocations[first].id!r} overlaps with "
            f"allocation {allocations[second].id!r}"
        )


def _check_pool_overlaps(pools: tuple[Pool, ...]) -> None:
    for pool in pools:
        if pool.offset is None:
            raise ValueError(f"pool {pool.id!r} is not placed")
    for i, pool_a in enumerate(pools):
        for pool_b in pools[i + 1 :]:
            if pool_a.overlaps(pool_b):
                raise ValueError(f"pool {pool_a.id!r} overlaps with pool {pool_b.id!r}")


def _validate_allocations(
    allocations: tuple[Allocation, ...], alignment: int | None, base: int = 0
) -> None:
    ensure_unique_ids(allocations, "allocation")
    uniform_dim(allocations)
    if alignment is not None:
        _check_alignment(allocations, alignment, base)
    _check_collisions(allocations)


def _validate_pools(pools: tuple[Pool, ...], alignment: int | None) -> None:
    ensure_unique_ids(pools, "pool")
    for pool in pools:
        try:
            _validate_allocations(pool.allocations, alignment, pool.offset or 0)
        except ValueError as e:
            raise ValueError(f"in pool {pool.id!r}, {e}") from e
    _check_ids_across_pools(pools)
    _check_pool_overlaps(pools)


def _check_size(memory: Memory, require_capacity: bool) -> None:
    if memory.size is None:
        if require_capacity:
            raise ValueError("no size declared")
        return
    if memory.extent > memory.size:
        raise ValueError(f"extent {memory.extent} exceeds memory size {memory.size}")


def _validate_memories(
    memories: tuple[Memory, ...], require_capacity: bool, alignment: int | None
) -> None:
    ensure_unique_ids(memories, "memory")
    for memory in memories:
        try:
            _validate_pools(memory.pools, alignment)
            _check_size(memory, require_capacity)
        except ValueError as e:
            raise ValueError(f"in memory {memory.id!r}, {e}") from e


def validate_allocation(
    entity: System | Memory | Pool | Sequence[Allocation],
    require_capacity: bool = False,
    alignment: int | None = None,
) -> None:
    """Raise ValueError unless the entity is fully placed with no collisions.

    Checks unique ids, that everything is placed, that no rectangles collide, and
    that each memory fits its size. `require_capacity`/`alignment` tighten it.
    """
    if alignment is not None and alignment <= 0:
        raise ValueError(f"Alignment must be positive, got {alignment}")

    if isinstance(entity, System | Memory | Pool):
        described = f"{type(entity).__name__} {entity.id!r}"
    elif isinstance(entity, Sequence) and not isinstance(entity, str | bytes):
        described = f"{len(entity)} allocations"
    else:
        raise TypeError(f"Unsupported entity type: {type(entity)!r}")

    try:
        if isinstance(entity, System):
            _validate_memories(entity.memories, require_capacity, alignment)
        elif isinstance(entity, Memory):
            _validate_memories((entity,), require_capacity, alignment)
        elif isinstance(entity, Pool):
            _validate_allocations(entity.allocations, alignment, entity.offset or 0)
        else:
            _validate_allocations(ensure_allocations(entity), alignment)
    except ValueError as e:
        raise ValueError(f"Validation of {described} failed, {e}.") from e
