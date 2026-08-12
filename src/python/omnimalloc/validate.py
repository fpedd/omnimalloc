#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from omnimalloc._cpp import find_collision as _find_collision

from .analysis._clock import uniform_dim
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


def _check_collisions(
    allocations: tuple[Allocation, ...], require_allocated: bool
) -> None:
    if require_allocated:
        for alloc in allocations:
            if not alloc.is_allocated:
                raise ValueError(f"allocation {alloc.id!r} is not allocated")
    placed = tuple(alloc for alloc in allocations if alloc.is_allocated)
    collision = _find_collision(placed)
    if collision is not None:
        first, second = collision
        raise ValueError(
            f"allocation {placed[first].id!r} overlaps with "
            f"allocation {placed[second].id!r}"
        )


def _placed_top(pool: Pool) -> int:
    """Highest placed address in the pool, 0 while nothing is placed."""
    heights = [alloc.height for alloc in pool.allocations if alloc.height is not None]
    return max(heights, default=0)


def _placed_extent(memory: Memory) -> int:
    """Highest address any placed pool's placed allocations reach."""
    tops = [
        pool.offset + _placed_top(pool)
        for pool in memory.pools
        if pool.offset is not None
    ]
    return max(tops, default=0)


def _alignment_base(offset: int | None, require_allocated: bool) -> int | None:
    # Pool-relative alignment treats a missing base as 0; the loosened mode
    # cannot, since placement will pick the real base later, so None skips.
    if offset is not None:
        return offset
    return 0 if require_allocated else None


def _check_pool_overlaps(pools: tuple[Pool, ...], require_allocated: bool) -> None:
    if require_allocated:
        for pool in pools:
            if pool.offset is None:
                raise ValueError(f"pool {pool.id!r} is not placed")
    # Pool.overlaps reads the full extent, which partially placed pools cannot
    # provide, so compare once-precomputed placed tops; fully placed agree.
    placed = [
        (pool, pool.offset, _placed_top(pool))
        for pool in pools
        if pool.offset is not None
    ]
    for i, (pool_a, base_a, top_a) in enumerate(placed):
        for pool_b, base_b, top_b in placed[i + 1 :]:
            if base_a < base_b + top_b and base_b < base_a + top_a:
                raise ValueError(f"pool {pool_a.id!r} overlaps with pool {pool_b.id!r}")


def _validate_allocations(
    allocations: tuple[Allocation, ...],
    require_allocated: bool,
    alignment: int | None,
    base: int | None = 0,
) -> None:
    ensure_unique_ids(allocations, "allocation")
    uniform_dim(allocations)
    if alignment is not None and base is not None:
        _check_alignment(allocations, alignment, base)
    _check_collisions(allocations, require_allocated)


def _validate_pools(
    pools: tuple[Pool, ...], require_allocated: bool, alignment: int | None
) -> None:
    ensure_unique_ids(pools, "pool")
    for pool in pools:
        try:
            _validate_allocations(
                pool.allocations,
                require_allocated,
                alignment,
                _alignment_base(pool.offset, require_allocated),
            )
        except ValueError as e:
            raise ValueError(f"in pool {pool.id!r}, {e}") from e
    _check_ids_across_pools(pools)
    _check_pool_overlaps(pools, require_allocated)


def _check_size(
    memory: Memory, require_capacity: bool, require_allocated: bool
) -> None:
    if memory.size is None:
        if require_capacity:
            raise ValueError("no size declared")
        return
    # Runs after _validate_pools: in strict mode everything is placed by then,
    # so the canonical cached extent is safe; the loosened arm reads what is
    # actually placed.
    extent = memory.extent if require_allocated else _placed_extent(memory)
    if extent > memory.size:
        raise ValueError(f"extent {extent} exceeds memory size {memory.size}")


def _validate_memories(
    memories: tuple[Memory, ...],
    require_allocated: bool,
    require_capacity: bool,
    alignment: int | None,
) -> None:
    ensure_unique_ids(memories, "memory")
    for memory in memories:
        try:
            _validate_pools(memory.pools, require_allocated, alignment)
            _check_size(memory, require_capacity, require_allocated)
        except ValueError as e:
            raise ValueError(f"in memory {memory.id!r}, {e}") from e


def validate_allocation(
    entity: System | Memory | Pool | Sequence[Allocation],
    require_allocated: bool = True,
    require_capacity: bool = False,
    alignment: int | None = None,
) -> None:
    """Raise ValueError unless the entity is fully placed with no collisions.

    Checks unique ids, placement completeness, collisions, and memory capacity.
    `require_allocated=False` drops completeness and checks the placed subset
    only, so pins and partial placements validate before an allocator runs.
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
            _validate_memories(
                entity.memories, require_allocated, require_capacity, alignment
            )
        elif isinstance(entity, Memory):
            _validate_memories(
                (entity,), require_allocated, require_capacity, alignment
            )
        elif isinstance(entity, Pool):
            _validate_allocations(
                entity.allocations,
                require_allocated,
                alignment,
                _alignment_base(entity.offset, require_allocated),
            )
        else:
            _validate_allocations(
                ensure_allocations(entity), require_allocated, alignment
            )
    except ValueError as e:
        raise ValueError(f"Validation of {described} failed, {e}.") from e
