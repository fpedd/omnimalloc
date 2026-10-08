#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from omnimalloc._cpp import find_collision as _find_collision

from .analysis._clock import uniform_dim
from .common.intervals import first_overlap
from .common.validation import ensure_positive
from .primitives import Allocation, IdType, Memory, Pool, System
from .primitives.utils import ensure_allocations, ensure_unique_ids


def _check_alignment(
    allocations: tuple[Allocation, ...], alignment: int, base: int
) -> None:
    # Offsets are pool-relative, so alignment is a property of `base + offset`
    for alloc in allocations:
        if alloc.offset is not None and (base + alloc.offset) % alignment != 0:
            raise ValueError(
                f"allocation {alloc.id!r} at address {base + alloc.offset} is not "
                f"{alignment}-byte aligned"
            )


def _check_collisions(
    allocations: tuple[Allocation, ...], require_allocated: bool
) -> None:
    placed = tuple(alloc for alloc in allocations if alloc.is_allocated)
    if require_allocated and len(placed) != len(allocations):
        unplaced = next(alloc for alloc in allocations if not alloc.is_allocated)
        raise ValueError(f"allocation {unplaced.id!r} is not allocated")
    collision = _find_collision(placed)
    if collision is not None:
        first, second = collision
        raise ValueError(
            f"allocation {placed[first].id!r} overlaps with "
            f"allocation {placed[second].id!r}"
        )


def _validate_allocations(
    allocations: tuple[Allocation, ...],
    require_allocated: bool,
    alignment: int | None,
    base: int | None,
) -> None:
    uniform_dim(allocations)
    if alignment is not None and base is not None:
        _check_alignment(allocations, alignment, base)
    _check_collisions(allocations, require_allocated)


def _validate_pool(pool: Pool, require_allocated: bool, alignment: int | None) -> None:
    # A missing base aligns as 0 in strict mode; the loosened mode skips it,
    # since placement picks the real base later
    base = 0 if pool.offset is None and require_allocated else pool.offset
    _validate_allocations(pool.allocations, require_allocated, alignment, base)


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


def _placed_top(pool: Pool) -> int:
    """Highest pool-relative address its placed allocations reach."""
    return max((alloc.height or 0 for alloc in pool.allocations), default=0)


def _validate_memory(
    memory: Memory,
    require_allocated: bool,
    require_capacity: bool,
    alignment: int | None,
) -> None:
    for pool in memory.pools:
        try:
            _validate_pool(pool, require_allocated, alignment)
        except ValueError as e:
            raise ValueError(f"in pool {pool.id!r}, {e}") from e
    _check_ids_across_pools(memory.pools)

    placed = [pool for pool in memory.pools if pool.offset is not None]
    if require_allocated and len(placed) != len(memory.pools):
        unplaced = next(pool for pool in memory.pools if pool.offset is None)
        raise ValueError(f"pool {unplaced.id!r} is not placed")
    # Pools span only their placed allocations, so partial placements compare
    spans = [
        (pool.offset, pool.offset + _placed_top(pool))
        for pool in memory.pools
        if pool.offset is not None
    ]
    overlap = first_overlap(spans)
    if overlap is not None:
        first, second = overlap
        raise ValueError(
            f"pool {placed[first].id!r} overlaps with pool {placed[second].id!r}"
        )

    if memory.size is None:
        if require_capacity:
            raise ValueError("no size declared")
        return
    extent = max((top for _, top in spans), default=0)
    if extent > memory.size:
        raise ValueError(f"extent {extent} exceeds memory size {memory.size}")


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
    ensure_positive(alignment, "alignment", allow_none=True)
    if isinstance(entity, System | Memory | Pool):
        described = f"{type(entity).__name__} {entity.id!r}"
    else:
        entity = ensure_allocations(entity)
        described = f"{len(entity)} allocations"
    try:
        if isinstance(entity, System):
            for memory in entity.memories:
                try:
                    _validate_memory(
                        memory, require_allocated, require_capacity, alignment
                    )
                except ValueError as e:
                    raise ValueError(f"in memory {memory.id!r}, {e}") from e
        elif isinstance(entity, Memory):
            _validate_memory(entity, require_allocated, require_capacity, alignment)
        elif isinstance(entity, Pool):
            _validate_pool(entity, require_allocated, alignment)
        else:
            ensure_unique_ids(entity, "allocation")
            _validate_allocations(entity, require_allocated, alignment, 0)
    except ValueError as e:
        raise ValueError(f"Validation of {described} failed, {e}.") from e
