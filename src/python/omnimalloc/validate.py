#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from .analysis.clock import uniform_dim
from .primitives import Allocation, IdType, Memory, Pool, System
from .primitives.utils import ensure_allocations


def _check_unique_ids(entities: tuple[Memory | Pool | Allocation, ...]) -> None:
    seen: dict[IdType, int] = {}
    for idx, entity in enumerate(entities):
        if entity.id in seen:
            raise ValueError(
                f"duplicate id {entity.id!r} at indices {seen[entity.id]} and {idx}"
            )
        seen[entity.id] = idx


def _check_overlaps(entities: tuple[Pool | Allocation, ...]) -> None:
    if not entities:
        return

    entity_name = str(type(entities[0]).__name__).lower()

    for entity in entities:
        if not entity.is_allocated:
            raise ValueError(f"{entity_name} {entity.id!r} is not allocated")

    for i, entity_a in enumerate(entities):
        for entity_b in entities[i + 1 :]:
            if entity_a.overlaps(entity_b):  # type: ignore[arg-type]
                raise ValueError(
                    f"{entity_name} {entity_a.id!r} overlaps with "
                    f"{entity_name} {entity_b.id!r}"
                )


def _validate_allocations(allocations: tuple[Allocation, ...]) -> None:
    _check_unique_ids(allocations)
    uniform_dim(allocations)
    _check_overlaps(allocations)


def _validate_pools(pools: tuple[Pool, ...]) -> None:
    _check_unique_ids(pools)
    _check_overlaps(pools)
    for pool in pools:
        try:
            _validate_allocations(pool.allocations)
        except ValueError as e:
            raise ValueError(f"in pool {pool.id!r}, {e}") from e


def _check_size(memory: Memory) -> None:
    if memory.size is None or not memory.is_allocated:
        return
    if memory.used_size > memory.size:
        raise ValueError(
            f"used size {memory.used_size} exceeds memory size {memory.size}"
        )


def _validate_memories(memories: tuple[Memory, ...]) -> None:
    _check_unique_ids(memories)
    for memory in memories:
        try:
            _validate_pools(memory.pools)
            _check_size(memory)
        except ValueError as e:
            raise ValueError(f"in memory {memory.id!r}, {e}") from e


def validate_allocation(entity: System | Memory | Pool | Sequence[Allocation]) -> None:
    """Raise ValueError unless the entity is fully placed with no collisions.

    Accepts a System, Memory, or Pool, or a raw sequence of Allocations.
    Checks unique ids, that every allocation and pool carries an offset,
    that no placed rectangles collide, and that each memory's used size
    stays within its declared size.
    """
    if isinstance(entity, System | Memory | Pool):
        described = f"{type(entity).__name__} {entity.id!r}"
    elif isinstance(entity, Sequence) and not isinstance(entity, str | bytes):
        described = f"{len(entity)} allocations"
    else:
        raise TypeError(f"Unsupported entity type: {type(entity)!r}")

    try:
        if isinstance(entity, System):
            _validate_memories(entity.memories)
        elif isinstance(entity, Memory):
            _validate_pools(entity.pools)
            _check_size(entity)
        elif isinstance(entity, Pool):
            _validate_allocations(entity.allocations)
        else:
            _validate_allocations(ensure_allocations(entity))
    except ValueError as e:
        raise ValueError(f"Validation of {described} failed, {e}.") from e
