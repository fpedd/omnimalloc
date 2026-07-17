#
# SPDX-License-Identifier: Apache-2.0
#

from .analysis.clock import uniform_dim
from .primitives import Allocation, IdType, Memory, Pool, System


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


def _check_capacity(memory: Memory) -> None:
    if memory.capacity is None or not memory.is_allocated:
        return
    if memory.used_size > memory.capacity:
        raise ValueError(
            f"used size {memory.used_size} exceeds capacity {memory.capacity}"
        )


def _validate_memories(memories: tuple[Memory, ...]) -> None:
    _check_unique_ids(memories)
    for memory in memories:
        try:
            _validate_pools(memory.pools)
            _check_capacity(memory)
        except ValueError as e:
            raise ValueError(f"in memory {memory.id!r}, {e}") from e


def validate_allocation(entity: System | Memory | Pool) -> None:
    """Raise ValueError unless the entity is fully placed with no collisions.

    Checks unique ids, that every allocation and pool carries an offset,
    that no placed rectangles collide, and that each memory's used size
    stays within its declared capacity.
    """
    try:
        if isinstance(entity, System):
            _validate_memories(entity.memories)
        elif isinstance(entity, Memory):
            _validate_pools(entity.pools)
            _check_capacity(entity)
        elif isinstance(entity, Pool):
            _validate_allocations(entity.allocations)
        else:
            raise TypeError(f"Unsupported entity type: {type(entity)!r}")
    except ValueError as e:
        raise ValueError(
            f"Validation of {type(entity).__name__} {entity.id!r} failed, {e}."
        ) from e
