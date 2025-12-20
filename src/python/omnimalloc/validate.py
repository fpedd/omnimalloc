#
# SPDX-License-Identifier: Apache-2.0
#

from .primitives import Allocation, IdType, Memory, Pool, System


def _check_unique_ids(entities: tuple[Memory | Pool | Allocation, ...]) -> None:
    seen: dict[IdType, int] = {}
    for idx, entity in enumerate(entities):
        if entity.id in seen:
            raise ValueError(f"duplicate id {entity.id!r} at indices")
        seen[entity.id] = idx


def _check_overlaps(
    entities: tuple[Pool | Allocation, ...], require_allocated: bool
) -> None:
    if not entities:
        return

    entity_name = str(type(entities[0]).__name__).lower()

    if require_allocated:
        for entity in entities:
            if not entity.is_allocated:
                raise ValueError(f"{entity_name} {entity.id!r} is not allocated")

    allocated = [e for e in entities if e.is_allocated]
    for i, entity_a in enumerate(allocated):
        for entity_b in allocated[i + 1 :]:
            if entity_a.overlaps(entity_b):  # type: ignore[arg-type]
                raise ValueError(
                    f"{entity_name} {entity_a.id!r} overlaps with "
                    f"{entity_name} {entity_b.id!r}"
                )


def _validate_allocations(
    allocations: tuple[Allocation, ...], require_allocated: bool
) -> None:
    _check_unique_ids(allocations)
    _check_overlaps(allocations, require_allocated)


def _validate_pools(pools: tuple[Pool, ...], require_allocated: bool) -> None:
    _check_unique_ids(pools)
    _check_overlaps(pools, require_allocated)
    for pool in pools:
        try:
            _validate_allocations(pool.allocations, require_allocated)
        except ValueError as e:
            raise ValueError(f"in pool {pool.id!r}, {e}") from e


def _validate_memories(memories: tuple[Memory, ...], require_allocated: bool) -> None:
    _check_unique_ids(memories)
    for memory in memories:
        try:
            _validate_pools(memory.pools, require_allocated)
        except ValueError as e:
            raise ValueError(f"in memory {memory.id!r}, {e}") from e


def validate_allocation(
    entity: System | Memory | Pool,
    raise_on_error: bool = True,
    require_allocated: bool = True,
) -> bool:
    """Validate the given allocated entity (System, Memory, or Pool).

    Args:
        entity: The entity to validate.
        raise_on_error: If True, raise ValueError on validation failure.
        require_allocated: If True, require all allocations to be allocated.

    Returns:
        True if valid, False otherwise.
    """
    try:
        if isinstance(entity, System):
            _validate_memories(entity.memories, require_allocated)
        elif isinstance(entity, Memory):
            _validate_pools(entity.pools, require_allocated)
        elif isinstance(entity, Pool):
            _validate_allocations(entity.allocations, require_allocated)
        else:
            raise TypeError(f"Unsupported entity type: {type(entity)!r}")
    except ValueError as e:
        if raise_on_error:
            raise ValueError(
                f"Validation of {type(entity).__name__} {entity.id!r} failed, {e}."
            ) from e
        return False

    return True
