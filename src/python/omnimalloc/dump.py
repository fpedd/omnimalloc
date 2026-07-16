#
# SPDX-License-Identifier: Apache-2.0
#

import csv
from pathlib import Path

from .analysis.clock import time_components
from .primitives import Allocation, BufferKind, Memory, Pool, System, TimePoint

_FIELDS = ("id", "lower", "upper", "size")


def _format_time(time_point: TimePoint) -> str:
    return ":".join(str(component) for component in time_components(time_point))


def _parse_time(text: str) -> TimePoint:
    if ":" in text:
        return tuple(int(component) for component in text.split(":"))
    return int(text)


def _collect_pools(entity: System | Memory | Pool) -> dict[str, Pool]:
    if isinstance(entity, Pool):
        return {str(entity.id): entity}
    if isinstance(entity, Memory):
        pools = {str(pool.id): pool for pool in entity.pools}
        if len(pools) != len(entity.pools):
            raise ValueError("pool ids must be unique after string conversion")
        return pools
    if isinstance(entity, System):
        pools = {
            f"{memory.id}_{pool.id}": pool
            for memory in entity.memories
            for pool in memory.pools
        }
        if len(pools) != sum(len(memory.pools) for memory in entity.memories):
            raise ValueError("memory/pool id combinations must be unique")
        return pools
    raise TypeError(f"Unsupported entity type: {type(entity)!r}")


def _write_pool(pool: Pool, file_path: Path) -> Path:
    with file_path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(_FIELDS)
        for alloc in pool.allocations:
            lower, upper = _format_time(alloc.start), _format_time(alloc.end)
            writer.writerow([alloc.id, lower, upper, alloc.size])
    return file_path


def dump_allocation(
    entity: System | Memory | Pool, path: str | Path
) -> tuple[Path, ...]:
    """Dump the entity's pools to disk as minimalloc-format CSV files.

    Path's stem is used as prefix, ie. ``<stem>_<pool_id>.csv`` per pool.
    Vector-clock lifetimes use an omnimalloc extension (``:``-joined
    components, e.g. ``3:0``); such files round-trip through
    ``load_allocation`` but are no longer minimalloc-readable.
    """
    path_ = Path(path)
    path_.parent.mkdir(parents=True, exist_ok=True)
    return tuple(
        _write_pool(pool, path_.with_name(f"{path_.stem}_{name}.csv"))
        for name, pool in _collect_pools(entity).items()
    )


def load_allocation(file_path: str | Path) -> Pool:
    """Load a minimalloc-format CSV file into a Pool."""
    file_path_ = Path(file_path)
    allocations = []
    with file_path_.open(newline="") as csvfile:
        for row in csv.DictReader(csvfile):
            allocation = Allocation(
                id=str(row["id"]),
                size=int(row["size"]),
                start=_parse_time(row["lower"]),
                end=_parse_time(row["upper"]),
                offset=int(row["offset"]) if row.get("offset") else None,
                kind=BufferKind.WORKSPACE,
            )
            allocations.append(allocation)
    return Pool(id=file_path_.stem, allocations=tuple(allocations))
