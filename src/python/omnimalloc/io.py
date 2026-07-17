#
# SPDX-License-Identifier: Apache-2.0
#

import csv
from pathlib import Path

from .analysis.clock import time_components
from .primitives import Allocation, Memory, Pool, System, TimePoint


def _format_time(time_point: TimePoint) -> str:
    return ":".join(str(component) for component in time_components(time_point))


def _parse_time(text: str) -> TimePoint:
    if ":" in text:
        return tuple(int(component) for component in text.split(":"))
    return int(text)


def _collect_pools(entity: Memory | System) -> dict[str, Pool]:
    if isinstance(entity, Memory):
        pools = {str(pool.id): pool for pool in entity.pools}
        if len(pools) != len(entity.pools):
            raise ValueError("pool ids must be unique after string conversion")
        return pools
    pools = {
        f"{memory.id}_{pool.id}": pool
        for memory in entity.memories
        for pool in memory.pools
    }
    if len(pools) != sum(len(memory.pools) for memory in entity.memories):
        raise ValueError("memory/pool id combinations must be unique")
    return pools


def _write_pool(pool: Pool, path: Path) -> Path:
    # Any placed allocation brings in the offset column (minimalloc's
    # solution format; unplaced rows leave the cell blank), so save/load
    # round-trips placements instead of silently stripping them.
    with_offsets = any(alloc.is_allocated for alloc in pool.allocations)
    fields = ("id", "lower", "upper", "size")
    if with_offsets:
        fields = (*fields, "offset")
    with path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fields)
        for alloc in pool.allocations:
            row = [alloc.id, _format_time(alloc.start), _format_time(alloc.end)]
            row.append(alloc.size)
            if with_offsets:
                row.append(alloc.offset)
            writer.writerow(row)
    return path


def save_allocation(
    entity: System | Memory | Pool, path: str | Path
) -> tuple[Path, ...]:
    """Save the entity's pools to disk as minimalloc-format CSV files.

    Saving a `Pool` writes exactly `path`; a `Memory` or `System` fans out
    to one `<stem>_<pool_id>.csv` per pool (the CSV is pool-level minimalloc
    interchange). Returns the tuple of paths actually written. Pools with
    any placed allocation include an `offset` column (minimalloc's solution
    format; unplaced rows leave the cell blank), so `load_allocation`
    round-trips placements. Vector-clock lifetimes use an
    omnimalloc extension (``:``-joined components, e.g. ``3:0``); such files
    round-trip through `load_allocation` but are no longer
    minimalloc-readable.
    """
    path_ = Path(path)
    path_.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(entity, Pool):
        return (_write_pool(entity, path_),)
    if not isinstance(entity, (Memory, System)):
        raise TypeError(f"Unsupported entity type: {type(entity)!r}")
    return tuple(
        _write_pool(pool, path_.with_name(f"{path_.stem}_{name}.csv"))
        for name, pool in _collect_pools(entity).items()
    )


def load_allocation(path: str | Path) -> Pool:
    """Load a minimalloc-format CSV file into a Pool.

    Loading is pool-level, matching the format's granularity; an `offset`
    column (minimalloc's solution format) restores placements. The format
    carries no allocation kind, so loaded allocations keep `kind=None`.
    """
    path_ = Path(path)
    allocations = []
    with path_.open(newline="") as csvfile:
        for row in csv.DictReader(csvfile):
            allocation = Allocation(
                id=str(row["id"]),
                size=int(row["size"]),
                start=_parse_time(row["lower"]),
                end=_parse_time(row["upper"]),
                offset=int(row["offset"]) if row.get("offset") else None,
            )
            allocations.append(allocation)
    return Pool(id=path_.stem, allocations=tuple(allocations))
