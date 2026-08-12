#
# SPDX-License-Identifier: Apache-2.0
#

import csv
import re
from collections.abc import Sequence
from pathlib import Path

from .analysis._clock import time_components
from .primitives import (
    Allocation,
    AllocationKind,
    IdType,
    Memory,
    Pool,
    System,
    TimePoint,
)

_INTEGER = re.compile(r"-?\d+")


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


def _parse_id(text: str) -> IdType:
    # Integer ids are the common case (every shipped source uses them) and CSV
    # cannot tell 1 from "1", so canonical digits read back as the int they
    # were; non-canonical ones like "007" stay strings to keep their identity.
    if _INTEGER.fullmatch(text) and str(int(text)) == text:
        return int(text)
    return text


def _write_pool(pool: Pool, path: Path) -> Path:
    # Any placed allocation brings in the offset column (minimalloc's solution
    # format; unplaced rows leave the cell blank), so save/load round-trips
    # placements instead of stripping them. A kind column appears the same way.
    with_offsets = pool.any_allocated
    with_kinds = any(alloc.kind is not None for alloc in pool.allocations)
    fields = ("id", "lower", "upper", "size")
    if with_offsets:
        fields = (*fields, "offset")
    if with_kinds:
        fields = (*fields, "kind")
    with path.open("w", newline="") as csvfile:
        if pool.offset is not None:
            # Deviates from plain minimalloc: only unpinned saves interoperate
            # with foreign readers; our loader restores the base exactly
            csvfile.write(f"# pool_offset={pool.offset}\r\n")
        writer = csv.writer(csvfile)
        writer.writerow(fields)
        for alloc in pool.allocations:
            row = [alloc.id, _format_time(alloc.start), _format_time(alloc.end)]
            row.append(alloc.size)
            if with_offsets:
                row.append(alloc.offset)
            if with_kinds:
                row.append(alloc.kind.name if alloc.kind is not None else None)
            writer.writerow(row)
    return path


def save_allocation(
    entity: System | Memory | Pool | Sequence[Allocation], path: str | Path
) -> tuple[Path, ...]:
    """Save the entity's pools to disk as minimalloc-format CSV files.

    A `Pool` writes exactly `path`; a `Memory` or `System` fans out per pool.
    Offsets, kinds, clocks, and a pinned pool base all round-trip. Unpinned
    saves are pure minimalloc; the base's `# pool_offset` line deviates.
    """
    path_ = Path(path)
    path_.parent.mkdir(parents=True, exist_ok=True)
    if not isinstance(entity, System | Memory | Pool):
        entity = Pool.from_allocations(entity)
    if isinstance(entity, Pool):
        return (_write_pool(entity, path_),)
    return tuple(
        _write_pool(pool, path_.with_name(f"{path_.stem}_{name}.csv"))
        for name, pool in _collect_pools(entity).items()
    )


def load_allocation(path: str | Path) -> Pool:
    """Load a minimalloc-format CSV file into a Pool.

    Loading is pool-level: the pool takes the file stem as its id. `offset` and
    `kind` columns and the `# pool_offset` base line restore a round-trip-equal
    result.
    """
    path_ = Path(path)
    allocations = []
    pool_offset = None
    with path_.open(newline="") as csvfile:
        first_line = csvfile.readline()
        if first_line.startswith("# pool_offset="):
            pool_offset = int(first_line.removeprefix("# pool_offset="))
        else:
            csvfile.seek(0)
        for row in csv.DictReader(csvfile):
            allocation = Allocation(
                id=_parse_id(row["id"]),
                size=int(row["size"]),
                start=_parse_time(row["lower"]),
                end=_parse_time(row["upper"]),
                offset=int(row["offset"]) if row.get("offset") else None,
                kind=AllocationKind[row["kind"]] if row.get("kind") else None,
            )
            allocations.append(allocation)
    return Pool(id=path_.stem, allocations=tuple(allocations), offset=pool_offset)
