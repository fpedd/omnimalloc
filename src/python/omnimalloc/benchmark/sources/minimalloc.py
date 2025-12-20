#
# SPDX-License-Identifier: Apache-2.0
#

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

from omnimalloc.common.directories import EXTERNAL_DIR
from omnimalloc.primitives import Allocation, BufferKind, IdType, Pool

from .base import BaseSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MinimallocBuffer:
    id: IdType
    lower: int
    upper: int
    size: int

    def __post_init__(self) -> None:
        if isinstance(self.id, int) and self.id < 0:
            raise ValueError(f"id must be non-negative, got {self.id}")
        if self.size <= 0:
            raise ValueError(f"size must be positive, got {self.size}")
        if self.upper <= self.lower:
            raise ValueError(f"upper ({self.upper}) < lower ({self.lower})")


def _read_minimalloc_csv(file_path: Path) -> list[_MinimallocBuffer]:
    buffers = []
    with Path.open(file_path, mode="r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            buffer = _MinimallocBuffer(
                id=str(row["id"]),
                lower=int(row["lower"]),
                upper=int(row["upper"]),
                size=int(row["size"]),
            )
            buffers.append(buffer)
    return buffers


def _from_minimalloc_csv(file_path: str | Path) -> Pool:
    file_path_ = Path(file_path)
    mm_buffers = _read_minimalloc_csv(file_path_)
    allocations = []
    for mm_buffer in mm_buffers:
        allocation = Allocation(
            id=mm_buffer.id,
            size=mm_buffer.size,
            start=mm_buffer.lower,
            end=mm_buffer.upper,
            offset=None,
            kind=BufferKind.WORKSPACE,
        )
        allocations.append(allocation)

    pool = Pool(
        id=file_path_.stem,
        allocations=tuple(allocations),
        offset=None,
    )

    return pool


def _get_minimalloc_pools() -> list[Pool]:
    csv_dir = EXTERNAL_DIR / "minimalloc" / "challenging"
    csv_files = list(csv_dir.glob("*.csv"))
    pools = [_from_minimalloc_csv(file) for file in csv_files]
    return pools


class MinimallocSource(BaseSource):
    """Load allocations from Minimalloc CSV format.

    This is a fixed source with predetermined pools from Minimalloc benchmarks.
    Can be initialized with either a specific CSV file or a directory of CSVs.
    """

    def __init__(self, file_path: str | Path | None = None) -> None:
        self.file_path = Path(file_path) if file_path is not None else None
        self._cached_pools: list[Pool] | None = None

        # Validate path exists if provided
        if self.file_path is not None and not self.file_path.exists():
            msg = f"Path does not exist: {self.file_path}"
            raise FileNotFoundError(msg)

        # Load pools to get actual num_allocations
        pools = self._pools
        num_allocs = sum(len(p.allocations) for p in pools) if pools else 1

        # Initialize with actual num_allocations
        super().__init__(num_allocations=num_allocs)

    @property
    def _pools(self) -> list[Pool]:
        if self._cached_pools is None:
            if self.file_path is None:
                self._cached_pools = _get_minimalloc_pools()
            elif self.file_path.is_file():
                self._cached_pools = [_from_minimalloc_csv(self.file_path)]
            elif self.file_path.is_dir():
                csv_files = list(self.file_path.glob("*.csv"))
                self._cached_pools = [_from_minimalloc_csv(f) for f in csv_files]
            else:
                msg = f"Path does not exist: {self.file_path}"
                raise FileNotFoundError(msg)
        return self._cached_pools

    def _all_allocations(self) -> tuple[Allocation, ...]:
        all_allocations: list[Allocation] = []
        for pool in self._pools:
            all_allocations.extend(pool.allocations)
        return tuple(all_allocations)

    def is_parameterizable(self) -> bool:
        """Minimalloc has fixed pools, not parameterizable."""
        return False

    def get_available_variants(self, variants: int | None = None) -> tuple[str, ...]:
        """Return pool IDs from Minimalloc benchmarks."""
        if variants is not None:
            logger.debug(f"Ignoring variants={variants}")
        return tuple(str(pool.id) for pool in self._pools)

    def get_variant(self, variant_id: IdType) -> Pool:
        """Get a specific Minimalloc pool by name."""
        if isinstance(variant_id, int):
            # Support integer indexing
            if 0 <= variant_id < len(self._pools):
                return self._pools[variant_id]
            msg = f"Pool index {variant_id} out of range [0, {len(self._pools)})"
            raise ValueError(msg)

        # String lookup by pool ID
        for pool in self._pools:
            if pool.id == variant_id:
                return pool

        raise ValueError(f"Pool with ID '{variant_id}' not found in Minimalloc source")

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        all_allocations = self._all_allocations()
        if skip >= len(all_allocations):
            return ()
        if num_allocations is None:
            return all_allocations[skip:]
        return all_allocations[skip : skip + num_allocations]

    def get_pools(
        self, num_pools: int | None = None, skip: int = 0
    ) -> tuple[Pool, ...]:
        if skip >= len(self._pools):
            return ()
        if num_pools is None:
            return tuple(self._pools[skip:])
        return tuple(self._pools[skip : skip + num_pools])
