#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from enum import Enum

from omnimalloc.common.directories import EXTERNAL_DIR
from omnimalloc.io import load_allocation
from omnimalloc.primitives import Allocation, IdType, Pool

from .base import BaseSource

logger = logging.getLogger(__name__)


class MinimallocSubset(str, Enum):
    """Bundled CSV subsets shipped under ``external/minimalloc/<value>``."""

    EXAMPLES = "examples"
    SMALL = "small"
    CHALLENGING = "challenging"


def _prefix_ids(pool: Pool) -> Pool:
    """CSV ids restart at 0 per file, so qualify them with the pool id."""
    return pool.with_allocations(
        tuple(
            Allocation(
                id=f"{pool.id}_{alloc.id}",
                size=alloc.size,
                start=alloc.start,
                end=alloc.end,
                offset=alloc.offset,
                kind=alloc.kind,
            )
            for alloc in pool.allocations
        )
    )


class MinimallocSource(BaseSource):
    """Fixed source loading pools from a bundled Minimalloc CSV ``subset``."""

    def __init__(
        self,
        subset: MinimallocSubset | str = MinimallocSubset.CHALLENGING,
    ) -> None:
        self.subset = MinimallocSubset(subset)
        self._cached_pools: list[Pool] | None = None

        # Load pools to get actual num_allocations
        pools = self._pools
        num_allocs = sum(len(p.allocations) for p in pools) if pools else 1

        # Initialize with actual num_allocations
        super().__init__(num_allocations=num_allocs)

    @property
    def _pools(self) -> list[Pool]:
        if self._cached_pools is None:
            csv_dir = EXTERNAL_DIR / "minimalloc" / self.subset.value
            # Sort for a filesystem-independent, reproducible variant order
            files = sorted(csv_dir.glob("*.csv")) if csv_dir.is_dir() else []
            if not files:
                logger.warning(
                    f"No Minimalloc CSVs found in {csv_dir}; the "
                    f"{self.subset.value!r} subset yields no variants. The "
                    "datasets are checked into the repository's external/ "
                    "directory and are absent from wheel installs."
                )
            self._cached_pools = [_prefix_ids(load_allocation(f)) for f in files]
        return self._cached_pools

    def _all_allocations(self) -> tuple[Allocation, ...]:
        return tuple(alloc for pool in self._pools for alloc in pool.allocations)

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
