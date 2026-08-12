#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from enum import Enum
from pathlib import Path
from typing import ClassVar

from omnimalloc.common.validation import ensure_positive
from omnimalloc.io import load_allocation
from omnimalloc.primitives import Allocation, IdType, Pool

from .base import BaseSource

logger = logging.getLogger(__name__)


class MinimallocSubset(str, Enum):
    """CSV subsets checked into the repository under ``external/minimalloc``."""

    EXAMPLES = "examples"
    SMALL = "small"
    CHALLENGING = "challenging"

    __str__ = str.__str__


def _checkout_csv_dir(subset: MinimallocSubset) -> Path | None:
    """The subset's directory in a source checkout; None from an install.

    The walk stops at the first project root, so an install cannot silently
    adopt a foreign external/ directory further up the tree.
    """
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            candidate = parent / "external" / "minimalloc" / subset.value
            return candidate if candidate.is_dir() else None
    return None


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
    """Fixed source loading pools from a directory of Minimalloc CSV files.

    `csv_dir` defaults to the `subset`'s directory in a source checkout, which
    an installed package does not have.
    """

    _label_fields: ClassVar[tuple[str, ...]] = ("subset", "csv_dir")

    def __init__(
        self,
        subset: MinimallocSubset | str = MinimallocSubset.CHALLENGING,
        csv_dir: str | Path | None = None,
    ) -> None:
        self.subset = MinimallocSubset(subset)
        # The label must carry an explicit csv_dir but not the checkout default
        self.csv_dir = Path(csv_dir) if csv_dir is not None else None
        self._cached_pools: list[Pool] | None = None

        # An empty dataset keeps the base invariant; the accessors raise instead
        num_allocs = sum(len(p.allocations) for p in self._pools)
        super().__init__(num_allocations=max(num_allocs, 1))

    @property
    def _pools(self) -> list[Pool]:
        if self._cached_pools is None:
            csv_dir = (
                self.csv_dir
                if self.csv_dir is not None
                else _checkout_csv_dir(self.subset)
            )
            # Sort for a filesystem-independent, reproducible variant order
            files = sorted(csv_dir.glob("*.csv")) if csv_dir is not None else []
            if csv_dir is None:
                logger.warning(
                    f"Not running from a source checkout, so the "
                    f"{self.subset.value!r} subset has no datasets; pass "
                    "csv_dir to read them from an install."
                )
            elif not files:
                logger.warning(
                    f"No Minimalloc CSVs found in {csv_dir}; the "
                    f"{self.subset.value!r} subset yields no variants."
                )
            self._cached_pools = [_prefix_ids(load_allocation(f)) for f in files]
        return self._cached_pools

    def _all_allocations(self) -> tuple[Allocation, ...]:
        return tuple(alloc for pool in self._pools for alloc in pool.allocations)

    def is_parameterizable(self) -> bool:
        """Minimalloc has fixed pools, not parameterizable."""
        return False

    def get_available_variants(
        self,
        count: int | None = None,  # noqa: ARG002
    ) -> tuple[str, ...]:
        """Return pool IDs from Minimalloc benchmarks."""
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
        ensure_positive(num_pools, "num_pools", allow_none=True)
        if skip >= len(self._pools):
            return ()
        if num_pools is None:
            return tuple(self._pools[skip:])
        return tuple(self._pools[skip : skip + num_pools])
