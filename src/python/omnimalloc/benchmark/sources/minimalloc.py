#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from enum import Enum
from pathlib import Path
from typing import ClassVar

from omnimalloc.io import load_allocation
from omnimalloc.primitives import Pool

from .base import FixedSource, prefix_ids

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


class MinimallocSource(FixedSource):
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
        super().__init__()
        self.subset = MinimallocSubset(subset)
        # The label must carry an explicit csv_dir but not the checkout default
        self.csv_dir = Path(csv_dir) if csv_dir is not None else None

    def _load_pools(self) -> tuple[Pool, ...]:
        csv_dir = self.csv_dir or _checkout_csv_dir(self.subset)
        if csv_dir is None:
            logger.warning(
                f"Not running from a source checkout, so the "
                f"{self.subset.value!r} subset has no datasets; pass "
                "csv_dir to read them from an install."
            )
            return ()
        # Sort for a filesystem-independent, reproducible variant order
        files = sorted(csv_dir.glob("*.csv"))
        if not files:
            logger.warning(
                f"No Minimalloc CSVs found in {csv_dir}; the "
                f"{self.subset.value!r} subset yields no variants."
            )
        # CSV ids restart at 0 per file
        return tuple(prefix_ids(load_allocation(f)) for f in files)
