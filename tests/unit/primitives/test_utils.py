#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.utils import ensure_unique_ids


def test_ensure_unique_ids_accepts_distinct() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=0, end=4),
    )
    ensure_unique_ids(allocations)


def test_ensure_unique_ids_rejects_duplicates() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=1, size=16, start=2, end=6),
    )
    with pytest.raises(ValueError, match="unique"):
        ensure_unique_ids(allocations)
