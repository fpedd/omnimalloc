#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.primitives.utils import ensure_unique_ids


def test_ensure_unique_ids_accepts_distinct() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=0, end=4),
    )
    ensure_unique_ids(allocations, "allocation")


def test_ensure_unique_ids_accepts_empty() -> None:
    ensure_unique_ids((), "allocation")


def test_ensure_unique_ids_rejects_duplicates() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=1, size=16, start=2, end=6),
    )
    with pytest.raises(ValueError, match="allocation ids must be unique"):
        ensure_unique_ids(allocations, "allocation")


def test_ensure_unique_ids_reports_kind_id_and_indices() -> None:
    pools = (Pool(id="a", allocations=()), Pool(id="a", allocations=()))
    expected = r"pool ids must be unique: duplicate id 'a' at indices 0 and 1"
    with pytest.raises(ValueError, match=expected):
        ensure_unique_ids(pools, "pool")


def test_ensure_unique_ids_rejects_entities_without_an_id() -> None:
    with pytest.raises(TypeError, match="Expected an entity with an id"):
        ensure_unique_ids(
            (Allocation(id=1, size=8, start=0, end=4), object()), "allocation"
        )


def test_ensure_unique_ids_reports_first_duplicate_of_several() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=0, end=4),
        Allocation(id=1, size=8, start=0, end=4),
    )
    with pytest.raises(ValueError, match=r"duplicate id 2 at indices 1 and 2"):
        ensure_unique_ids(allocations, "allocation")
