#
# SPDX-License-Identifier: Apache-2.0
#

from concurrent.futures import ThreadPoolExecutor

import pytest
from omnimalloc.allocators import greedy
from omnimalloc.allocators.base import BaseAllocator
from omnimalloc.allocators.greedy import allocate_parallel
from omnimalloc.allocators.omni import OmniAllocator
from omnimalloc.primitives import Allocation

ALLOCATIONS = tuple(Allocation(id=i, size=10 + i, start=i, end=i + 3) for i in range(6))


class TruncatingAllocator(BaseAllocator):
    supports_vector_time = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(a.with_offset(0) for a in allocations[:1])


class PaddingAllocator(BaseAllocator):
    supports_vector_time = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        placed = tuple(a.with_offset(0) for a in allocations)
        return (*placed, placed[0])


def test_allocator_dropping_allocations_is_rejected() -> None:
    with pytest.raises(ValueError, match="returned a different allocation set"):
        TruncatingAllocator().allocate(ALLOCATIONS)


def test_allocator_duplicating_allocations_is_rejected() -> None:
    with pytest.raises(ValueError, match="returned a different allocation set"):
        PaddingAllocator().allocate(ALLOCATIONS)


def test_a_faithful_allocator_is_accepted() -> None:
    placed = OmniAllocator().allocate(ALLOCATIONS)
    assert {a.id for a in placed} == {a.id for a in ALLOCATIONS}


def test_portfolio_drops_a_truncating_variant_instead_of_preferring_it() -> None:
    placed = allocate_parallel(
        ALLOCATIONS, (OmniAllocator(), TruncatingAllocator()), num_threads=1
    )
    assert len(placed) == len(ALLOCATIONS)


def test_portfolio_never_spawns_more_workers_than_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = []

    class Recorder(ThreadPoolExecutor):
        def __init__(self, max_workers: int) -> None:
            seen.append(max_workers)
            super().__init__(max_workers=max_workers)

    monkeypatch.setattr(greedy, "ThreadPoolExecutor", Recorder)
    variants = (OmniAllocator(), OmniAllocator())
    placed = allocate_parallel(ALLOCATIONS, variants, num_threads=32)
    assert seen == [len(variants)]
    assert len(placed) == len(ALLOCATIONS)


def test_supports_counts_pins_like_ensure_supported() -> None:
    pinned = (Allocation(id=1, size=4, start=0, end=2, offset=0),)
    assert OmniAllocator().supports(pinned) is True
    assert TruncatingAllocator().supports(pinned) is False
    with pytest.raises(ValueError, match="cannot honor pinned offsets"):
        TruncatingAllocator().allocate(pinned)
