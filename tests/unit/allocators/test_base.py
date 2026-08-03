#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators import greedy_base
from omnimalloc.allocators.base import BaseAllocator
from omnimalloc.allocators.greedy_base import allocate_parallel
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

    class Recorder:
        def __init__(self, max_workers: int) -> None:
            seen.append(max_workers)
            raise RuntimeError("Stop before spawning workers")

    monkeypatch.setattr(greedy_base, "ProcessPoolExecutor", Recorder)
    variants = (OmniAllocator(), OmniAllocator())
    with pytest.raises(RuntimeError):
        allocate_parallel(ALLOCATIONS, variants, num_threads=32)
    assert seen == [len(variants)]
