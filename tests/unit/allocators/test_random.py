#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.random import RandomAllocator
from omnimalloc.analysis import placement_pressure
from omnimalloc.primitives import Allocation

ALLOCATIONS = tuple(
    Allocation(id=i, size=(i % 5 + 1) * 10, start=i % 3, end=i % 3 + i % 4 + 1)
    for i in range(30)
)


def test_random_rejects_zero_trials() -> None:
    with pytest.raises(ValueError, match="num_trials must be positive"):
        RandomAllocator(num_trials=0)


def test_random_more_trials_never_worse_for_same_seed() -> None:
    few = RandomAllocator(num_trials=5, seed=3).allocate(ALLOCATIONS)
    many = RandomAllocator(num_trials=50, seed=3).allocate(ALLOCATIONS)
    assert placement_pressure(many) <= placement_pressure(few)


def test_random_timeout_bounds_the_trials() -> None:
    allocator = RandomAllocator(num_trials=10**9, timeout=0.05)
    assert len(allocator.allocate(ALLOCATIONS)) == len(ALLOCATIONS)
