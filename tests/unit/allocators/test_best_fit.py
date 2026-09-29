#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocators.best_fit import BestFitAllocator
from omnimalloc.allocators.greedy import GreedyAllocator
from omnimalloc.primitives import Allocation


def test_best_fit_takes_the_tighter_gap_that_first_fit_passes_over() -> None:
    allocations = (
        Allocation(id="a", size=10, start=0, end=100),
        Allocation(id="filler1", size=50, start=1, end=2),
        Allocation(id="b", size=10, start=1, end=55),
        Allocation(id="filler2", size=25, start=3, end=4),
        Allocation(id="c", size=10, start=3, end=55),
        Allocation(id="t", size=15, start=50, end=55),
    )
    assert GreedyAllocator().allocate(allocations)[-1].offset == 10
    assert BestFitAllocator().allocate(allocations)[-1].offset == 45
