#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation


@pytest.mark.parametrize(
    ("allocations", "offsets"),
    [
        ([(100, 0, 10)] * 4, [0, 100, 200, 300]),
        ([(100, 0, 10), (200, 10, 20)], [0, 100]),
        ([(10, 0, 2), (250, 1, 3), (3, 2, 4), (42, 3, 5)], [0, 10, 260, 263]),
    ],
)
def test_naive_stacks_in_input_order_without_reuse(
    allocations: list[tuple[int, int, int]], offsets: list[int]
) -> None:
    allocs = tuple(
        Allocation(id=i, size=size, start=start, end=end)
        for i, (size, start, end) in enumerate(allocations)
    )
    assert [a.offset for a in NaiveAllocator().allocate(allocs)] == offsets
