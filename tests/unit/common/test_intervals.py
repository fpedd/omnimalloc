#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.common.intervals import first_overlap, stack_around_pins


@pytest.mark.parametrize(
    ("sizes", "pins", "expected"),
    [
        ([], [], []),
        ([4, 8, 16], [None, None, None], [0, 4, 12]),
        ([4, 8], [64, None], [64, 0]),
        ([8, 4, 4], [16, None, None], [16, 0, 4]),
        ([8, 16], [4, None], [4, 12]),
        ([4, 4, 2, 2, 8], [0, 12, None, None, None], [0, 12, 4, 6, 16]),
        ([4, 8, 2], [6, None, None], [6, 10, 0]),
        ([8, 8, 4], [0, 4, None], [0, 4, 12]),
        ([0, 8], [4, None], [4, 0]),
    ],
)
def test_stack_around_pins(
    sizes: list[int], pins: list[int | None], expected: list[int]
) -> None:
    assert stack_around_pins(sizes, pins) == expected


@pytest.mark.parametrize(
    ("ranges", "expected"),
    [
        ([], None),
        ([(0, 8), (8, 16)], None),
        ([(0, 8), (4, 12)], (0, 1)),
        ([(8, 16), (0, 12)], (1, 0)),
        ([(0, 16), (4, 4)], None),
        ([(4, 4), (4, 4)], None),
        ([(0, 32), (4, 8), (16, 20)], (0, 1)),
    ],
)
def test_first_overlap(
    ranges: list[tuple[int, int]], expected: tuple[int, int] | None
) -> None:
    assert first_overlap(ranges) == expected
