#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.common.intervals import first_overlap, stack_around_pins


def test_stack_around_pins_stacks_when_nothing_is_pinned() -> None:
    assert stack_around_pins([4, 8, 16], [None, None, None]) == [0, 4, 12]


def test_stack_around_pins_returns_nothing_for_no_items() -> None:
    assert stack_around_pins([], []) == []


def test_stack_around_pins_leaves_a_pinned_offset_alone() -> None:
    assert stack_around_pins([4, 8], [64, None]) == [64, 0]


def test_stack_around_pins_fills_the_gap_below_a_pin() -> None:
    assert stack_around_pins([8, 4, 4], [16, None, None]) == [16, 0, 4]


def test_stack_around_pins_steps_over_a_pin_that_blocks_the_gap() -> None:
    assert stack_around_pins([8, 16], [4, None]) == [4, 12]


def test_stack_around_pins_packs_several_free_items_around_two_pins() -> None:
    offsets = stack_around_pins([4, 4, 2, 2, 8], [0, 12, None, None, None])
    assert offsets == [0, 12, 4, 6, 16]


def test_stack_around_pins_backfills_a_gap_a_larger_item_skipped() -> None:
    assert stack_around_pins([4, 8, 2], [6, None, None]) == [6, 10, 0]


def test_stack_around_pins_merges_overlapping_pins() -> None:
    assert stack_around_pins([8, 8, 4], [0, 4, None]) == [0, 4, 12]


def test_stack_around_pins_ignores_a_zero_width_pin() -> None:
    assert stack_around_pins([0, 8], [4, None]) == [4, 0]


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
