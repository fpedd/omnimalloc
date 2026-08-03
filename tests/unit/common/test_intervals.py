#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.common.intervals import lowest_gap, stack_around_pins


def test_lowest_gap_of_nothing_occupied_is_zero() -> None:
    assert lowest_gap([], 16) == 0


def test_lowest_gap_takes_an_exactly_fitting_hole() -> None:
    assert lowest_gap([(8, 16)], 8) == 0


def test_lowest_gap_skips_a_hole_one_short() -> None:
    assert lowest_gap([(7, 16)], 8) == 16


def test_lowest_gap_lands_above_the_last_range_when_nothing_fits() -> None:
    assert lowest_gap([(0, 8), (8, 24)], 16) == 24


def test_lowest_gap_tolerates_overlapping_occupied_ranges() -> None:
    assert lowest_gap([(0, 32), (8, 16)], 8) == 32


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
