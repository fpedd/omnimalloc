#
# SPDX-License-Identifier: Apache-2.0
#

import random

from omnimalloc.common.intervals import FreeGaps, stack_around_pins


def test_free_gaps_of_nothing_occupied_claims_from_zero() -> None:
    assert FreeGaps([]).claim(16) == 0


def test_free_gaps_takes_an_exactly_fitting_hole() -> None:
    assert FreeGaps([(8, 16)]).claim(8) == 0


def test_free_gaps_skips_a_hole_one_short() -> None:
    assert FreeGaps([(7, 16)]).claim(8) == 16


def test_free_gaps_lands_above_the_last_range_when_nothing_fits() -> None:
    assert FreeGaps([(0, 8), (8, 24)]).claim(16) == 24


def test_free_gaps_tolerates_overlapping_occupied_ranges() -> None:
    assert FreeGaps([(0, 32), (8, 16)]).claim(8) == 32


def test_free_gaps_tolerates_unsorted_occupied_ranges() -> None:
    assert FreeGaps([(24, 32), (0, 8)]).claim(16) == 8


def test_free_gaps_claiming_nothing_reserves_nothing() -> None:
    gaps = FreeGaps([(8, 16)])
    assert gaps.claim(0) == 0
    assert gaps.claim(8) == 0


def test_free_gaps_reuses_a_hole_a_later_claim_still_fits() -> None:
    gaps = FreeGaps([(16, 24)])
    assert gaps.claim(12) == 0
    assert gaps.claim(4) == 12
    assert gaps.claim(4) == 24


def test_free_gaps_keeps_claiming_from_the_hole_it_shrank() -> None:
    gaps = FreeGaps([(0, 4), (20, 24)])
    assert gaps.claim(8) == 4
    assert gaps.claim(8) == 12
    assert gaps.claim(8) == 24


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


def test_stack_around_pins_never_overlaps_a_pin_on_random_instances() -> None:
    rng = random.Random(7)
    for _ in range(200):
        count = rng.randint(1, 24)
        sizes = [rng.randint(0, 12) for _ in range(count)]
        offsets = [rng.choice([None, None, 0, 5, 11, 24]) for _ in range(count)]
        placed = stack_around_pins(sizes, offsets)
        assert [p for p, o in zip(placed, offsets, strict=True) if o is not None] == [
            o for o in offsets if o is not None
        ]
        ranges = [
            (p, p + s, o is not None)
            for p, s, o in zip(placed, sizes, offsets, strict=True)
            if s > 0
        ]
        for i, (lo, hi, pinned) in enumerate(ranges):
            for other_lo, other_hi, other_pinned in ranges[i + 1 :]:
                if pinned and other_pinned:
                    continue  # colliding pins are the caller's to reject
                assert hi <= other_lo or other_hi <= lo


def test_stack_around_pins_places_a_hundred_thousand_items_around_one_pin() -> None:
    count = 100_000
    offsets = stack_around_pins([8] * count, [10**9, *([None] * (count - 1))])
    assert offsets[0] == 10**9
    assert offsets[1:] == [8 * i for i in range(count - 1)]
