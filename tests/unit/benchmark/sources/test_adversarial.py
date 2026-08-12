#
# SPDX-License-Identifier: Apache-2.0
#

import random

import pytest
from omnimalloc import allocate, validate_allocation
from omnimalloc.analysis import antichain_pressure, try_linearize
from omnimalloc.benchmark.sources import SkewedSource, TwoPlusTwoSource
from omnimalloc.benchmark.sources.sizes import SIZE_DISTRIBUTIONS, sample_sizes
from omnimalloc.primitives import Pool


def _share_of_largest(sizes: tuple[int, ...], count: int) -> float:
    ordered = sorted(sizes, reverse=True)
    return sum(ordered[:count]) / sum(ordered)


def test_skewed_source_generates_the_requested_count() -> None:
    allocations = SkewedSource(num_allocations=64).get_allocations()
    assert len(allocations) == 64
    assert {a.id for a in allocations} == set(range(64))


def test_skewed_source_is_deterministic_for_a_fixed_seed() -> None:
    first = SkewedSource(num_allocations=32, seed=7).get_allocations()
    second = SkewedSource(num_allocations=32, seed=7).get_allocations()
    assert first == second


def test_skewed_source_respects_the_size_bounds() -> None:
    for distribution in SIZE_DISTRIBUTIONS:
        allocations = SkewedSource(
            num_allocations=128,
            distribution=distribution,
            size_min=64,
            size_max=4096,
        ).get_allocations()
        assert all(64 <= a.size <= 4096 for a in allocations), distribution


def test_dominant_distribution_concentrates_the_bytes() -> None:
    uniform = SkewedSource(num_allocations=200, distribution="uniform")
    dominant = SkewedSource(num_allocations=200, distribution="dominant")
    flat = tuple(a.size for a in uniform.get_allocations())
    skewed = tuple(a.size for a in dominant.get_allocations())
    assert _share_of_largest(skewed, 1) > 5 * _share_of_largest(flat, 1)


def test_skewed_source_rejects_an_unknown_distribution() -> None:
    with pytest.raises(ValueError, match="not a valid SizeDistribution"):
        SkewedSource(distribution="gaussian")


def test_skewed_placements_validate() -> None:
    for distribution in SIZE_DISTRIBUTIONS:
        allocations = SkewedSource(
            num_allocations=120, distribution=distribution
        ).get_allocations()
        validate_allocation(allocate(Pool(id="p", allocations=allocations), "omni"))


def test_two_plus_two_never_linearizes() -> None:
    allocations = TwoPlusTwoSource(num_allocations=64).get_allocations()
    assert try_linearize(allocations, work_budget=None) is None


def test_two_plus_two_stays_non_linearizable_under_noise() -> None:
    for noise in (0.0, 0.25, 0.5, 0.9):
        allocations = TwoPlusTwoSource(
            num_allocations=64, noise=noise
        ).get_allocations()
        assert try_linearize(allocations, work_budget=None) is None, noise


def test_two_plus_two_rejects_a_noise_of_one() -> None:
    with pytest.raises(ValueError, match="noise must be in"):
        TwoPlusTwoSource(noise=1.0)


def test_two_plus_two_generates_vector_clocks() -> None:
    allocations = TwoPlusTwoSource(num_allocations=16).get_allocations()
    assert len(allocations) == 16
    assert all(a.dim == 2 for a in allocations)


def test_sample_sizes_is_empty_for_a_non_positive_count() -> None:
    assert sample_sizes(random.Random(0), 0, "uniform", 1, 2) == []


def test_two_plus_two_placements_validate_and_respect_the_bound() -> None:
    for noise in (0.0, 0.5):
        allocations = TwoPlusTwoSource(
            num_allocations=96, noise=noise
        ).get_allocations()
        placed = allocate(Pool(id="p", allocations=allocations), "omni")
        validate_allocation(placed)
        bound = antichain_pressure(allocations, work_budget=None)
        assert max(a.offset + a.size for a in placed.allocations) >= bound


def test_two_plus_two_rejects_fewer_than_a_full_group() -> None:
    with pytest.raises(ValueError, match="at least 4"):
        TwoPlusTwoSource().get_allocations(num_allocations=3)
