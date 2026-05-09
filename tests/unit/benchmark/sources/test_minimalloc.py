#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.benchmark.sources.minimalloc import MinimallocSource, MinimallocSubset
from omnimalloc.primitives import BufferKind


def test_minimalloc_source_default_subset_is_challenging() -> None:
    source = MinimallocSource()
    assert source.subset is MinimallocSubset.CHALLENGING
    assert source.num_allocations > 0


def test_minimalloc_source_accepts_enum_member() -> None:
    source = MinimallocSource(MinimallocSubset.SMALL)
    assert source.subset is MinimallocSubset.SMALL


def test_minimalloc_source_accepts_string_alias() -> None:
    """Raw strings are coerced to the matching enum member."""
    source = MinimallocSource("small")
    assert source.subset is MinimallocSubset.SMALL
    assert source.subset == "small"


def test_minimalloc_source_examples_subset() -> None:
    source = MinimallocSource(subset="examples")
    assert source.subset == "examples"
    variants = source.get_available_variants()
    assert len(variants) == 1  # Only one example pool


def test_minimalloc_source_small_subset() -> None:
    source = MinimallocSource(subset="small")
    assert source.subset == "small"
    variants = source.get_available_variants()
    assert len(variants) > 0
    assert all(v[0].islower() for v in variants)


def test_minimalloc_source_challenging_subset() -> None:
    source = MinimallocSource(subset="challenging")
    variants = source.get_available_variants()
    assert len(variants) > 0


def test_minimalloc_source_subsets_are_disjoint() -> None:
    examples = set(MinimallocSource(subset="examples").get_available_variants())
    small = set(MinimallocSource(subset="small").get_available_variants())
    challenging = set(MinimallocSource(subset="challenging").get_available_variants())
    assert examples
    assert small
    assert challenging
    assert examples.isdisjoint(small)
    assert examples.isdisjoint(challenging)
    assert small.isdisjoint(challenging)


def test_minimalloc_source_invalid_subset() -> None:
    with pytest.raises(ValueError, match="not a valid MinimallocSubset"):
        MinimallocSource(subset="bogus")  # type: ignore[arg-type]


def test_minimalloc_source_get_allocations_skip_past_end() -> None:
    source = MinimallocSource(subset="examples")
    allocations = source.get_allocations(skip=10**9)
    assert len(allocations) == 0


def test_minimalloc_source_get_pools_with_skip_past_end() -> None:
    source = MinimallocSource(subset="examples")
    pools = source.get_pools(skip=10)
    assert len(pools) == 0


def test_minimalloc_source_get_pools_count_zero() -> None:
    source = MinimallocSource(subset="examples")
    pools = source.get_pools(num_pools=0)
    assert len(pools) == 0


def test_minimalloc_source_get_allocation_workspace_kind() -> None:
    """All loaded allocations are tagged as WORKSPACE buffers."""
    source = MinimallocSource(subset="examples")
    allocation = source.get_allocation()
    assert allocation.kind == BufferKind.WORKSPACE


def test_minimalloc_source_get_variant_by_id() -> None:
    source = MinimallocSource(subset="small")
    variants = source.get_available_variants()
    pool = source.get_variant(variants[0])
    assert pool.id == variants[0]


def test_minimalloc_source_get_variant_by_index() -> None:
    source = MinimallocSource(subset="small")
    pool = source.get_variant(0)
    assert pool.id in source.get_available_variants()


def test_minimalloc_source_get_variant_unknown_id() -> None:
    source = MinimallocSource(subset="examples")
    with pytest.raises(ValueError, match="not found"):
        source.get_variant("does-not-exist")
