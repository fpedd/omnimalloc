#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation, BufferKind


# ---------------------------------------------------------------------------
# Empty and single-allocation cases
# ---------------------------------------------------------------------------


def test_naive_empty() -> None:
    allocator = NaiveAllocator()
    result = allocator.allocate(())
    assert result == ()


def test_naive_single_allocation() -> None:
    allocator = NaiveAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=10)
    result = allocator.allocate((alloc,))
    assert len(result) == 1
    assert result[0].offset == 0


def test_naive_single_large_allocation() -> None:
    allocator = NaiveAllocator()
    alloc = Allocation(id=1, size=1_000_000, start=0, end=1)
    result = allocator.allocate((alloc,))
    assert result[0].offset == 0


# ---------------------------------------------------------------------------
# Sequential placement: offsets are cumulative sums of sizes
# ---------------------------------------------------------------------------


def test_naive_two_allocations_offsets_are_sequential() -> None:
    """Second allocation starts right after the first regardless of time."""
    allocator = NaiveAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=200, start=5, end=15)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_naive_three_allocations_cumulative_offsets() -> None:
    allocator = NaiveAllocator()
    alloc1 = Allocation(id=1, size=50, start=0, end=5)
    alloc2 = Allocation(id=2, size=75, start=3, end=8)
    alloc3 = Allocation(id=3, size=25, start=6, end=12)
    result = allocator.allocate((alloc1, alloc2, alloc3))
    assert result[0].offset == 0
    assert result[1].offset == 50
    assert result[2].offset == 125


def test_naive_uniform_sizes_produce_regular_offsets() -> None:
    allocator = NaiveAllocator()
    size = 64
    n = 8
    allocs = tuple(Allocation(id=i, size=size, start=0, end=10) for i in range(n))
    result = allocator.allocate(allocs)
    for i, alloc in enumerate(result):
        assert alloc.offset == i * size


def test_naive_varying_sizes_produce_cumulative_offsets() -> None:
    allocator = NaiveAllocator()
    sizes = [10, 20, 30, 40, 50]
    allocs = tuple(Allocation(id=i, size=s, start=0, end=5) for i, s in enumerate(sizes))
    result = allocator.allocate(allocs)
    expected_offsets = [0, 10, 30, 60, 100]
    for alloc, expected in zip(result, expected_offsets, strict=True):
        assert alloc.offset == expected


# ---------------------------------------------------------------------------
# No space reuse: naive ignores temporal non-overlaps
# ---------------------------------------------------------------------------


def test_naive_non_overlapping_in_time_still_sequential() -> None:
    """Naive allocator does NOT reuse space for temporally disjoint allocations."""
    allocator = NaiveAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=100, start=10, end=15)  # completely after alloc1
    result = allocator.allocate((alloc1, alloc2))
    # Greedy would put alloc2 at offset 0, but naive always stacks sequentially
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_naive_fully_overlapping_in_time_are_sequential() -> None:
    allocator = NaiveAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=100, start=0, end=10)
    alloc3 = Allocation(id=3, size=100, start=0, end=10)
    result = allocator.allocate((alloc1, alloc2, alloc3))
    assert result[0].offset == 0
    assert result[1].offset == 100
    assert result[2].offset == 200


def test_naive_single_timestep_allocations() -> None:
    allocator = NaiveAllocator()
    alloc1 = Allocation(id=1, size=32, start=5, end=6)
    alloc2 = Allocation(id=2, size=32, start=5, end=6)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 32


# ---------------------------------------------------------------------------
# Property preservation
# ---------------------------------------------------------------------------


def test_naive_preserves_integer_ids() -> None:
    allocator = NaiveAllocator()
    allocs = tuple(Allocation(id=i * 10, size=100, start=0, end=5) for i in range(4))
    result = allocator.allocate(allocs)
    assert [a.id for a in result] == [0, 10, 20, 30]


def test_naive_preserves_string_ids() -> None:
    allocator = NaiveAllocator()
    alloc1 = Allocation(id="weights", size=512, start=0, end=10)
    alloc2 = Allocation(id="activations", size=256, start=5, end=15)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].id == "weights"
    assert result[1].id == "activations"


def test_naive_preserves_sizes() -> None:
    allocator = NaiveAllocator()
    sizes = [100, 200, 50, 300]
    allocs = tuple(Allocation(id=i, size=s, start=0, end=5) for i, s in enumerate(sizes))
    result = allocator.allocate(allocs)
    assert [a.size for a in result] == sizes


def test_naive_preserves_time_range() -> None:
    allocator = NaiveAllocator()
    alloc = Allocation(id=1, size=100, start=7, end=42)
    result = allocator.allocate((alloc,))
    assert result[0].start == 7
    assert result[0].end == 42


def test_naive_preserves_kind() -> None:
    allocator = NaiveAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=5, kind=BufferKind.CONSTANT)
    result = allocator.allocate((alloc,))
    assert result[0].kind == BufferKind.CONSTANT


def test_naive_allocations_without_kind_stay_without_kind() -> None:
    allocator = NaiveAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=5)
    result = allocator.allocate((alloc,))
    assert result[0].kind is None


# ---------------------------------------------------------------------------
# Output contract
# ---------------------------------------------------------------------------


def test_naive_returns_tuple() -> None:
    allocator = NaiveAllocator()
    allocs = (Allocation(id=1, size=100, start=0, end=5),)
    result = allocator.allocate(allocs)
    assert isinstance(result, tuple)


def test_naive_output_length_matches_input() -> None:
    allocator = NaiveAllocator()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=5) for i in range(10))
    result = allocator.allocate(allocs)
    assert len(result) == 10


def test_naive_all_output_allocations_are_placed() -> None:
    allocator = NaiveAllocator()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=5) for i in range(6))
    result = allocator.allocate(allocs)
    assert all(a.offset is not None for a in result)
    assert all(a.is_allocated for a in result)


def test_naive_input_allocations_are_not_mutated() -> None:
    """Original allocations must remain unplaced after a call to allocate()."""
    allocator = NaiveAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=5)
    allocator.allocate((alloc,))
    assert alloc.offset is None


# ---------------------------------------------------------------------------
# Order sensitivity: naive allocates in the order it receives allocations
# ---------------------------------------------------------------------------


def test_naive_order_determines_offsets() -> None:
    allocator = NaiveAllocator()
    a = Allocation(id="a", size=100, start=0, end=5)
    b = Allocation(id="b", size=200, start=0, end=5)

    result_ab = allocator.allocate((a, b))
    result_ba = allocator.allocate((b, a))

    # With (a, b): a→0, b→100
    assert result_ab[0].offset == 0
    assert result_ab[1].offset == 100

    # With (b, a): b→0, a→200
    assert result_ba[0].offset == 0
    assert result_ba[1].offset == 200


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_naive_is_deterministic() -> None:
    allocator = NaiveAllocator()
    allocs = tuple(
        Allocation(id=i, size=(i + 1) * 64, start=i, end=i + 5) for i in range(20)
    )
    result1 = allocator.allocate(allocs)
    result2 = allocator.allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


# ---------------------------------------------------------------------------
# Large workload
# ---------------------------------------------------------------------------


def test_naive_large_workload_offsets_are_cumulative_sums() -> None:
    allocator = NaiveAllocator()
    n = 500
    size = 128
    allocs = tuple(Allocation(id=i, size=size, start=0, end=10) for i in range(n))
    result = allocator.allocate(allocs)
    assert len(result) == n
    for i, alloc in enumerate(result):
        assert alloc.offset == i * size
