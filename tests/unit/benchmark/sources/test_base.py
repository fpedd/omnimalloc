#
# SPDX-License-Identifier: Apache-2.0
#


import pytest
from omnimalloc.benchmark.sources.generator import RandomSource


def test_base_source_initialization_with_defaults() -> None:
    """Test BaseSource initialization with default parameters."""
    source = RandomSource()
    assert source.num_allocations == 100
    assert source.num_pools == 1
    assert source.num_memories == 1
    assert source.num_systems == 1


def test_base_source_initialization_with_custom_values() -> None:
    """Test BaseSource initialization with custom parameters."""
    source = RandomSource(num_allocations=50)
    assert source.num_allocations == 50
    assert source.num_pools == 1


def test_base_source_raises_error_for_zero_allocations() -> None:
    """Test that zero allocations raises ValueError."""
    with pytest.raises(ValueError, match="num_allocations must be positive"):
        RandomSource(num_allocations=0)


def test_base_source_raises_error_for_negative_allocations() -> None:
    """Test that negative allocations raises ValueError."""
    with pytest.raises(ValueError, match="num_allocations must be positive"):
        RandomSource(num_allocations=-1)


def test_base_source_num_allocations_property_setter() -> None:
    """Test num_allocations property setter."""
    source = RandomSource(num_allocations=10)
    assert source.num_allocations == 10

    source.num_allocations = 20
    assert source.num_allocations == 20


def test_base_source_num_allocations_setter_validates_positive() -> None:
    """Test that num_allocations setter validates positive values."""
    source = RandomSource(num_allocations=10)

    with pytest.raises(ValueError, match="num_allocations must be positive"):
        source.num_allocations = 0

    with pytest.raises(ValueError, match="num_allocations must be positive"):
        source.num_allocations = -5


def test_base_source_num_pools_property_setter() -> None:
    """Test num_pools property setter."""
    source = RandomSource()
    assert source.num_pools == 1

    source.num_pools = 3
    assert source.num_pools == 3


def test_base_source_num_pools_setter_validates_positive() -> None:
    """Test that num_pools setter validates positive values."""
    source = RandomSource()

    with pytest.raises(ValueError, match="num_pools must be positive"):
        source.num_pools = 0


def test_base_source_num_memories_property_setter() -> None:
    """Test num_memories property setter."""
    source = RandomSource()
    assert source.num_memories == 1

    source.num_memories = 2
    assert source.num_memories == 2


def test_base_source_num_memories_setter_validates_positive() -> None:
    """Test that num_memories setter validates positive values."""
    source = RandomSource()

    with pytest.raises(ValueError, match="num_memories must be positive"):
        source.num_memories = -1


def test_base_source_num_systems_property_setter() -> None:
    """Test num_systems property setter."""
    source = RandomSource()
    assert source.num_systems == 1

    source.num_systems = 4
    assert source.num_systems == 4


def test_base_source_num_systems_setter_validates_positive() -> None:
    """Test that num_systems setter validates positive values."""
    source = RandomSource()

    with pytest.raises(ValueError, match="num_systems must be positive"):
        source.num_systems = 0


def test_base_source_is_parameterizable() -> None:
    """Test is_parameterizable returns True for RandomSource."""
    source = RandomSource()
    assert source.is_parameterizable() is True


def test_base_source_get_allocation() -> None:
    """Test get_allocation returns a single allocation."""
    source = RandomSource(num_allocations=10, seed=42)
    allocation = source.get_allocation()

    assert allocation.id == 0
    assert allocation.size > 0
    assert allocation.start >= 0
    assert allocation.end > allocation.start


def test_base_source_get_allocations() -> None:
    """Test get_allocations returns correct number of allocations."""
    source = RandomSource(num_allocations=5, seed=42)
    allocations = source.get_allocations()

    assert len(allocations) == 5
    assert all(alloc.size > 0 for alloc in allocations)


def test_base_source_get_allocations_with_custom_num() -> None:
    """Test get_allocations with custom num_allocations parameter."""
    source = RandomSource(num_allocations=10, seed=42)
    allocations = source.get_allocations(num_allocations=3)

    assert len(allocations) == 3


def test_base_source_get_pool() -> None:
    """Test get_pool returns a single pool."""
    source = RandomSource(num_allocations=5, seed=42)
    pool = source.get_pool()

    assert pool.id == "random_source_pool_0"
    assert len(pool.allocations) == 5


def test_base_source_get_pools() -> None:
    """Test get_pools returns correct number of pools."""
    source = RandomSource(num_allocations=5, seed=42)
    pools = source.get_pools(num_pools=3)

    assert len(pools) == 3
    assert all(len(pool.allocations) == 5 for pool in pools)
    assert pools[0].id == "random_source_pool_0"
    assert pools[1].id == "random_source_pool_1"
    assert pools[2].id == "random_source_pool_2"


def test_base_source_get_memory() -> None:
    """Test get_memory returns a single memory."""
    source = RandomSource(num_allocations=5, seed=42)
    memory = source.get_memory()

    assert memory.id == "random_source_memory_0"
    assert len(memory.pools) == 1


def test_base_source_get_memories() -> None:
    """Test get_memories returns correct number of memories."""
    source = RandomSource(num_allocations=5, seed=42)
    memories = source.get_memories(num_memories=2)

    assert len(memories) == 2
    assert memories[0].id == "random_source_memory_0"
    assert memories[1].id == "random_source_memory_1"


def test_base_source_get_system() -> None:
    """Test get_system returns a single system."""
    source = RandomSource(num_allocations=5, seed=42)
    system = source.get_system()

    assert system.id == "random_source_system_0"
    assert len(system.memories) == 1


def test_base_source_get_systems() -> None:
    """Test get_systems returns correct number of systems."""
    source = RandomSource(num_allocations=5, seed=42)
    systems = source.get_systems(num_systems=2)

    assert len(systems) == 2
    assert systems[0].id == "random_source_system_0"
    assert systems[1].id == "random_source_system_1"


def test_base_source_get_variant_with_int() -> None:
    """Test get_variant with integer variant_id."""
    source = RandomSource(num_allocations=10, seed=42)
    pool = source.get_variant(5)

    assert len(pool.allocations) == 5
    # Original num_allocations should be preserved
    assert source.num_allocations == 10


def test_base_source_get_variant_with_str_raises_error() -> None:
    """Test get_variant with string raises ValueError for RandomSource."""
    source = RandomSource(num_allocations=10, seed=42)

    with pytest.raises(ValueError, match="does not support variant ID"):
        source.get_variant("model_name")


def test_base_source_get_pools_with_skip() -> None:
    """Test get_pools with skip parameter."""
    source = RandomSource(num_allocations=5, seed=42)
    pools_no_skip = source.get_pools(num_pools=2)
    pools_with_skip = source.get_pools(num_pools=2, skip=2)

    # Should return different pools when skipping
    assert len(pools_with_skip) == 2
    assert pools_with_skip[0].allocations != pools_no_skip[0].allocations


def test_base_source_hierarchical_structure() -> None:
    """Test that the hierarchical structure (system -> memory -> pool) works."""
    source = RandomSource(num_allocations=3, seed=42)
    source.num_pools = 2
    source.num_memories = 2

    systems = source.get_systems(num_systems=1)
    system = systems[0]

    assert len(system.memories) == 2
    assert len(system.memories[0].pools) == 2
    assert len(system.memories[0].pools[0].allocations) == 3
