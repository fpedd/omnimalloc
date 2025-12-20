#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives.utils import hash_id


def test_hash_id_with_string() -> None:
    """Test that hash_id returns a stable positive integer for string input."""
    result = hash_id("test_allocation")
    assert isinstance(result, int)
    assert result > 0
    # Verify deterministic - same input produces same output
    assert hash_id("test_allocation") == result


def test_hash_id_with_integer() -> None:
    """Test that hash_id returns the same integer when given an integer."""
    assert hash_id(42) == 42
    assert hash_id(0) == 0
    assert hash_id(999999) == 999999


def test_hash_id_deterministic() -> None:
    """Test that hash_id produces deterministic output."""
    test_strings = ["alloc_1", "buffer_workspace", "tensor_x"]
    for test_str in test_strings:
        first_hash = hash_id(test_str)
        second_hash = hash_id(test_str)
        assert first_hash == second_hash


def test_hash_id_different_strings() -> None:
    """Test that different strings produce different hashes."""
    hash1 = hash_id("allocation_a")
    hash2 = hash_id("allocation_b")
    assert hash1 != hash2


def test_hash_id_invalid_type() -> None:
    """Test that hash_id raises TypeError for invalid input types."""
    with pytest.raises(TypeError, match="id_value must be str or int"):
        hash_id(3.14)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="id_value must be str or int"):
        hash_id(None)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="id_value must be str or int"):
        hash_id([1, 2, 3])  # type: ignore[arg-type]
