#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives import BufferKind


def test_workspace_value() -> None:
    """Test WORKSPACE enum value exists."""
    assert BufferKind.WORKSPACE is not None
    assert BufferKind.WORKSPACE.value == 0


def test_constant_value() -> None:
    """Test CONSTANT enum value exists."""
    assert BufferKind.CONSTANT is not None
    assert BufferKind.CONSTANT.value == 1


def test_input_value() -> None:
    """Test INPUT enum value exists."""
    assert BufferKind.INPUT is not None
    assert BufferKind.INPUT.value == 2


def test_output_value() -> None:
    """Test OUTPUT enum value exists."""
    assert BufferKind.OUTPUT is not None
    assert BufferKind.OUTPUT.value == 3


def test_workspace_str() -> None:
    """Test WORKSPACE string representation."""
    assert str(BufferKind.WORKSPACE) == "workspace"


def test_constant_str() -> None:
    """Test CONSTANT string representation."""
    assert str(BufferKind.CONSTANT) == "constant"


def test_input_str() -> None:
    """Test INPUT string representation."""
    assert str(BufferKind.INPUT) == "input"


def test_output_str() -> None:
    """Test OUTPUT string representation."""
    assert str(BufferKind.OUTPUT) == "output"


def test_workspace_repr() -> None:
    """Test WORKSPACE repr representation."""
    assert repr(BufferKind.WORKSPACE) == "workspace"


def test_constant_repr() -> None:
    """Test CONSTANT repr representation."""
    assert repr(BufferKind.CONSTANT) == "constant"


def test_input_repr() -> None:
    """Test INPUT repr representation."""
    assert repr(BufferKind.INPUT) == "input"


def test_output_repr() -> None:
    """Test OUTPUT repr representation."""
    assert repr(BufferKind.OUTPUT) == "output"


def test_workspace_is_not_io() -> None:
    """Test WORKSPACE is_io property returns False."""
    assert BufferKind.WORKSPACE.is_io is False


def test_constant_is_not_io() -> None:
    """Test CONSTANT is_io property returns False."""
    assert BufferKind.CONSTANT.is_io is False


def test_input_is_io() -> None:
    """Test INPUT is_io property returns True."""
    assert BufferKind.INPUT.is_io is True


def test_output_is_io() -> None:
    """Test OUTPUT is_io property returns True."""
    assert BufferKind.OUTPUT.is_io is True


def test_enum_equality() -> None:
    """Test enum values are equal to themselves."""
    assert BufferKind.WORKSPACE == BufferKind.WORKSPACE
    assert BufferKind.CONSTANT == BufferKind.CONSTANT
    assert BufferKind.INPUT == BufferKind.INPUT
    assert BufferKind.OUTPUT == BufferKind.OUTPUT


def test_enum_inequality() -> None:
    """Test different enum values are not equal."""
    assert BufferKind.WORKSPACE != BufferKind.CONSTANT
    assert BufferKind.WORKSPACE != BufferKind.INPUT
    assert BufferKind.WORKSPACE != BufferKind.OUTPUT
    assert BufferKind.CONSTANT != BufferKind.INPUT
    assert BufferKind.CONSTANT != BufferKind.OUTPUT
    assert BufferKind.INPUT != BufferKind.OUTPUT


def test_enum_identity() -> None:
    """Test enum values have consistent identity."""
    assert BufferKind.WORKSPACE is BufferKind.WORKSPACE
    assert BufferKind.CONSTANT is BufferKind.CONSTANT
    assert BufferKind.INPUT is BufferKind.INPUT
    assert BufferKind.OUTPUT is BufferKind.OUTPUT


def test_enum_iteration() -> None:
    """Test iterating over all BufferKind values."""
    kinds = list(BufferKind)
    assert len(kinds) == 4
    assert BufferKind.WORKSPACE in kinds
    assert BufferKind.CONSTANT in kinds
    assert BufferKind.INPUT in kinds
    assert BufferKind.OUTPUT in kinds


def test_enum_by_name() -> None:
    """Test accessing enum values by name."""
    assert BufferKind["WORKSPACE"] == BufferKind.WORKSPACE
    assert BufferKind["CONSTANT"] == BufferKind.CONSTANT
    assert BufferKind["INPUT"] == BufferKind.INPUT
    assert BufferKind["OUTPUT"] == BufferKind.OUTPUT


def test_enum_by_value() -> None:
    """Test accessing enum values by numeric value."""
    assert BufferKind(0) == BufferKind.WORKSPACE
    assert BufferKind(1) == BufferKind.CONSTANT
    assert BufferKind(2) == BufferKind.INPUT
    assert BufferKind(3) == BufferKind.OUTPUT


def test_invalid_enum_value() -> None:
    """Test that invalid numeric value raises ValueError."""
    with pytest.raises(ValueError, match="4 is not a valid BufferKind"):
        BufferKind(4)


def test_invalid_enum_name() -> None:
    """Test that invalid name raises KeyError."""
    with pytest.raises(KeyError):
        BufferKind["INVALID"]


def test_hash_consistency() -> None:
    """Test that enum values are hashable and consistent."""
    kinds_set = {
        BufferKind.WORKSPACE,
        BufferKind.CONSTANT,
        BufferKind.INPUT,
        BufferKind.OUTPUT,
    }
    assert len(kinds_set) == 4
    assert BufferKind.WORKSPACE in kinds_set
    assert BufferKind.CONSTANT in kinds_set
    assert BufferKind.INPUT in kinds_set
    assert BufferKind.OUTPUT in kinds_set


def test_enum_names() -> None:
    """Test enum member names."""
    assert BufferKind.WORKSPACE.name == "WORKSPACE"
    assert BufferKind.CONSTANT.name == "CONSTANT"
    assert BufferKind.INPUT.name == "INPUT"
    assert BufferKind.OUTPUT.name == "OUTPUT"


def test_is_io_filter() -> None:
    """Test filtering enum values by is_io property."""
    io_kinds = [kind for kind in BufferKind if kind.is_io]
    non_io_kinds = [kind for kind in BufferKind if not kind.is_io]

    assert len(io_kinds) == 2
    assert BufferKind.INPUT in io_kinds
    assert BufferKind.OUTPUT in io_kinds

    assert len(non_io_kinds) == 2
    assert BufferKind.WORKSPACE in non_io_kinds
    assert BufferKind.CONSTANT in non_io_kinds


def test_str_and_repr_consistency() -> None:
    """Test that str and repr return the same value."""
    assert str(BufferKind.WORKSPACE) == repr(BufferKind.WORKSPACE)
    assert str(BufferKind.CONSTANT) == repr(BufferKind.CONSTANT)
    assert str(BufferKind.INPUT) == repr(BufferKind.INPUT)
    assert str(BufferKind.OUTPUT) == repr(BufferKind.OUTPUT)
