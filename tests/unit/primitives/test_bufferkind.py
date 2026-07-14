#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives import BufferKind

KIND_PROPERTIES = (
    (BufferKind.WORKSPACE, 0, "workspace", False),
    (BufferKind.CONSTANT, 1, "constant", False),
    (BufferKind.INPUT, 2, "input", True),
    (BufferKind.OUTPUT, 3, "output", True),
)


@pytest.mark.parametrize(("kind", "value", "text", "is_io"), KIND_PROPERTIES)
def test_bufferkind_properties(
    kind: BufferKind, value: int, text: str, is_io: bool
) -> None:
    assert kind.value == value
    assert str(kind) == repr(kind) == text
    assert kind.is_io is is_io
    assert BufferKind(value) is kind
    assert BufferKind[kind.name] is kind


def test_bufferkind_members_are_distinct_and_hashable() -> None:
    kinds = list(BufferKind)
    assert len(kinds) == 4
    assert len(set(kinds)) == 4


def test_bufferkind_invalid_lookups() -> None:
    with pytest.raises(ValueError, match="4 is not a valid BufferKind"):
        BufferKind(4)
    with pytest.raises(KeyError):
        BufferKind["INVALID"]
