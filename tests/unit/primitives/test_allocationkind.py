#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives import AllocationKind

KIND_PROPERTIES = (
    (AllocationKind.WORKSPACE, 0, "workspace", False),
    (AllocationKind.CONSTANT, 1, "constant", False),
    (AllocationKind.INPUT, 2, "input", True),
    (AllocationKind.OUTPUT, 3, "output", True),
)


@pytest.mark.parametrize(("kind", "value", "text", "is_io"), KIND_PROPERTIES)
def test_allocationkind_properties(
    kind: AllocationKind, value: int, text: str, is_io: bool
) -> None:
    assert kind.value == value
    assert str(kind) == repr(kind) == text
    assert kind.is_io is is_io
    assert AllocationKind(value) is kind
    assert AllocationKind[kind.name] is kind


def test_allocationkind_members_are_distinct_and_hashable() -> None:
    kinds = list(AllocationKind)
    assert len(kinds) == 4
    assert len(set(kinds)) == 4


def test_allocationkind_invalid_lookups() -> None:
    with pytest.raises(ValueError, match="4 is not a valid AllocationKind"):
        AllocationKind(4)
    with pytest.raises(KeyError):
        AllocationKind["INVALID"]
