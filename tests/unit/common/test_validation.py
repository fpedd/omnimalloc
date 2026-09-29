#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.common.validation import ensure_non_negative, ensure_positive


@pytest.mark.parametrize(
    ("check", "value", "allow_none"),
    [
        (ensure_positive, 1, False),
        (ensure_positive, 0.5, False),
        (ensure_positive, None, True),
        (ensure_non_negative, 0, False),
        (ensure_non_negative, 100_000_000, False),
        (ensure_non_negative, None, True),
    ],
)
def test_accepts(check: object, value: float | None, allow_none: bool) -> None:
    check(value, "x", allow_none=allow_none)


@pytest.mark.parametrize(
    ("check", "value", "allow_none", "message"),
    [
        (ensure_positive, 0, False, "x must be positive,"),
        (ensure_positive, -0.5, False, "x must be positive,"),
        (ensure_positive, None, False, "x must be positive,"),
        (ensure_positive, 0, True, "x must be positive or None"),
        (ensure_non_negative, -1, False, "x must be non-negative,"),
        (ensure_non_negative, None, False, "x must be non-negative,"),
        (ensure_non_negative, -1, True, "x must be non-negative or None"),
    ],
)
def test_rejects_naming_the_parameter(
    check: object, value: float | None, allow_none: bool, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        check(value, "x", allow_none=allow_none)
