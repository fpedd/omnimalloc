#
# SPDX-License-Identifier: Apache-2.0
#

import time

import pytest
from omnimalloc.common.deadline import (
    deadline_expired,
    deadline_remaining,
    ensure_valid_timeout,
    make_deadline,
)


@pytest.mark.parametrize("timeout", [0.001, 1, 3.5, None])
def test_ensure_valid_timeout_accepts_positive_and_none(timeout: float | None) -> None:
    ensure_valid_timeout(timeout)


@pytest.mark.parametrize(
    "timeout", [0, 0.0, -1, -2.5, float("nan"), float("inf"), float("-inf")]
)
def test_ensure_valid_timeout_rejects_nonpositive_and_nonfinite(
    timeout: float,
) -> None:
    with pytest.raises(ValueError, match=r"timeout must be \w+ or None"):
        ensure_valid_timeout(timeout)


def test_make_deadline_none_disables_budget() -> None:
    assert make_deadline(None) is None
    assert deadline_remaining(None) is None
    assert deadline_expired(None) is False


def test_deadline_remaining_positive_before_expiry() -> None:
    deadline = make_deadline(60.0)
    remaining = deadline_remaining(deadline)
    assert remaining is not None
    assert 0 < remaining <= 60.0
    assert deadline_expired(deadline) is False


def test_deadline_remaining_clamps_to_zero_after_expiry() -> None:
    expired = time.monotonic() - 1.0
    assert deadline_remaining(expired) == 0.0
    assert deadline_expired(expired) is True
