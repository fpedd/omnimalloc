#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.common.validation import ensure_non_negative, ensure_positive


@pytest.mark.parametrize("value", [1, 0.5, 100])
def test_ensure_positive_accepts_positive(value: float) -> None:
    ensure_positive(value, "value")


@pytest.mark.parametrize("value", [0, -1, -0.5])
def test_ensure_positive_rejects_non_positive(value: float) -> None:
    with pytest.raises(ValueError, match="value must be positive"):
        ensure_positive(value, "value")


def test_ensure_positive_none_needs_allow_none() -> None:
    ensure_positive(None, "num_threads", allow_none=True)
    with pytest.raises(ValueError, match="num_threads must be positive"):
        ensure_positive(None, "num_threads")


@pytest.mark.parametrize("value", [0, 1, 100_000_000])
def test_ensure_non_negative_accepts_non_negative(value: int) -> None:
    ensure_non_negative(value, "work_budget")


def test_ensure_non_negative_rejects_negative() -> None:
    with pytest.raises(ValueError, match="work_budget must be non-negative"):
        ensure_non_negative(-1, "work_budget")


def test_ensure_non_negative_none_needs_allow_none() -> None:
    ensure_non_negative(None, "work_budget", allow_none=True)
    with pytest.raises(ValueError, match="work_budget must be non-negative"):
        ensure_non_negative(None, "work_budget")


def test_error_names_the_given_parameter() -> None:
    with pytest.raises(ValueError, match="linearize_budget must be non-negative"):
        ensure_non_negative(-1, "linearize_budget", allow_none=True)


def test_allow_none_messages_name_none_as_an_option() -> None:
    with pytest.raises(ValueError, match="num_threads must be positive or None"):
        ensure_positive(0, "num_threads", allow_none=True)
    with pytest.raises(ValueError, match="work_budget must be non-negative or None"):
        ensure_non_negative(-1, "work_budget", allow_none=True)
