#
# SPDX-License-Identifier: Apache-2.0
#


def ensure_size_range(size_min: int, size_max: int | None = None) -> None:
    """Raise ValueError unless size_min is positive and size_max at least matches."""
    if size_min <= 0:
        raise ValueError("size_min must be positive")
    if size_max is not None and size_max < size_min:
        raise ValueError("size_max must be >= size_min")


def ensure_duration_range(duration_min: int, duration_max: int | None = None) -> None:
    """Raise ValueError unless duration_min is positive and duration_max matches."""
    if duration_min <= 0:
        raise ValueError("duration_min must be positive")
    if duration_max is not None and duration_max < duration_min:
        raise ValueError("duration_max must be >= duration_min")
