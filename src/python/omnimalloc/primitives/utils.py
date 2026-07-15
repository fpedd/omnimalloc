#
# SPDX-License-Identifier: Apache-2.0
#

from .allocation import Allocation


def ensure_unique_ids(allocations: tuple[Allocation, ...]) -> None:
    """Raise if any allocation id repeats; id-keyed placement assumes uniqueness."""
    if len({alloc.id for alloc in allocations}) != len(allocations):
        raise ValueError("allocation ids must be unique")
