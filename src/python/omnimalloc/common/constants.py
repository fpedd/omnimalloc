#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Final

# Shared wall-clock budget for every time-bounded allocator (seconds);
# None disables the budget.
DEFAULT_TIMEOUT: Final[float] = 3.0

# Shared seed for every randomized allocator and benchmark source.
DEFAULT_SEED: Final[int] = 42

# Budgets make huge vector-clock instances fail fast instead of stalling or
# exhausting memory; None means unbounded. Work counts clock-component
# comparisons; entry points materializing per unit get 100x less, and the
# `conflicts` sweep less again, as its Python map costs ~260 bytes per pair.
DEFAULT_WORK_BUDGET: Final[int] = 10_000_000_000
DEFAULT_MATERIALIZE_BUDGET: Final[int] = DEFAULT_WORK_BUDGET // 100
DEFAULT_CONFLICT_MAP_BUDGET: Final[int] = 10_000_000

# Join-closure enumeration cap for the exact realizable-peak queries.
DEFAULT_CLOSURE_CAP: Final[int] = 1 << 14

# Storage units in bytes
KB: Final[int] = 1_024
MB: Final[int] = 1_024 * KB
GB: Final[int] = 1_024 * MB
TB: Final[int] = 1_024 * GB
