#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Final

# Storage units in bytes
B: Final[int] = 1
KB: Final[int] = 1_024
MB: Final[int] = 1_024 * KB
GB: Final[int] = 1_024 * MB
TB: Final[int] = 1_024 * GB

# Frequency units in hertz
HZ: Final[int] = 1
KHZ: Final[int] = 1_000 * HZ
MHZ: Final[int] = 1_000 * KHZ
GHZ: Final[int] = 1_000 * MHZ
