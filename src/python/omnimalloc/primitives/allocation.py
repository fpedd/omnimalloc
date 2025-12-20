#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import Allocation, BufferKind

# Type alias for allocation identifiers (int or str)
IdType = int | str

__all__ = ["Allocation", "BufferKind", "IdType"]
