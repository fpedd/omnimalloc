#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omnimalloc.allocators import BaseAllocator


from .allocation import IdType
from .memory import Memory


@dataclass(frozen=True)
class System:
    """Top-level container representing a complete memory hierarchy."""

    id: IdType
    memories: tuple[Memory, ...]

    def __post_init__(self) -> None:
        if len({memory.id for memory in self.memories}) != len(self.memories):
            raise ValueError("memory ids must be unique")

    @property
    def is_allocated(self) -> bool:
        """True if all memories have been allocated."""
        return all(memory.is_allocated for memory in self.memories)

    def with_memories(self, memories: tuple[Memory, ...]) -> "System":
        """Return new System with specified memories."""
        return System(id=self.id, memories=memories)

    def allocate(self, allocator: "BaseAllocator") -> "System":
        """Apply allocator to all memories."""
        return self.with_memories(tuple(m.allocate(allocator) for m in self.memories))
