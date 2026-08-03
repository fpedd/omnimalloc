#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omnimalloc.allocators import BaseAllocator


from .allocation import IdType
from .memory import Memory
from .utils import ensure_items, ensure_unique_ids


@dataclass(frozen=True)
class System:
    """Top-level container representing a complete memory hierarchy."""

    id: IdType
    memories: tuple[Memory, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "memories", ensure_items(self.memories, Memory, "memories")
        )
        ensure_unique_ids(self.memories, "memory")

    @property
    def is_allocated(self) -> bool:
        """True if all memories have been allocated."""
        return all(memory.is_allocated for memory in self.memories)

    @property
    def any_allocated(self) -> bool:
        """True if any memory has a placed allocation."""
        return any(memory.any_allocated for memory in self.memories)

    def with_memories(self, memories: tuple[Memory, ...]) -> "System":
        """Return new System with specified memories."""
        return System(id=self.id, memories=memories)

    def allocate(self, allocator: "BaseAllocator") -> "System":
        """Apply allocator to all memories."""
        return self.with_memories(tuple(m.allocate(allocator) for m in self.memories))
