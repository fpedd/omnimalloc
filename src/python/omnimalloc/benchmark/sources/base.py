#
# SPDX-License-Identifier: Apache-2.0
#

from abc import abstractmethod

from omnimalloc.common.registry import Registered
from omnimalloc.primitives import Allocation, IdType, Memory, Pool, System


class BaseSource(Registered):
    """Base class for benchmark allocation sources with automatic registry.

    Sources provide workloads at different abstraction levels.
    Subclasses must implement `get_allocations()`. Higher-level methods
    (pools, memories, systems) have default implementations that build
    on allocations.

    Sources can be either:
    - Parameterizable: Can generate arbitrary numbers of allocations
      (e.g., RandomSource)
    - Fixed: Have predetermined models/pools with fixed allocation counts
      (e.g., Huggingface)
    """

    def __init__(
        self,
        num_allocations: int = 100,
        num_pools: int = 1,
        num_memories: int = 1,
        num_systems: int = 1,
    ) -> None:
        super().__init__()
        if num_allocations <= 0:
            raise ValueError("num_allocations must be positive")
        if num_pools <= 0:
            raise ValueError("num_pools must be positive")
        if num_memories <= 0:
            raise ValueError("num_memories must be positive")
        if num_systems <= 0:
            raise ValueError("num_systems must be positive")
        self._num_allocations = num_allocations
        self._num_pools = num_pools
        self._num_memories = num_memories
        self._num_systems = num_systems

    @property
    def num_allocations(self) -> int:
        return self._num_allocations

    @num_allocations.setter
    def num_allocations(self, value: int) -> None:
        if value <= 0:
            raise ValueError("num_allocations must be positive")
        self._num_allocations = value

    @property
    def num_pools(self) -> int:
        return self._num_pools

    @num_pools.setter
    def num_pools(self, value: int) -> None:
        if value <= 0:
            raise ValueError("num_pools must be positive")
        self._num_pools = value

    @property
    def num_memories(self) -> int:
        return self._num_memories

    @num_memories.setter
    def num_memories(self, value: int) -> None:
        if value <= 0:
            raise ValueError("num_memories must be positive")
        self._num_memories = value

    @property
    def num_systems(self) -> int:
        return self._num_systems

    @num_systems.setter
    def num_systems(self, value: int) -> None:
        if value <= 0:
            raise ValueError("num_systems must be positive")
        self._num_systems = value

    def is_parameterizable(self) -> bool:
        """Whether this source can generate arbitrary allocation counts."""
        return True

    def get_available_variants(
        self, variants: int | None = None
    ) -> tuple[str, ...] | None:
        """Return available variant identifiers for fixed sources."""
        ...

    def get_variant(self, variant_id: IdType) -> Pool:
        """Get a specific pool variant by ID."""

        if isinstance(variant_id, int):
            original_num = self._num_allocations
            self._num_allocations = variant_id
            try:
                pool = self.get_pool()
            finally:
                self._num_allocations = original_num
            return pool

        msg = f"Source {self.name()} does not support variant ID: {variant_id}"
        raise ValueError(msg)

    @abstractmethod
    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]: ...

    def get_pools(
        self, num_pools: int | None = None, skip: int = 0
    ) -> tuple[Pool, ...]:
        num_pools = num_pools or self._num_pools
        pools = []
        for i in range(num_pools):
            allocations = self.get_allocations(
                num_allocations=self._num_allocations,
                skip=(skip + i) * self._num_allocations,
            )
            if not allocations:
                raise ValueError(f"source {self.name()} returned no allocations")
            pools.append(Pool(id=f"{self.name()}_pool_{i}", allocations=allocations))
        return tuple(pools)

    def get_memories(
        self, num_memories: int | None = None, skip: int = 0
    ) -> tuple[Memory, ...]:
        num_memories = num_memories or self._num_memories
        memories = []
        for i in range(num_memories + skip):
            pools = self.get_pools(
                num_pools=self._num_pools,
                skip=(skip + i) * self._num_pools,
            )
            if not pools:
                raise ValueError(f"source {self.name()} returned no pools")
            memories.append(Memory(id=f"{self.name()}_memory_{i}", pools=pools))
        return tuple(memories)

    def get_systems(
        self, num_systems: int | None = None, skip: int = 0
    ) -> tuple[System, ...]:
        num_systems = num_systems or self._num_systems
        systems = []
        for i in range(num_systems + skip):
            memories = self.get_memories(
                num_memories=self._num_memories,
                skip=(skip + i) * self._num_memories,
            )
            if not memories:
                raise ValueError(f"source {self.name()} returned no memories")
            systems.append(System(id=f"{self.name()}_system_{i}", memories=memories))
        return tuple(systems)

    def get_allocation(self) -> Allocation:
        allocations = self.get_allocations(num_allocations=1)
        return allocations[0]

    def get_pool(self) -> Pool:
        pools = self.get_pools(num_pools=1)
        return pools[0]

    def get_memory(self) -> Memory:
        memories = self.get_memories(num_memories=1)
        return memories[0]

    def get_system(self) -> System:
        systems = self.get_systems(num_systems=1)
        return systems[0]
