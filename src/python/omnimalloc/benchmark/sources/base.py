#
# SPDX-License-Identifier: Apache-2.0
#

from abc import abstractmethod
from typing import ClassVar

from omnimalloc.common.registry import Registered
from omnimalloc.common.validation import ensure_positive
from omnimalloc.primitives import Allocation, IdType, Memory, Pool, System


class BaseSource(Registered):
    """Base class for benchmark allocation sources with automatic registry.

    Subclasses implement `get_allocations()`. `_label_fields` names the
    parameters making one instance a different workload, keeping sweeps separable.
    """

    _strip_suffix: ClassVar[str] = "Source"
    _label_fields: ClassVar[tuple[str, ...]] = ()

    def __init__(
        self,
        num_allocations: int = 100,
        num_pools: int = 1,
        num_memories: int = 1,
        num_systems: int = 1,
    ) -> None:
        super().__init__()
        self.num_allocations = num_allocations
        self.num_pools = num_pools
        self.num_memories = num_memories
        self.num_systems = num_systems

    @property
    def num_allocations(self) -> int:
        return self._num_allocations

    @num_allocations.setter
    def num_allocations(self, value: int) -> None:
        ensure_positive(value, "num_allocations")
        self._num_allocations = value

    @property
    def num_pools(self) -> int:
        return self._num_pools

    @num_pools.setter
    def num_pools(self, value: int) -> None:
        ensure_positive(value, "num_pools")
        self._num_pools = value

    @property
    def num_memories(self) -> int:
        return self._num_memories

    @num_memories.setter
    def num_memories(self, value: int) -> None:
        ensure_positive(value, "num_memories")
        self._num_memories = value

    @property
    def num_systems(self) -> int:
        return self._num_systems

    @num_systems.setter
    def num_systems(self, value: int) -> None:
        ensure_positive(value, "num_systems")
        self._num_systems = value

    @property
    def memory_capacity(self) -> int | None:
        """Declared size for the memories this source builds.

        A generated workload models no hardware, so it declares no capacity;
        sources knowing an achievable optimum override it.
        """
        return None

    def label(self) -> str:
        """Registry name, plus the `_label_fields` that make this instance distinct.

        Two instances configured differently must not collapse into one series,
        so the label carries their parameters: `sync_pattern[num_threads=16]`.
        """
        if not self._label_fields:
            return self.name()
        fields = ",".join(f"{f}={getattr(self, f)}" for f in self._label_fields)
        return f"{self.name()}[{fields}]"

    def is_parameterizable(self) -> bool:
        """Whether this source can generate arbitrary allocation counts."""
        return True

    def get_known_optimum(self, variant_id: IdType | None = None) -> int | None:
        """Provably achievable peak size for a variant, or None if unknown.

        Sources that reverse-construct their instances from a packing know
        the optimum and override this; everyone else leaves it unknown.
        """

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
        for i in range(num_memories):
            pools = self.get_pools(
                num_pools=self._num_pools,
                skip=(skip + i) * self._num_pools,
            )
            if not pools:
                raise ValueError(f"source {self.name()} returned no pools")
            memories.append(
                Memory(
                    id=f"{self.name()}_memory_{i}",
                    pools=pools,
                    size=self.memory_capacity,
                )
            )
        return tuple(memories)

    def get_systems(
        self, num_systems: int | None = None, skip: int = 0
    ) -> tuple[System, ...]:
        num_systems = num_systems or self._num_systems
        systems = []
        for i in range(num_systems):
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
