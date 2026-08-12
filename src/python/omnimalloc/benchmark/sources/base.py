#
# SPDX-License-Identifier: Apache-2.0
#

import inspect
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
        ensure_positive(num_allocations, "num_allocations")
        ensure_positive(num_pools, "num_pools")
        ensure_positive(num_memories, "num_memories")
        ensure_positive(num_systems, "num_systems")
        self.num_allocations = num_allocations
        self.num_pools = num_pools
        self.num_memories = num_memories
        self.num_systems = num_systems

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
        so the label carries every field departing from its constructor default,
        even an explicit None: `sync_pattern[num_threads=16]`, `random[seed=None]`.
        """
        parameters = inspect.signature(type(self).__init__).parameters
        no_default = object()
        defaults = {n: p.default for n, p in parameters.items()}
        fields = ",".join(
            f"{f}={getattr(self, f)}"
            for f in self._label_fields
            if getattr(self, f) != defaults.get(f, no_default)
        )
        return f"{self.name()}[{fields}]" if fields else self.name()

    def is_parameterizable(self) -> bool:
        """Whether this source can generate arbitrary allocation counts."""
        return True

    def get_known_optimum(self, variant_id: IdType | None = None) -> int | None:
        """Provably achievable peak size for a variant, or None if unknown.

        Sources that reverse-construct their instances from a packing know
        the optimum and override this; everyone else leaves it unknown.
        """

    def get_available_variants(
        self,
        count: int | None = None,  # noqa: ARG002
    ) -> tuple[str, ...] | None:
        """Variant names offered by a fixed source; None from parameterizable ones.

        `count` is how many the caller needs; a source materializing variants
        lazily may provision at least that many.
        """
        return None

    def get_variant(self, variant_id: IdType) -> Pool:
        """Get a specific pool variant by ID."""

        if isinstance(variant_id, int):
            allocations = self.get_allocations(num_allocations=variant_id)
            if not allocations:
                raise ValueError(f"source {self.name()} returned no allocations")
            return Pool(id=f"{self.name()}_pool_0", allocations=allocations)

        msg = f"Source {self.name()} does not support variant ID: {variant_id}"
        raise ValueError(msg)

    @abstractmethod
    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        """Generate the workload.

        `skip` selects a deterministic alternate stream; whether it continues
        the unskipped stream is source-specific.
        """

    def get_pools(
        self, num_pools: int | None = None, skip: int = 0
    ) -> tuple[Pool, ...]:
        num_pools = self.num_pools if num_pools is None else num_pools
        ensure_positive(num_pools, "num_pools")
        pools = []
        for i in range(num_pools):
            allocations = self.get_allocations(
                num_allocations=self.num_allocations,
                skip=(skip + i) * self.num_allocations,
            )
            if not allocations:
                raise ValueError(f"source {self.name()} returned no allocations")
            pools.append(Pool(id=f"{self.name()}_pool_{i}", allocations=allocations))
        return tuple(pools)

    def get_memories(
        self, num_memories: int | None = None, skip: int = 0
    ) -> tuple[Memory, ...]:
        num_memories = self.num_memories if num_memories is None else num_memories
        ensure_positive(num_memories, "num_memories")
        memories = []
        for i in range(num_memories):
            pools = self.get_pools(
                num_pools=self.num_pools,
                skip=(skip + i) * self.num_pools,
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
        num_systems = self.num_systems if num_systems is None else num_systems
        ensure_positive(num_systems, "num_systems")
        systems = []
        for i in range(num_systems):
            memories = self.get_memories(
                num_memories=self.num_memories,
                skip=(skip + i) * self.num_memories,
            )
            if not memories:
                raise ValueError(f"source {self.name()} returned no memories")
            systems.append(System(id=f"{self.name()}_system_{i}", memories=memories))
        return tuple(systems)

    def get_allocation(self) -> Allocation:
        allocations = self.get_allocations(num_allocations=1)
        if not allocations:
            raise ValueError(f"source {self.name()} returned no allocations")
        return allocations[0]

    def get_pool(self) -> Pool:
        pools = self.get_pools(num_pools=1)
        if not pools:
            raise ValueError(f"source {self.name()} returned no pools")
        return pools[0]

    def get_memory(self) -> Memory:
        memories = self.get_memories(num_memories=1)
        return memories[0]

    def get_system(self) -> System:
        systems = self.get_systems(num_systems=1)
        return systems[0]
