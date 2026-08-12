#
# SPDX-License-Identifier: Apache-2.0
#

from types import ModuleType

import omnimalloc
import omnimalloc.analysis
import omnimalloc.benchmark
from omnimalloc.allocators import BaseAllocator

TOP_LEVEL_API = {
    "Allocation",
    "AllocationKind",
    "IdType",
    "Memory",
    "Pool",
    "System",
    "TimePoint",
    "VectorClock",
    "allocate",
    "available_allocators",
    "max_threads",
    "plot_allocation",
    "set_max_threads",
    "validate_allocation",
}

ANALYSIS_API = {
    "ConflictGraph",
    "antichain_pressure",
    "antichain_pressure_per_allocation",
    "closure_pressure",
    "closure_pressure_per_allocation",
    "conflict_degrees",
    "conflict_graph",
    "conflicts",
    "placement_pressure",
    "placement_pressure_per_allocation",
    "try_linearize",
}

BENCHMARK_API = {
    "BaseSource",
    "BenchmarkCampaign",
    "BenchmarkReport",
    "BenchmarkResult",
    "ConcurrentTilingSource",
    "DEFAULT_SOURCE",
    "HighContentionSource",
    "HuggingfaceSource",
    "MinimallocSource",
    "MinimallocSubset",
    "PinwheelSource",
    "PowerOf2Source",
    "RandomSource",
    "SIZE_DISTRIBUTIONS",
    "SYNC_PATTERNS",
    "SequentialSource",
    "SizeDistribution",
    "SkewedSource",
    "SyncPattern",
    "SyncPatternSource",
    "TilingSource",
    "TwoPlusTwoSource",
    "UniformSource",
    "VariantSpec",
    "available_sources",
    "plot_benchmark",
    "run_benchmark",
    "save_benchmark",
}


def _public_names(module: ModuleType) -> set[str]:
    return {
        name
        for name, value in vars(module).items()
        if not name.startswith("_") and not isinstance(value, ModuleType)
    }


def test_top_level_api_is_pinned() -> None:
    assert _public_names(omnimalloc) == TOP_LEVEL_API


def test_analysis_api_is_pinned() -> None:
    assert _public_names(omnimalloc.analysis) == ANALYSIS_API


def test_benchmark_api_is_pinned() -> None:
    assert _public_names(omnimalloc.benchmark) == BENCHMARK_API


def test_version_is_exposed() -> None:
    assert omnimalloc.__version__


def test_allocate_resolves_every_advertised_allocator_name() -> None:
    for name in omnimalloc.available_allocators():
        assert isinstance(BaseAllocator.get(name), type)
