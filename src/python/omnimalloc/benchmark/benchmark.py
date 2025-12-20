#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from typing import Any

from omnimalloc import run_allocation, validate_allocation
from omnimalloc.allocators import BaseAllocator, get_available_allocators
from omnimalloc.primitives import IdType

from .results import BenchmarkCampaign, BenchmarkReport, BenchmarkResult
from .results.utils import get_date_time_snake_case
from .sources import BaseSource, get_default_source
from .timer import Timer

logger = logging.getLogger(__name__)

try:
    from tqdm.auto import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

    # Fallback: tqdm without progress bars (just returns iterable)
    def tqdm(iterable: Any, **kwargs: Any) -> Any:  # noqa: ARG001, ANN401
        """No-op tqdm fallback when tqdm is not installed."""
        return iterable


def _resolve_allocator(
    allocator: BaseAllocator | type[BaseAllocator] | str,
) -> BaseAllocator:
    if isinstance(allocator, str):
        return BaseAllocator.get(allocator)()
    if isinstance(allocator, type):
        return allocator()
    return allocator


def _resolve_source(source: BaseSource | type[BaseSource] | str) -> BaseSource:
    if isinstance(source, str):
        return BaseSource.get(source)()
    if isinstance(source, type):
        return source()
    return source


def _resolve_parameterizable_variants(
    variants: int | tuple[IdType, ...] | None,
) -> tuple[int, ...]:
    if variants is None:
        return (100,)
    if isinstance(variants, int):
        return (variants,)
    resolved_variants = []
    for v in variants:
        if isinstance(v, int):
            resolved_variants.append(v)
        else:
            logger.warning(
                f"Skipping non-integer variant {v!r} for parameterizable source"
            )
    return tuple(resolved_variants)


def _resolve_fixed_variants(
    source: BaseSource, variants: int | tuple[IdType, ...] | None
) -> tuple[str, ...]:
    variant_count = (
        variants if isinstance(variants, int) else len(variants) if variants else None
    )
    available = source.get_available_variants(variant_count)
    if available is None:
        return ()
    if variants is None:
        return available
    if isinstance(variants, int):
        return available[:variants]
    resolved_variants = []
    for i, v in enumerate(variants):
        if isinstance(v, str) and v in available:
            resolved_variants.append(v)
        # TODO(fpedd): Might want to treat int variants as indices?
        elif isinstance(v, int):
            resolved_variants.append(available[i])
        else:
            logger.warning(f"Skipping unknown variant {v!r} for source {source.name()}")
    return tuple(resolved_variants)


def _get_variant_ids(
    source_inst: BaseSource,
    variants: int | tuple[IdType, ...] | None,
) -> tuple[IdType, ...]:
    if source_inst.is_parameterizable():
        return _resolve_parameterizable_variants(variants)
    return _resolve_fixed_variants(source_inst, variants)


def _benchmark_result(
    allocator: BaseAllocator,
    source: BaseSource,
    variant_id: IdType,
    result_id: IdType,
    validate: bool,
) -> BenchmarkResult:
    pool = source.get_variant(variant_id)
    if pool is None:
        raise ValueError(f"source {source.name()} returned no pool")

    with Timer() as timer:
        allocated_pool = run_allocation(pool, allocator, validate=False)

    if validate:
        validate_allocation(allocated_pool)

    return BenchmarkResult(
        id=result_id,
        allocator=allocator,
        source=source,
        entity=allocated_pool,
        duration=timer.elapsed_s,
    )


def _benchmark_report(
    allocator: BaseAllocator,
    source: BaseSource,
    iterations: int,
    variant_id: IdType,
    report_id: int,
    result_id: int,
    validate: bool,
) -> BenchmarkReport:
    results = []
    variant_desc = variant_id if isinstance(variant_id, str) else f"{variant_id} allocs"
    for _ in tqdm(
        range(iterations),
        desc=f"Iterations [{variant_desc}]",
        position=3,
        leave=False,
    ):
        result = _benchmark_result(allocator, source, variant_id, result_id, validate)
        results.append(result)
        result_id += 1

    return BenchmarkReport(
        id=report_id,
        results=tuple(results),
        allocator=allocator,
        source=source,
        variant_id=variant_id,
    )


def run_benchmark(
    allocators: tuple[BaseAllocator | type[BaseAllocator] | str, ...] | None = None,
    sources: tuple[BaseSource | type[BaseSource] | str, ...] | None = None,
    variants: int | tuple[IdType, ...] | None = None,
    campaign_id: IdType | None = None,
    iterations: int = 1,
    validate: bool = False,
) -> BenchmarkCampaign:
    """Run a benchmark campaign across multiple allocators and sources.

    Args:
        allocators: Allocators to benchmark (defaults to all available).
        sources: Sources to benchmark (defaults to default source).
        variants: For parameterizable sources, specifies allocation counts
                 (int or tuple of ints). For fixed sources, specifies which
                 models/pools to test (tuple of names, or int for "first N",
                 or None for all). Examples:
                 - 100: Test with 100 allocations (parameterizable only)
                 - (10, 100, 1000): Test with multiple sizes (parameterizable)
                 - ("model1", "model2"): Test specific models (fixed sources)
                 - 5: Test first 5 models (fixed sources)
                 - None: Use defaults (all models for fixed, 100 for parameterizable)
        campaign_id: Unique identifier for this campaign.
        iterations: Number of iterations per variant (for statistical averaging).
        validate: Whether to validate allocations after running.

    Returns:
        BenchmarkCampaign containing all reports.
    """
    allocators = allocators or get_available_allocators()
    sources = sources or (get_default_source(),)
    campaign_id = campaign_id or "campaign_" + get_date_time_snake_case()

    reports = []
    report_id = 0
    result_id = 0

    timer = Timer()
    timer.start()

    for source in tqdm(
        sources,
        desc="Sources",
        position=0,
        leave=False,
    ):
        source_inst = _resolve_source(source)

        for allocator in tqdm(
            allocators,
            desc=f"Allocators [{source}]",
            position=1,
            leave=False,
        ):
            allocator_inst = _resolve_allocator(allocator)
            variant_ids = _get_variant_ids(source_inst, variants)

            for variant_id in tqdm(
                variant_ids,
                desc=f"Variants [{allocator}]",
                position=2,
                leave=False,
            ):
                report = _benchmark_report(
                    allocator_inst,
                    source_inst,
                    iterations,
                    variant_id,
                    report_id,
                    result_id,
                    validate,
                )
                reports.append(report)
                report_id += 1
                result_id += iterations

    timer.stop()

    campaign = BenchmarkCampaign(
        id=campaign_id,
        reports=tuple(reports),
        metadata={"total_duration": timer.elapsed},
    )
    campaign = campaign.finalize_metadata()
    return campaign


benchmark_campaign = run_benchmark
