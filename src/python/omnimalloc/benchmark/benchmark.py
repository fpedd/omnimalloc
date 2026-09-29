#
# SPDX-License-Identifier: Apache-2.0
#

import logging

from omnimalloc import allocate, validate_allocation
from omnimalloc.allocators import BaseAllocator, available_allocators
from omnimalloc.common.validation import ensure_positive
from omnimalloc.primitives import IdType, Pool

from .results import BenchmarkCampaign, BenchmarkReport, BenchmarkResult
from .results.utils import get_date_time_snake_case, get_environment_metadata
from .sources import DEFAULT_SOURCE, BaseSource
from .timer import Timer
from .utils import tqdm

logger = logging.getLogger(__name__)

VariantSpec = int | tuple[IdType, ...] | None


def _ensure_known_variant_keys(
    sources: tuple[BaseSource, ...],
    variants: VariantSpec | dict[str, VariantSpec],
) -> None:
    if not isinstance(variants, dict):
        return
    known = {s.label() for s in sources} | {s.name() for s in sources}
    unknown = sorted(set(variants) - known)
    if unknown:
        raise ValueError(f"Variants keys {unknown} match no source in this campaign")


def _parameterizable_variants(
    source: BaseSource, variants: VariantSpec
) -> tuple[IdType, ...]:
    if variants is None:
        return (source.num_allocations,)
    if isinstance(variants, int):
        return (variants,)
    for v in variants:
        if not isinstance(v, int):
            raise TypeError(
                f"Non-integer variant {v!r} for parameterizable source {source.name()}"
            )
    return variants


def _fixed_variants(source: BaseSource, variants: VariantSpec) -> tuple[str, ...]:
    available = source.get_available_variants() or ()
    if variants is None:
        return available
    if isinstance(variants, int):
        return available[:variants]
    resolved = []
    for v in variants:
        if isinstance(v, str) and v in available:
            resolved.append(v)
        # Int variants index into the available variants
        elif isinstance(v, int) and 0 <= v < len(available):
            resolved.append(available[v])
        else:
            raise ValueError(f"Unknown variant {v!r} for source {source.name()}")
    return tuple(resolved)


def _get_variant_ids(
    source: BaseSource,
    variants: VariantSpec | dict[str, VariantSpec],
) -> tuple[IdType, ...]:
    if isinstance(variants, dict):
        # Labelled instances can be addressed individually; the class name
        # keeps working and covers every instance of that source
        label = source.label()
        variants = variants[label] if label in variants else variants.get(source.name())
    if source.is_parameterizable():
        return _parameterizable_variants(source, variants)
    return _fixed_variants(source, variants)


def _resolve_allocators(
    allocators: tuple[BaseAllocator | type[BaseAllocator] | str, ...],
    skipped: list[dict[str, str]],
) -> list[BaseAllocator]:
    # An allocator wrapping an uninstalled library is a skip, not an abort:
    # `available_allocators()` lists every registered name, so the default
    # campaign would otherwise die on the first optional one
    resolved = []
    for allocator in allocators:
        try:
            resolved.append(BaseAllocator.resolve(allocator))
        except ImportError as error:
            name = allocator if isinstance(allocator, str) else allocator.name()
            _skip(skipped, str(error).splitlines()[0], allocator=name)
    return resolved


def _skip(skipped: list[dict[str, str]], reason: str, **where: str) -> None:
    """Record a combination left out, so a shrunken comparison is visible."""
    logger.warning(f"Skipping {'/'.join(where.values())}: {reason}")
    skipped.append(where | {"reason": reason})


def _benchmark_report(
    report_id: int,
    allocator: BaseAllocator,
    source: BaseSource,
    variant_id: IdType,
    pool: Pool,
    iterations: int,
    validate: bool,
) -> BenchmarkReport:
    results = []
    for i in tqdm(
        range(iterations),
        desc=f"Iterations [{allocator.name()}]",
        position=3,
        leave=False,
    ):
        # Validation runs outside the timer: it is quadratic and would skew timings
        with Timer() as timer:
            allocated_pool = allocate(pool, allocator, validate=False)
        if validate:
            validate_allocation(allocated_pool)
        results.append(
            BenchmarkResult(
                id=i,
                allocator=allocator,
                source=source,
                entity=allocated_pool,
                duration=timer.elapsed_s,
            )
        )
    return BenchmarkReport(
        id=report_id,
        results=tuple(results),
        allocator=allocator,
        source=source,
        variant_id=variant_id,
        known_optimum=source.get_known_optimum(),
    )


def run_benchmark(
    allocators: tuple[BaseAllocator | type[BaseAllocator] | str, ...] | None = None,
    sources: tuple[BaseSource | type[BaseSource] | str, ...] | None = None,
    variants: VariantSpec | dict[str, VariantSpec] = None,
    campaign_id: IdType | None = None,
    iterations: int = 1,
    validate: bool = True,
) -> BenchmarkCampaign:
    """Run a benchmark campaign across multiple allocators and sources.

    `variants` selects workloads per source: counts, names, indices, or a dict
    keyed by source. `iterations` re-runs one instance, measuring jitter.
    Unlike `allocate`, `validate` defaults to True here.
    """
    ensure_positive(iterations, "iterations")
    source_insts = tuple(BaseSource.resolve(s) for s in sources or (DEFAULT_SOURCE,))
    _ensure_known_variant_keys(source_insts, variants)
    if campaign_id is None:
        campaign_id = "campaign_" + get_date_time_snake_case()

    skipped: list[dict[str, str]] = []
    allocator_insts = _resolve_allocators(allocators or available_allocators(), skipped)
    reports: list[BenchmarkReport] = []

    with Timer() as timer:
        for source in tqdm(source_insts, desc="Sources", position=0, leave=False):
            label = source.label()
            if getattr(source, "seed", 0) is None:
                logger.warning(
                    f"Source {label} has seed=None; each allocator gets a "
                    f"different random problem, so results are not comparable"
                )
            for variant_id in tqdm(
                _get_variant_ids(source, variants),
                desc=f"Variants [{label}]",
                position=1,
                leave=False,
            ):
                # A variant the source cannot express (e.g. fewer allocations
                # than threads) skips instead of aborting the whole campaign
                try:
                    pool = source.get_variant(variant_id)
                except ValueError as error:
                    _skip(skipped, str(error), source=label, variant=str(variant_id))
                    continue
                for allocator in tqdm(
                    allocator_insts,
                    desc=f"Allocators [{variant_id}]",
                    position=2,
                    leave=False,
                ):
                    try:
                        allocator.ensure_supported(pool.allocations)
                    except ValueError as error:
                        _skip(
                            skipped,
                            str(error),
                            source=label,
                            variant=str(variant_id),
                            allocator=allocator.name(),
                        )
                        continue
                    reports.append(
                        _benchmark_report(
                            len(reports),
                            allocator,
                            source,
                            variant_id,
                            pool,
                            iterations,
                            validate,
                        )
                    )

    if not reports:
        raise ValueError(
            "No benchmark reports produced; every allocator/source/variant "
            "combination was skipped or empty. Double-check your setup."
        )

    metadata = get_environment_metadata() | {
        "total_duration": f"{timer.elapsed_s:.2f} s",
        "num_reports": len(reports),
        "num_results": len(reports) * iterations,
        "skipped": skipped,
    }
    return BenchmarkCampaign(id=campaign_id, reports=tuple(reports), metadata=metadata)
