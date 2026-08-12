#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from dataclasses import asdict, dataclass

from omnimalloc import allocate, validate_allocation
from omnimalloc.allocators import BaseAllocator, available_allocators
from omnimalloc.common.validation import ensure_positive
from omnimalloc.primitives import IdType, Pool

from .results import BenchmarkCampaign, BenchmarkReport, BenchmarkResult
from .results.utils import get_date_time_snake_case
from .sources import DEFAULT_SOURCE, BaseSource
from .timer import Timer
from .utils import tqdm

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SkippedAllocator:
    """An allocator left out of a campaign, with the reason it was skipped."""

    source: str
    allocator: str
    reason: str


@dataclass(frozen=True)
class SkippedVariant:
    """A workload a source could not express, with the reason it was skipped."""

    source: str
    variant: str
    reason: str


def _resolve_parameterizable_variants(
    source: BaseSource, variants: int | tuple[IdType, ...] | None
) -> tuple[int, ...]:
    if variants is None:
        return (source.num_allocations,)
    if isinstance(variants, int):
        return (variants,)
    resolved_variants = []
    for v in variants:
        if not isinstance(v, int):
            raise TypeError(
                f"Non-integer variant {v!r} for parameterizable source {source.name()}"
            )
        resolved_variants.append(v)
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
    for v in variants:
        if isinstance(v, str) and v in available:
            resolved_variants.append(v)
        # Int variants index into the available variants
        elif isinstance(v, int) and 0 <= v < len(available):
            resolved_variants.append(available[v])
        else:
            raise ValueError(f"Unknown variant {v!r} for source {source.name()}")
    return tuple(resolved_variants)


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


def _get_variant_ids(
    source_inst: BaseSource,
    variants: VariantSpec | dict[str, VariantSpec],
) -> tuple[IdType, ...]:
    if isinstance(variants, dict):
        # Labelled instances can be addressed individually; the class name
        # keeps working and covers every instance of that source
        label = source_inst.label()
        variants = (
            variants[label] if label in variants else variants.get(source_inst.name())
        )
    if source_inst.is_parameterizable():
        return _resolve_parameterizable_variants(source_inst, variants)
    return _resolve_fixed_variants(source_inst, variants)


def _benchmark_result(
    allocator: BaseAllocator,
    source: BaseSource,
    pool: Pool,
    result_id: IdType,
    validate: bool,
) -> BenchmarkResult:
    with Timer() as timer:
        allocated_pool = allocate(pool, allocator, validate=False)

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
    known_optima: dict[IdType, int | None],
) -> BenchmarkReport | SkippedAllocator | SkippedVariant:
    """Time one allocator/source/variant, or report why it was skipped."""
    variant_desc = variant_id if isinstance(variant_id, str) else f"{variant_id} allocs"

    # Validate and error out early; a variant the source cannot express
    # (e.g. fewer allocations than threads) skips instead of aborting the
    # whole campaign
    try:
        pool = source.get_variant(variant_id)
    except ValueError as error:
        logger.warning(f"Skipping {source.label()}[{variant_desc}]: {error}")
        return SkippedVariant(
            source=source.label(), variant=str(variant_id), reason=str(error)
        )
    if pool is None:
        raise ValueError(f"source {source.name()} returned no pool")
    try:
        allocator.ensure_supported(pool.allocations)
    except ValueError as error:
        logger.warning(
            f"Skipping {allocator.name()} on {source.label()}[{variant_desc}]: {error}"
        )
        return SkippedAllocator(
            source=source.label(),
            allocator=allocator.name(),
            reason=str(error),
        )

    results = []
    for _ in tqdm(
        range(iterations),
        desc=f"Iterations [{variant_desc}]",
        position=3,
        leave=False,
    ):
        result = _benchmark_result(allocator, source, pool, result_id, validate)
        results.append(result)
        result_id += 1

    # The ground truth is a property of the instance, not the allocator, and
    # the tiling sources rebuild their whole construction to read it
    if variant_id not in known_optima:
        known_optima[variant_id] = source.get_known_optimum(variant_id)

    return BenchmarkReport(
        id=report_id,
        results=tuple(results),
        allocator=allocator,
        source=source,
        variant_id=variant_id,
        known_optimum=known_optima[variant_id],
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
    allocators = allocators or available_allocators()
    sources = sources or (DEFAULT_SOURCE,)
    source_insts = tuple(BaseSource.resolve(source) for source in sources)
    _ensure_known_variant_keys(source_insts, variants)
    campaign_id = campaign_id or "campaign_" + get_date_time_snake_case()

    reports = []
    skipped: list[SkippedAllocator] = []
    skipped_variants: list[SkippedVariant] = []
    report_id = 0
    result_id = 0

    timer = Timer()
    timer.start()

    for source_inst in tqdm(
        source_insts,
        desc="Sources",
        position=0,
        leave=False,
    ):
        if getattr(source_inst, "seed", 0) is None:
            logger.warning(
                f"Source {source_inst.name()} has seed=None; each allocator "
                f"gets a different random problem, so results are not comparable"
            )

        variant_ids = _get_variant_ids(source_inst, variants)
        known_optima: dict[IdType, int | None] = {}

        for allocator in tqdm(
            allocators,
            desc=f"Allocators [{source_inst.label()}]",
            position=1,
            leave=False,
        ):
            # An allocator wrapping an uninstalled library is a skip, not an
            # abort: `available_allocators()` lists every registered name, so
            # the default campaign would otherwise die on the first optional one
            try:
                allocator_inst = BaseAllocator.resolve(allocator)
            except ImportError as error:
                reason = str(error).splitlines()[0]
                name = allocator if isinstance(allocator, str) else allocator.name()
                logger.warning(f"Skipping {name} on {source_inst.label()}: {reason}")
                skipped.append(
                    SkippedAllocator(
                        source=source_inst.label(), allocator=name, reason=reason
                    )
                )
                continue

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
                    known_optima,
                )
                if isinstance(report, SkippedAllocator):
                    skipped.append(report)
                    continue
                if isinstance(report, SkippedVariant):
                    skipped_variants.append(report)
                    continue
                reports.append(report)
                report_id += 1
                result_id += iterations

    timer.stop()

    if not reports:
        raise ValueError(
            "No benchmark reports produced; every allocator/source/variant "
            "combination was skipped or empty. Double-check your setup."
        )

    campaign = BenchmarkCampaign(
        id=campaign_id,
        reports=tuple(reports),
        metadata={
            "total_duration": timer.elapsed,
            # Same allocator and source repeat once per variant, and a dropped
            # variant repeats once per allocator; report the distinct omissions
            # so a shrunken comparison is visible
            "skipped_allocators": [asdict(s) for s in dict.fromkeys(skipped)],
            "skipped_variants": [asdict(s) for s in dict.fromkeys(skipped_variants)],
        },
    )
    campaign = campaign.finalize_metadata()
    return campaign
