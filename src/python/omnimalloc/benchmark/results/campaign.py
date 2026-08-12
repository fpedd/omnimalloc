#
# SPDX-License-Identifier: Apache-2.0
#

import copy
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from omnimalloc.primitives import IdType
from omnimalloc.primitives.utils import ensure_unique_ids

from .report import BenchmarkReport
from .utils import get_environment_metadata


@dataclass(frozen=True)
class BenchmarkCampaign:
    """A collection of benchmark reports."""

    id: IdType
    reports: tuple[BenchmarkReport, ...]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.reports:
            raise ValueError("BenchmarkCampaign must contain at least one report")
        ensure_unique_ids(self.reports, "report")
        try:
            object.__setattr__(self, "metadata", copy.deepcopy(self.metadata))
        except TypeError as error:
            raise TypeError(
                f"Campaign metadata must be deep-copyable: {error}"
            ) from error

    @property
    def num_reports(self) -> int:
        return len(self.reports)

    @property
    def num_results(self) -> int:
        return sum(r.num_results for r in self.reports)

    @property
    def num_results_per_report(self) -> float:
        return self.num_results / self.num_reports

    @property
    def num_allocations(self) -> int:
        return sum(r.total_num_allocations for r in self.reports)

    @property
    def num_allocations_per_report(self) -> float:
        return self.num_allocations / self.num_reports

    @property
    def num_allocations_per_result(self) -> float:
        return self.num_allocations / self.num_results

    @property
    def num_allocators(self) -> int:
        return len(self.allocator_names)

    @property
    def num_sources(self) -> int:
        return len(self.source_names)

    @property
    def allocator_names(self) -> tuple[str, ...]:
        return tuple(sorted({r.allocator_name for r in self.reports}))

    @property
    def source_names(self) -> tuple[str, ...]:
        return tuple(sorted({r.source_name for r in self.reports}))

    @property
    def reports_by_source_allocator_variant(
        self,
    ) -> dict[str, dict[str, dict[str, tuple[BenchmarkReport, ...]]]]:
        """Group reports by source -> allocator -> variant_label."""

        result: dict[str, dict[str, dict[str, list[BenchmarkReport]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for report in self.reports:
            source = report.source_name
            allocator = report.allocator_name
            variant = report.variant_label
            result[source][allocator][variant].append(report)

        return {
            source: {
                alloc: {
                    variant: tuple(reports)
                    for variant, reports in sorted(variants.items())
                }
                for alloc, variants in sorted(allocs.items())
            }
            for source, allocs in sorted(result.items())
        }

    @property
    def default_metadata(self) -> dict[str, Any]:
        """Environment plus campaign shape; `metadata` wins on a key clash."""
        return get_environment_metadata() | {
            "num_reports": self.num_reports,
            "num_results_per_report": round(self.num_results_per_report, 2),
            "num_results_total": self.num_results,
            "num_allocations_per_report": round(self.num_allocations_per_report, 2),
            "num_allocations_per_result": round(self.num_allocations_per_result, 2),
            "num_allocations_total": self.num_allocations,
            "num_allocators": self.num_allocators,
            "num_sources": self.num_sources,
        }

    def finalize_metadata(self) -> "BenchmarkCampaign":
        m = self.default_metadata | self.metadata
        return BenchmarkCampaign(id=self.id, reports=self.reports, metadata=m)
