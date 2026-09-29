#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass, field
from typing import Any

from omnimalloc.primitives import IdType
from omnimalloc.primitives.utils import ensure_unique_ids

from .report import BenchmarkReport


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

    @property
    def num_reports(self) -> int:
        return len(self.reports)

    @property
    def num_results(self) -> int:
        return sum(r.num_results for r in self.reports)

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
