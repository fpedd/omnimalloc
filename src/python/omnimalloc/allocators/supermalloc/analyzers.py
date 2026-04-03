#
# SPDX-License-Identifier: Apache-2.0
#

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from functools import cache, cached_property
from itertools import pairwise

from omnimalloc.primitives.allocation import Allocation, IdType


class EventKind(Enum):
    ENTER = 1
    EXIT = 0  # processed first at equal timestamps


@dataclass(frozen=True)
class Event:
    allocation: Allocation
    kind: EventKind

    def __post_init__(self) -> None:
        if self.allocation.start >= self.allocation.end:
            raise ValueError(f"Allocation requires start < end: {self.allocation}")

    @cached_property
    def id(self) -> IdType:
        return self.allocation.id

    @cached_property
    def time(self) -> int:
        if self.kind == EventKind.ENTER:
            return self.allocation.start
        return self.allocation.end

    @cached_property
    def sort_key(self) -> tuple[int, int, IdType]:
        return (self.time, self.kind.value, self.id)


@dataclass(frozen=True)
class Section:
    allocations: tuple[Allocation]

    @cached_property
    def ids(self) -> tuple[IdType]:
        return tuple(alloc.id for alloc in self.allocations)

    @cache
    def overlaps(self, other: "Section") -> bool:
        return bool(self.allocations & other.allocations)


@dataclass(frozen=True)
class Partition:
    sections: tuple[Section]
    offset: int | None = None

    @cached_property
    def allocations(self) -> tuple[Allocation]:
        return tuple(a for s in self.sections for a in s.allocations)

    @cached_property
    def ids(self) -> tuple[IdType]:
        return tuple(alloc.id for alloc in self.allocations)

    @cached_property
    def lower_bound(self) -> int:
        return max(
            sum(alloc.size for alloc in section.allocations)
            for section in self.sections
        )


def get_events(
    allocations: tuple[Allocation, ...], sort: bool = True
) -> tuple[Event, ...]:
    events: list[Event] = []
    for alloc in allocations:
        events.append(Event(allocation=alloc, kind=EventKind.ENTER))
        events.append(Event(allocation=alloc, kind=EventKind.EXIT))
    if sort:
        events.sort(key=lambda e: e.sort_key)
    return tuple(events)


def get_sections(allocations: tuple[Allocation, ...]) -> tuple[Section, ...]:
    if not allocations:
        return ()

    events = get_events(allocations, sort=True)

    sections: list[Section] = []
    active: set[Allocation] = set()
    last_section_time: int = events[0].time

    for event in events:
        if event.kind == EventKind.EXIT:
            if last_section_time < event.time:  # Prevents duplicate sections
                last_section_time = event.time
                sections.append(Section(allocations=tuple(active)))
            active.discard(event.allocation)

        else:
            active.add(event.allocation)

    return tuple(sections)


def get_partitions(allocations: tuple[Allocation, ...]) -> tuple[Partition, ...]:
    sections = get_sections(allocations)
    if not sections:
        return ()

    partitions: list[Partition] = []
    current: list[Section] = [sections[0]]

    for prev, section in pairwise(sections):
        if not prev.overlaps(section):
            partitions.append(Partition(sections=tuple(current)))
            current = []
        current.append(section)

    partitions.append(Partition(sections=tuple(current)))
    return tuple(partitions)


def get_overlaps(
    allocations: tuple[Allocation, ...],
) -> dict[Allocation, tuple[Allocation]]:



def get_overlap_ids(allocations: tuple[Allocation, ...]) -> dict[IdType, set[IdType]]:
    return {
        alloc.id: {other.id for other in others}
        for alloc, others in get_overlaps(allocations).items()
    }
