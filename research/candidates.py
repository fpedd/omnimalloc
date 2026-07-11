#
# SPDX-License-Identifier: Apache-2.0
#
"""Prototype O(N log N) allocator candidates.

All candidates take a list of (size, start, end) tuples and return a list of
offsets aligned with the input order. Pure Python, optimized for clarity.
"""

import heapq
import random
from bisect import bisect_left, bisect_right

# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------


class SegTree:
    """Segment tree over time slots: range max query + range assign update.

    Range assign is only valid when the assigned value is >= the current max
    over the range (true for skyline placement: offset+size >= watermark).
    """

    def __init__(self, n: int) -> None:
        self.n = n
        self.max = [0] * (4 * n)
        self.lazy = [None] * (4 * n)

    def _push(self, node: int) -> None:
        val = self.lazy[node]
        if val is not None:
            for child in (2 * node, 2 * node + 1):
                self.max[child] = val
                self.lazy[child] = val
            self.lazy[node] = None

    def query(
        self, lo: int, hi: int, node: int = 1, nlo: int = 0, nhi: int | None = None
    ) -> int:
        if nhi is None:
            nhi = self.n
        if hi <= nlo or nhi <= lo:
            return 0
        if lo <= nlo and nhi <= hi:
            return self.max[node]
        self._push(node)
        mid = (nlo + nhi) // 2
        return max(
            self.query(lo, hi, 2 * node, nlo, mid),
            self.query(lo, hi, 2 * node + 1, mid, nhi),
        )

    def assign(
        self,
        lo: int,
        hi: int,
        val: int,
        node: int = 1,
        nlo: int = 0,
        nhi: int | None = None,
    ) -> None:
        if nhi is None:
            nhi = self.n
        if hi <= nlo or nhi <= lo:
            return
        if lo <= nlo and nhi <= hi:
            self.max[node] = val
            self.lazy[node] = val
            return
        self._push(node)
        mid = (nlo + nhi) // 2
        self.assign(lo, hi, val, 2 * node, nlo, mid)
        self.assign(lo, hi, val, 2 * node + 1, mid, nhi)
        self.max[node] = max(self.max[2 * node], self.max[2 * node + 1])


def compress_times(allocs):
    """Map starts/ends to slot indices. Returns (slot_starts, slot_ends, n_slots)."""
    times = sorted({t for size, start, end in allocs for t in (start, end)})
    index = {t: i for i, t in enumerate(times)}
    starts = [index[a[1]] for a in allocs]
    ends = [index[a[2]] for a in allocs]
    return starts, ends, len(times)


def skyline_place(allocs, order):
    """Place allocations in the given index order at the top of the skyline."""
    starts, ends, n_slots = compress_times(allocs)
    tree = SegTree(max(n_slots, 1))
    offsets = [0] * len(allocs)
    for i in order:
        offset = tree.query(starts[i], ends[i])
        offsets[i] = offset
        tree.assign(starts[i], ends[i], offset + allocs[i][0])
    return offsets


def compute_conflicts(allocs):
    """Number of temporally overlapping allocations, via sweep. O(N log N)."""
    starts = sorted(a[1] for a in allocs)
    ends = sorted(a[2] for a in allocs)
    return [
        bisect_left(starts, end) - bisect_right(ends, start) - 1
        for size, start, end in allocs
    ]


def lower_bound(allocs):
    """Max over time of the sum of active sizes."""
    events = []
    for size, start, end in allocs:
        events.append((start, size))
        events.append((end, -size))
    events.sort()
    peak = active = 0
    for _, delta in events:
        active += delta
        peak = max(peak, active)
    return peak


def peak_of(allocs, offsets):
    return max((o + a[0] for a, o in zip(allocs, offsets, strict=False)), default=0)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------


def order_indices(allocs, key, reverse=True):
    return sorted(range(len(allocs)), key=key, reverse=reverse)


ORDERS = {
    "size": lambda allocs: order_indices(allocs, lambda i: allocs[i][0]),
    "duration": lambda allocs: order_indices(
        allocs, lambda i: allocs[i][2] - allocs[i][1]
    ),
    "area": lambda allocs: order_indices(
        allocs, lambda i: allocs[i][0] * (allocs[i][2] - allocs[i][1])
    ),
    "start": lambda allocs: sorted(
        range(len(allocs)), key=lambda i: (allocs[i][1], -allocs[i][0])
    ),
}


def conflict_orders(allocs):
    conflicts = compute_conflicts(allocs)
    return {
        "conflict": order_indices(allocs, lambda i: (conflicts[i], allocs[i][0])),
        "conflict_size": order_indices(
            allocs, lambda i: (conflicts[i] * allocs[i][0], allocs[i][0])
        ),
    }


def all_orders(allocs):
    orders = {name: fn(allocs) for name, fn in ORDERS.items()}
    orders.update(conflict_orders(allocs))
    orders["input"] = list(range(len(allocs)))
    return orders


# ---------------------------------------------------------------------------
# Compaction (monotone improvement pass)
# ---------------------------------------------------------------------------


def compact(allocs, offsets, max_passes=10):
    """Re-place in ascending-offset order; peak never increases. Iterate."""
    for _ in range(max_passes):
        order = sorted(range(len(allocs)), key=lambda i: (offsets[i], allocs[i][1]))
        new_offsets = skyline_place(allocs, order)
        if new_offsets == offsets:
            break
        offsets = new_offsets
    return offsets


# ---------------------------------------------------------------------------
# Candidate allocators
# ---------------------------------------------------------------------------


def skyline_by(name):
    def place(allocs):
        order = all_orders(allocs)[name]
        return skyline_place(allocs, order)

    place.__name__ = f"skyline_{name}"
    return place


def skyline_by_all(allocs):
    best = None
    for order in all_orders(allocs).values():
        offsets = skyline_place(allocs, order)
        if best is None or peak_of(allocs, offsets) < peak_of(allocs, best):
            best = offsets
    return best


def skyline_by_all_compact(allocs):
    best = None
    for order in all_orders(allocs).values():
        offsets = compact(allocs, skyline_place(allocs, order))
        if best is None or peak_of(allocs, offsets) < peak_of(allocs, best):
            best = offsets
    return best


def sweep_fit(allocs, best_fit=False):
    """Chronological sweep with an address-ordered free list (true gap reuse).

    At each start event (larger sizes first on ties) allocate from the free
    list (first-fit or best-fit); at each end event free and coalesce.
    """
    n = len(allocs)
    events = []
    for i, (size, start, end) in enumerate(allocs):
        events.append((start, 0, -size, i))
        events.append((end, -1, 0, i))
    # Ends processed before starts at the same time (half-open intervals).
    events.sort()

    INF = float("inf")
    free_offsets = [0]  # address-ordered gap start offsets
    free_sizes = [INF]  # parallel gap lengths; last gap is unbounded
    offsets = [0] * n

    for _, kind, _, i in events:
        size = allocs[i][0]
        if kind == -1:  # free event
            offset = offsets[i]
            pos = bisect_left(free_offsets, offset)
            # Coalesce with successor gap
            if pos < len(free_offsets) and free_offsets[pos] == offset + size:
                size += free_sizes[pos]
                del free_offsets[pos], free_sizes[pos]
            # Coalesce with predecessor gap
            if pos > 0 and free_offsets[pos - 1] + free_sizes[pos - 1] == offset:
                free_sizes[pos - 1] += size
            else:
                free_offsets.insert(pos, offset)
                free_sizes.insert(pos, size)
        else:  # allocation event
            if best_fit:
                pos = min(
                    (p for p in range(len(free_offsets)) if free_sizes[p] >= size),
                    key=lambda p: free_sizes[p],
                )
            else:
                pos = next(p for p in range(len(free_offsets)) if free_sizes[p] >= size)
            offsets[i] = free_offsets[pos]
            if free_sizes[pos] == size:
                del free_offsets[pos], free_sizes[pos]
            else:
                free_offsets[pos] += size
                free_sizes[pos] -= size
    return offsets


def sweep_first_fit(allocs):
    return sweep_fit(allocs, best_fit=False)


def sweep_best_fit(allocs):
    return sweep_fit(allocs, best_fit=True)


def validate(allocs, offsets):
    """Check no temporal+spatial overlap via sweep over active offset intervals."""
    events = []
    for i, (size, start, end) in enumerate(allocs):
        events.append((end, 0, i))
        events.append((start, 1, i))
    events.sort()
    active_offsets = []  # sorted offsets of active allocations
    active_ids = []
    for _, is_start, i in events:
        offset, size = offsets[i], allocs[i][0]
        if is_start:
            pos = bisect_left(active_offsets, offset)
            if pos > 0:
                j = active_ids[pos - 1]
                if offsets[j] + allocs[j][0] > offset:
                    return False
            if pos < len(active_offsets) and offset + size > active_offsets[pos]:
                return False
            active_offsets.insert(pos, offset)
            active_ids.insert(pos, i)
        else:
            pos = bisect_left(active_offsets, offset)
            while active_ids[pos] != i:
                pos += 1
            del active_offsets[pos], active_ids[pos]
    return True


# ---------------------------------------------------------------------------
# Two-ended sweep: large sizes from the bottom, small from the top of a band
# ---------------------------------------------------------------------------


def sweep_two_ended(allocs, quantile=0.5):
    """Chronological sweep; small allocs are placed best-fit, large first-fit."""
    sizes = sorted(a[0] for a in allocs)
    threshold = sizes[int(len(sizes) * quantile)] if sizes else 0

    n = len(allocs)
    events = []
    for i, (size, start, end) in enumerate(allocs):
        events.append((start, 0, -size, i))
        events.append((end, -1, 0, i))
    events.sort()

    INF = float("inf")
    free_offsets = [0]
    free_sizes = [INF]
    offsets = [0] * n

    for _, kind, _, i in events:
        size = allocs[i][0]
        if kind == -1:
            offset = offsets[i]
            pos = bisect_left(free_offsets, offset)
            if pos < len(free_offsets) and free_offsets[pos] == offset + size:
                size += free_sizes[pos]
                del free_offsets[pos], free_sizes[pos]
            if pos > 0 and free_offsets[pos - 1] + free_sizes[pos - 1] == offset:
                free_sizes[pos - 1] += size
            else:
                free_offsets.insert(pos, offset)
                free_sizes.insert(pos, size)
        else:
            fits = [p for p in range(len(free_offsets)) if free_sizes[p] >= size]
            if allocs[i][0] >= threshold:
                pos = fits[0]
            else:
                pos = min(fits, key=lambda p: free_sizes[p])
            offsets[i] = free_offsets[pos]
            if free_sizes[pos] == size:
                del free_offsets[pos], free_sizes[pos]
            else:
                free_offsets[pos] += size
                free_sizes[pos] -= size
    return offsets


# ---------------------------------------------------------------------------
# Hybrid: exact first-fit on the K largest allocs, sweep the rest around them
# ---------------------------------------------------------------------------


def first_fit_exact(allocs, order, best_fit=False):
    """Quadratic reference first-fit (used only on small K subsets).

    With best_fit=True, picks the smallest fitting gap instead of the lowest
    (Pisarchyk & Lee 2020, Algorithm 3), falling back to the watermark top.
    """
    placed = []  # (offset, top, start, end)
    offsets = [0] * len(allocs)
    for i in order:
        size, start, end = allocs[i]
        blockers = sorted(
            (p for p in placed if p[2] < end and start < p[3]),
            key=lambda p: p[0],
        )
        cursor = 0
        best = None  # (gap_size, offset)
        for b_off, b_top, _, _ in blockers:
            gap = b_off - cursor
            if gap >= size:
                if not best_fit:
                    best = (gap, cursor)
                    break
                if best is None or gap < best[0]:
                    best = (gap, cursor)
            cursor = max(cursor, b_top)
        if best is None:
            best = (0, cursor)
        offsets[i] = best[1]
        placed.append((best[1], best[1] + size, start, end))
    return offsets


def hybrid_obstacles(
    allocs, k=None, obstacle_order="conflict_size", select="size", best_fit=False
):
    """Top-K (by size or area) placed exactly first; rest swept around them."""
    n = len(allocs)
    if k is None:
        k = min(n, max(16, int(4 * n**0.5)))
    select_key = {
        "size": lambda i: allocs[i][0],
        "area": lambda i: allocs[i][0] * (allocs[i][2] - allocs[i][1]),
    }[select]
    # Keep selected ids in input order so ordering tie-breaks match the
    # stable sorts of the exact greedy variants.
    big_ids = sorted(sorted(range(n), key=select_key, reverse=True)[:k])
    big = [allocs[i] for i in big_ids]
    big_offsets = first_fit_exact(big, all_orders(big)[obstacle_order], best_fit)
    return _sweep_around(allocs, big_ids, big, big_offsets)


def _sweep_around(allocs, big_ids_list, big, big_offsets):
    """Sweep-place all non-obstacle allocs around fixed obstacle placements."""
    n = len(allocs)
    big_ids = set(big_ids_list)
    obstacles = [
        (off, off + a[0], a[1], a[2]) for a, off in zip(big, big_offsets, strict=False)
    ]

    offsets = [0] * n
    for i, off in zip(big_ids_list, big_offsets, strict=False):
        offsets[i] = off

    small_ids = [i for i in range(n) if i not in big_ids]
    events = []
    for i in small_ids:
        size, start, end = allocs[i]
        events.append((start, 0, -size, i))
        events.append((end, -1, 0, i))
    events.sort()

    INF = float("inf")
    free_offsets = [0]
    free_sizes = [INF]

    for _, kind, _, i in events:
        size, start, end = allocs[i]
        if kind == -1:
            offset = offsets[i]
            pos = bisect_left(free_offsets, offset)
            if pos < len(free_offsets) and free_offsets[pos] == offset + size:
                size += free_sizes[pos]
                del free_offsets[pos], free_sizes[pos]
            if pos > 0 and free_offsets[pos - 1] + free_sizes[pos - 1] == offset:
                free_sizes[pos - 1] += size
            else:
                free_offsets.insert(pos, offset)
                free_sizes.insert(pos, size)
        else:
            # Forbidden offset bands: obstacles overlapping [start, end)
            forbidden = sorted(
                (o[0], o[1]) for o in obstacles if o[2] < end and start < o[3]
            )
            merged = []
            for lo, hi in forbidden:
                if merged and lo <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
                else:
                    merged.append((lo, hi))
            offsets[i] = _first_fit_with_forbidden(
                free_offsets, free_sizes, merged, size
            )
    return offsets


def _first_fit_with_forbidden(free_offsets, free_sizes, forbidden, size):
    """Lowest offset inside a free-list gap avoiding forbidden bands; splices."""
    f_idx = 0
    for pos in range(len(free_offsets)):
        gap_lo = free_offsets[pos]
        gap_hi = gap_lo + free_sizes[pos]
        candidate = gap_lo
        while f_idx > 0 and forbidden[f_idx - 1][1] > candidate:
            f_idx -= 1
        while True:
            while f_idx < len(forbidden) and forbidden[f_idx][1] <= candidate:
                f_idx += 1
            if f_idx < len(forbidden) and forbidden[f_idx][0] < candidate + size:
                candidate = forbidden[f_idx][1]
                if candidate + size > gap_hi:
                    break
                continue
            if candidate + size <= gap_hi:
                # Splice [candidate, candidate+size) out of this gap
                del free_offsets[pos], free_sizes[pos]
                if candidate > gap_lo:
                    free_offsets.insert(pos, gap_lo)
                    free_sizes.insert(pos, candidate - gap_lo)
                    pos += 1
                if gap_hi > candidate + size:
                    free_offsets.insert(pos, candidate + size)
                    free_sizes.insert(pos, gap_hi - (candidate + size))
                return candidate
            break
    raise AssertionError("Unbounded top gap must always fit")


def hybrid_size(allocs):
    return hybrid_obstacles(allocs, obstacle_order="size")


def hybrid_conflict_size(allocs):
    return hybrid_obstacles(allocs, obstacle_order="conflict_size")


def first_fit_exact_noisy(allocs, rng, sigma=0.25):
    """Exact first-fit with a log-normally perturbed size ordering."""
    order = sorted(
        range(len(allocs)),
        key=lambda i: allocs[i][0] * rng.lognormvariate(0, sigma),
        reverse=True,
    )
    return first_fit_exact(allocs, order)


def noisy_restarts(allocs, restarts=8, k=512, seed=0):
    """Best of hybrid runs with perturbed obstacle orderings."""
    rng = random.Random(seed)
    n = len(allocs)
    k = min(n, k)
    select_key = lambda i: allocs[i][0]  # noqa: E731
    by_size = sorted(range(n), key=select_key, reverse=True)
    big = [allocs[i] for i in by_size[:k]]

    best = None
    for _ in range(restarts):
        big_offsets = first_fit_exact_noisy(big, rng)
        offsets = _sweep_around(allocs, by_size[:k], big, big_offsets)
        if best is None or peak_of(allocs, offsets) < peak_of(allocs, best):
            best = offsets
    return best


def sweep_ff_compact(allocs):
    return compact(allocs, sweep_first_fit(allocs))


def sweep_bf_compact(allocs):
    return compact(allocs, sweep_best_fit(allocs))


# ---------------------------------------------------------------------------
# Boxing-lite: power-of-2 size classes, optimal channel assignment per class
# ---------------------------------------------------------------------------


def boxed_channels(allocs):
    """Round sizes to powers of 2; per class, greedy channel assignment
    (optimal interval-graph coloring); stack class bands; caller compacts.
    """
    n = len(allocs)
    groups = {}
    for i, (size, start, end) in enumerate(allocs):
        groups.setdefault((size - 1).bit_length(), []).append(i)

    offsets = [0] * n
    base = 0
    for c in sorted(groups, reverse=True):
        width = 1 << c
        expiry = []  # (end, channel) of live allocs
        free = []  # released channel numbers
        max_ch = 0
        for i in sorted(groups[c], key=lambda i: allocs[i][1]):
            start = allocs[i][1]
            while expiry and expiry[0][0] <= start:
                heapq.heappush(free, heapq.heappop(expiry)[1])
            if free:
                ch = heapq.heappop(free)
            else:
                ch = max_ch
                max_ch += 1
            offsets[i] = base + ch * width
            heapq.heappush(expiry, (allocs[i][2], ch))
        base += max_ch * width
    return offsets


def boxed(allocs):
    return boxed_channels(allocs)


def boxed_compact(allocs):
    return compact(allocs, boxed_channels(allocs))


def fast_by_all(allocs):
    candidates = (
        sweep_first_fit,
        sweep_best_fit,
        sweep_two_ended,
        hybrid_size,
        hybrid_conflict_size,
    )
    best = None
    for fn in candidates:
        offsets = compact(allocs, fn(allocs))
        if best is None or peak_of(allocs, offsets) < peak_of(allocs, best):
            best = offsets
    return best


def fast_by_all_v2(allocs, k=1024):
    members = [
        lambda a: sweep_first_fit(a),
        lambda a: sweep_best_fit(a),
        lambda a: sweep_two_ended(a),
        lambda a: hybrid_obstacles(a, k=k, obstacle_order="size"),
        lambda a: hybrid_obstacles(a, k=k, obstacle_order="conflict_size"),
        lambda a: hybrid_obstacles(a, k=k, obstacle_order="duration"),
        lambda a: hybrid_obstacles(a, k=k, obstacle_order="conflict"),
        lambda a: hybrid_obstacles(a, k=k, obstacle_order="area"),
        lambda a: hybrid_obstacles(a, k=k, obstacle_order="area", select="area"),
        lambda a: hybrid_obstacles(a, k=k, obstacle_order="size", best_fit=True),
        lambda a: hybrid_obstacles(
            a, k=k, obstacle_order="conflict_size", best_fit=True
        ),
    ]
    best = None
    for fn in members:
        offsets = fn(allocs)
        if best is None or peak_of(allocs, offsets) < peak_of(allocs, best):
            best = offsets
    return best


CANDIDATES = {
    "skyline_size": skyline_by("size"),
    "skyline_area": skyline_by("area"),
    "skyline_duration": skyline_by("duration"),
    "skyline_start": skyline_by("start"),
    "skyline_input": skyline_by("input"),
    "skyline_all": skyline_by_all,
    "skyline_all_compact": skyline_by_all_compact,
    "sweep_first_fit": sweep_first_fit,
    "sweep_best_fit": sweep_best_fit,
    "sweep_ff_compact": sweep_ff_compact,
    "sweep_bf_compact": sweep_bf_compact,
    "sweep_two_ended": sweep_two_ended,
    "hybrid_size": hybrid_size,
    "hybrid_conflict_size": hybrid_conflict_size,
    "fast_by_all": fast_by_all,
    "fast_by_all_v2": fast_by_all_v2,
    "noisy_restarts": noisy_restarts,
    "boxed": boxed,
    "boxed_compact": boxed_compact,
}
