#
# SPDX-License-Identifier: Apache-2.0
#
"""Render the profiling JSON as readable tables.

Refusals and skips print as such rather than as blanks, so a table never
implies a measurement that was never taken.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean, median

# Ratios within this of 1.0 are optimal up to the bound's own exactness
OPTIMAL_SLACK = 1.0005


def format_seconds(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f}us"
    if seconds < 1:
        return f"{seconds * 1e3:.1f}ms"
    return f"{seconds:.2f}s"


def cell(row: dict | None) -> str:
    """One table cell: a timing, a refusal, a skip, or nothing measured."""
    if row is None:
        return ""
    if row.get("seconds") is not None:
        return format_seconds(row["seconds"])
    return "refused" if "error" in row else "skipped"


def table_analysis(rows: list[dict]) -> None:
    print("\n### Analysis throughput\n")
    by_workload: dict[str, dict] = defaultdict(dict)
    calls: list[str] = []
    scales: set[int] = set()
    for row in rows:
        if row["bench"] != "analysis":
            continue
        by_workload[row["workload"]][row["call"], row["n"]] = row
        if row["call"] not in calls:
            calls.append(row["call"])
        scales.add(row["n"])

    order = sorted(scales)
    for workload, cells in by_workload.items():
        dims = sorted({r["dim"] for r in cells.values() if r.get("dim")})
        print(f"\n{workload}  (clock dim {dims or ['-']})")
        print(f"  {'call':<26}" + "".join(f"{n:>12,}" for n in order))
        for call in calls:
            line = f"  {call:<26}"
            for n in order:
                line += f"{cell(cells.get((call, n))):>12}"
            print(line)


def table_builds(rows: list[dict], threshold: float) -> None:
    slow = [r for r in rows if r["bench"] == "build" and r["seconds"] > threshold]
    if not slow:
        return
    print(f"\n### Workload generation over {threshold}s (not part of the API)\n")
    for row in slow:
        print(
            f"  {row['workload']:<28} n={row['n']:>9,}  "
            f"{format_seconds(row['seconds'])}"
        )


def table_allocators(rows: list[dict]) -> None:
    print("\n### Allocator throughput, with quality at the largest scale\n")
    by_workload: dict[str, dict[str, dict[int, dict]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for row in rows:
        if row["bench"] == "allocator" and row.get("seconds") is not None:
            by_workload[row["workload"]][row["allocator"]][row["n"]] = row

    for workload, allocators in by_workload.items():
        scales = sorted({n for cells in allocators.values() for n in cells})
        print(f"\n{workload}")
        header = f"  {'allocator':<22}"
        header += "".join(f"{n:>11,}" for n in scales)
        print(header + f"{'peak/bound':>13}")
        for name, cells in sorted(allocators.items()):
            line = f"  {name:<22}"
            for n in scales:
                line += f"{cell(cells.get(n)):>11}"
            ratio = cells[max(cells)].get("ratio")
            line += f"{(f'{ratio:.4f}' if ratio else '-'):>13}"
            print(line)


def table_quality(rows: list[dict]) -> None:
    entries = [r for r in rows if r["bench"] == "quality" and r.get("peaks")]
    if not entries:
        return
    print("\n### Placement quality: peak over the exact lower bound\n")
    ratios: dict[str, list[float]] = defaultdict(list)
    for entry in entries:
        for name, peak in entry["peaks"].items():
            if entry["bound"]:
                ratios[name].append(peak / entry["bound"])

    print(
        f"  {'allocator':<22}{'inst':>6}{'mean':>9}{'median':>9}"
        f"{'worst':>11}{'optimal':>12}"
    )
    for name, values in sorted(ratios.items(), key=lambda kv: mean(kv[1])):
        optimal = sum(1 for v in values if v < OPTIMAL_SLACK)
        print(
            f"  {name:<22}{len(values):>6}{mean(values):>9.3f}"
            f"{median(values):>9.3f}{max(values):>11.3f}"
            f"{f'{optimal} / {len(values)}':>12}"
        )
    print(f"\n  instances scored: {len(entries)}")


def table_known_optimum(rows: list[dict]) -> None:
    entries = [
        r
        for r in rows
        if r["bench"] == "quality" and r.get("known_optimum") and r.get("peaks")
    ]
    if not entries:
        return
    print("\n### Reverse-constructed instances: peak over the known optimum\n")
    names = sorted({name for entry in entries for name in entry["peaks"]})
    print(f"  {'workload':<26}" + "".join(f"{n[:11]:>12}" for n in names))
    for entry in entries:
        line = f"  {entry['workload']:<26}"
        for name in names:
            peak = entry["peaks"].get(name)
            line += f"{peak / entry['known_optimum']:>12.3f}" if peak else f"{'-':>12}"
        print(line)


def table_concurrency(rows: list[dict]) -> None:
    by_job: dict[tuple[str, str], dict[int, dict]] = defaultdict(dict)
    for row in rows:
        if row["bench"] == "concurrency" and "workers" in row:
            by_job[row["workload"], row["job"]][row["workers"]] = row
    if not by_job:
        return
    print("\n### Concurrency scaling\n")
    for (workload, job), cells in by_job.items():
        workers = sorted(cells)
        print(f"\n{job} on {workload} (n={cells[workers[0]]['n']:,})")
        print(f"  {'workers':<12}" + "".join(f"{w:>10}" for w in workers))
        for label, key, spec in (
            ("calls/s", "calls_per_s", "10.1f"),
            ("speedup", "speedup", "10.2f"),
            ("mismatches", "mismatches", "10d"),
        ):
            values = "".join(f"{cells[w][key]:>{spec}}" for w in workers)
            print(f"  {label:<12}{values}")


def table_memory(rows: list[dict]) -> None:
    entries = [r for r in rows if r["bench"] == "memory"]
    if not entries:
        return
    print("\n### Conflict graph footprint\n")
    for row in entries:
        print(
            f"  {row['workload']:<20} n={row['n']:>8,} "
            f"pairs={row['pairs']:>14,}  RSS +{row['rss_delta_mb']:>9.1f} MB  "
            f"{row['bytes_per_pair']:.1f} B/pair"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--slow-build", type=float, default=0.5)
    args = parser.parse_args()

    rows = json.loads(args.results.read_text())
    table_analysis(rows)
    table_builds(rows, args.slow_build)
    table_allocators(rows)
    table_quality(rows)
    table_known_optimum(rows)
    table_concurrency(rows)
    table_memory(rows)


if __name__ == "__main__":
    main()
