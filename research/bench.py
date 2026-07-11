#
# SPDX-License-Identifier: Apache-2.0
#
"""Quality benchmark: candidates vs GreedyByAll (best-of-7 C++ first-fit)."""

import csv
import math
import pickle
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from candidates import (  # ty: ignore[unresolved-import]
    CANDIDATES,
    lower_bound,
    peak_of,
    validate,
)
from omnimalloc.allocators.greedy_cpp import (
    GreedyAllocatorCpp,
    GreedyByAreaAllocatorCpp,
    GreedyByConflictAllocatorCpp,
    GreedyByConflictSizeAllocatorCpp,
    GreedyByDurationAllocatorCpp,
    GreedyBySizeAllocatorCpp,
    GreedyByStartAllocatorCpp,
)
from omnimalloc.benchmark.sources.generator import (
    HighContentionSource,
    PowerOf2Source,
    RandomSource,
    SequentialSource,
)
from omnimalloc.primitives import Allocation

EXTERNAL = Path(__file__).parent.parent / "external" / "minimalloc"

GREEDY_VARIANTS = (
    GreedyAllocatorCpp,
    GreedyBySizeAllocatorCpp,
    GreedyByDurationAllocatorCpp,
    GreedyByAreaAllocatorCpp,
    GreedyByConflictAllocatorCpp,
    GreedyByConflictSizeAllocatorCpp,
    GreedyByStartAllocatorCpp,
)


def load_problems():
    problems = {}
    for csv_path in sorted(EXTERNAL.glob("*/*.csv")):
        with open(csv_path) as f:
            rows = list(csv.DictReader(f))
        allocs = [(int(r["size"]), int(r["lower"]), int(r["upper"])) for r in rows]
        problems[f"mm_{csv_path.parent.name[0]}_{csv_path.stem.split('.')[0]}"] = allocs

    sources = {
        "rand_100": RandomSource(num_allocations=100),
        "rand_1000": RandomSource(num_allocations=1000),
        "rand_5000": RandomSource(num_allocations=5000, time_max=100000),
        "contention_1000": HighContentionSource(num_allocations=1000),
        "pow2_1000": PowerOf2Source(num_allocations=1000, time_max=1000),
        "seq_1000": SequentialSource(num_allocations=1000),
    }
    for name, source in sources.items():
        problems[name] = [(a.size, a.start, a.end) for a in source.get_allocations()]
    problems.update(load_onnx_problems())
    return problems


ONNX_MODELS = (
    "resnet18_Opset18_timm",
    "alexnet_Opset17",
    "adv_inception_v3_Opset18",
    "beit_large_patch16_224_Opset17",
    "cait_m48_448_Opset18",
    "convnext_large_384_in22ft1k_Opset18",
    "convmixer_1536_20_Opset16",
    "coat_mini_Opset18",
)


def load_onnx_problems():
    cache = Path(__file__).parent / ".onnx_cache.pkl"
    if cache.exists():
        return pickle.loads(cache.read_bytes())

    from omnimalloc.benchmark.converters.model import model_to_allocations
    from omnimalloc.benchmark.converters.onnx import from_onnx

    hub = Path.home() / ".cache" / "huggingface" / "hub"
    problems = {}
    for model_name in ONNX_MODELS:
        paths = list(hub.glob(f"models--onnxmodelzoo--{model_name}/snapshots/*/*.onnx"))
        if not paths:
            continue
        model = from_onnx(paths[0])
        allocs = model_to_allocations(model)
        problems[f"onnx_{model_name.split('_Opset')[0]}"] = [
            (a.size, a.start, a.end) for a in allocs
        ]
    cache.write_bytes(pickle.dumps(problems))
    return problems


def greedy_by_all(allocs):
    entities = tuple(
        Allocation(id=i, size=s, start=b, end=e) for i, (s, b, e) in enumerate(allocs)
    )
    best = None
    for variant in GREEDY_VARIANTS:
        placed = variant().allocate(entities)
        peak = max(a.offset + a.size for a in placed if a.offset is not None)
        if best is None or peak < best:
            best = peak
    return best


def main():
    names = sys.argv[1:] or list(CANDIDATES)
    problems = load_problems()
    ratios_lb = {n: [] for n in names}
    ratios_gba = {n: [] for n in names}
    times = dict.fromkeys(names, 0.0)

    header = f"{'problem':<18}{'N':>6}{'LB':>12}{'gba/LB':>8}{'gba_t':>8}"
    for n in names:
        header += f"{n:>{max(len(n) + 2, 9)}}"
    print(header)

    gba_time_total = 0.0
    for pname, allocs in problems.items():
        lb = lower_bound(allocs)
        t0 = time.perf_counter()
        gba = greedy_by_all(allocs)
        gba_t = time.perf_counter() - t0
        gba_time_total += gba_t
        row = f"{pname:<18}{len(allocs):>6}{lb:>12}{gba / lb:>8.4f}{gba_t:>8.2f}"
        for n in names:
            t0 = time.perf_counter()
            offsets = CANDIDATES[n](allocs)
            times[n] += time.perf_counter() - t0
            assert validate(allocs, offsets), f"{n} invalid on {pname}"
            peak = peak_of(allocs, offsets)
            ratios_lb[n].append(peak / lb)
            ratios_gba[n].append(peak / gba)
            row += f"{peak / gba:>{max(len(n) + 2, 9)}.4f}"
        print(row)

    print("\n--- geomean peak/greedy_by_all (lower is better, <1 beats baseline) ---")
    for n in names:
        gm_gba = math.exp(sum(map(math.log, ratios_gba[n])) / len(ratios_gba[n]))
        gm_lb = math.exp(sum(map(math.log, ratios_lb[n])) / len(ratios_lb[n]))
        wins = sum(r < 0.9999 for r in ratios_gba[n])
        ties = sum(abs(r - 1) < 0.0001 for r in ratios_gba[n])
        print(
            f"{n:<22} vs_gba={gm_gba:.4f}  vs_lb={gm_lb:.4f}  "
            f"wins={wins} ties={ties} losses={len(ratios_gba[n]) - wins - ties}  "
            f"total_t={times[n]:.2f}s"
        )
    print(f"greedy_by_all total_t={gba_time_total:.2f}s")


if __name__ == "__main__":
    main()
