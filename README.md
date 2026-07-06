<h1 align="center">OmniMalloc</h1>

<p align="center">State-of-the-art static memory allocation for neural networks.</p>

<p align="center">
  <a href="https://github.com/fpedd/omnimalloc/actions/workflows/checks.yml"><img src="https://img.shields.io/github/actions/workflow/status/fpedd/omnimalloc/checks.yml?branch=main&label=checks" alt="Checks"></a>
  <a href="https://github.com/fpedd/omnimalloc/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/fpedd/omnimalloc/build.yml?branch=main&label=build" alt="Build"></a>
  <a href="https://pypi.org/project/omnimalloc/"><img src="https://img.shields.io/pypi/v/omnimalloc" alt="PyPI"></a>
  <a href="https://pypi.org/project/omnimalloc/"><img src="https://img.shields.io/pypi/pyversions/omnimalloc" alt="Python versions"></a>
  <a href="https://github.com/fpedd/omnimalloc/blob/main/LICENSE"><img src="https://img.shields.io/pypi/l/omnimalloc" alt="License"></a>
</p>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/fpedd/omnimalloc/main/assets/hero_dark.svg">
  <img src="https://raw.githubusercontent.com/fpedd/omnimalloc/main/assets/hero_light.svg" alt="Solution quality vs. solve time across allocators">
</picture>

OmniMalloc is a Python library for **static memory allocation**: given buffers
with known sizes and lifetimes, assign offsets so that **peak memory is minimized**.
This is the memory-planning step at the heart of **ML compilers**, embedded
runtimes, and accelerator toolchains.

It ships a collection of allocators and allocation algorithms behind one API,
implemented with an efficient C++ backend. This includes **SuperMalloc**, a new
allocator that **outperforms the best open-source alternatives** (see benchmarks
below). OmniMalloc also provides a rich benchmark harness and visualization
tools to develop and evaluate new allocation strategies.

## Installation

```bash
pip install omnimalloc
```

## Usage

```python
from omnimalloc import Allocation, Pool, run_allocation

pool = Pool(id="pool", allocations=(
    Allocation(id=0, size=64, start=0, end=10),
    Allocation(id=1, size=64, start=12, end=20),
    Allocation(id=2, size=32, start=5, end=15),
))

pool = run_allocation(pool, allocator="supermalloc_allocator", validate=True)

print(pool.size)                                     # 96
print([alloc.offset for alloc in pool.allocations])  # [0, 0, 64]
```

On a real problem, the result looks like this: 308 buffers of an ML workload
packed with zero wasted memory.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/fpedd/omnimalloc/main/assets/allocation_dark.svg">
  <img src="https://raw.githubusercontent.com/fpedd/omnimalloc/main/assets/allocation_light.svg" alt="A solved allocation problem rendered as offset/time rectangles">
</picture>

See [examples](examples/) for allocator selection, visualization, custom
allocation sources, and benchmarking.

## Benchmarks

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/fpedd/omnimalloc/main/assets/quality_dark.svg">
    <img src="https://raw.githubusercontent.com/fpedd/omnimalloc/main/assets/quality_light.svg" alt="Packing efficiency per problem" width="49%">
  </picture>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/fpedd/omnimalloc/main/assets/scaling_dark.svg">
    <img src="https://raw.githubusercontent.com/fpedd/omnimalloc/main/assets/scaling_light.svg" alt="Solve time vs. problem size" width="49%">
  </picture>
</p>

Every figure on this page is generated from a deterministic benchmark run by
[`scripts/generate_readme_assets.py`](scripts/generate_readme_assets.py).
Run your own campaigns with the [benchmark harness](examples/05_benchmark.py).

## Development

```bash
# Initial setup
git clone git@github.com:fpedd/omnimalloc.git
cd omnimalloc
uv sync --all-extras --group dev

# Run tests, linting, type checking
uv run pytest
uv run ruff check --fix && uv run ruff format && uv run ty check

# Setup pre-commit hooks (run once)
uv run pre-commit install

# Run pre-commit checks manually
uv run pre-commit run --all-files
```

## License

Copyright 2025 Fabian Peddinghaus. Licensed under Apache 2.0 License. See [LICENSE](LICENSE) for details.
