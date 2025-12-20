# OmniMalloc

Your one-stop shop for static memory allocation.

## Installation

```bash
pip install omnimalloc
```

## Usage

Please refer to [examples](examples/), in particular [examples/01_basic.py](examples/01_basic.py).

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
