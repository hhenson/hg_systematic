# HG Systematic

This is a library of utilities and examples to highlight how HGraph can be used to implement
systematic trading strategies.


See [this](https://hgraph.readthedocs.io/en/latest/) for more information.

## Development

This project uses uv for environment and dependency management.
See https://docs.astral.sh/uv/ for installation instructions.

Here are some useful commands:

Create a local virtual environment in the project directory (./.venv) with Python 3.12:

```bash
uv venv --python 3.12
```

Install the project and its dependencies:

```bash
# Base dependencies
uv sync

# Include documentation dependencies
uv sync --extra docs

# Install all optional extras (e.g. tests, adaptors)
uv sync --all-extras --all-groups
```

PyCharm can use the interpreter from ./.venv (created by uv) to set up the project.
Recent versions of PyCharm support uv, use the "uv" interpreter type.

### Run Tests

```bash
# No Coverage
uv run pytest
```

```bash
# Generate Coverage Report
uv run pytest --cov=hg_systematic --cov-report=xml
```
