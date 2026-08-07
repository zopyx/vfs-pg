.PHONY: check lint format format-check test test-cov test-cov-html docs build

UV = uv
PYTEST = uv run pytest
RUFF_PATHS = src/ tests/ examples/

check: lint format-check test-cov docs build

lint:
	$(UV) run ruff check $(RUFF_PATHS)

format:
	$(UV) run ruff format $(RUFF_PATHS)

format-check:
	$(UV) run ruff format --check $(RUFF_PATHS)

test:
	$(PYTEST)

test-cov:
	$(PYTEST) --cov=chuk_vfs_postgres --cov=chuk_fsspec \
		--cov-report=term-missing --cov-fail-under=99

test-cov-html: test-cov
	$(UV) run coverage html
	@echo "→ htmlcov/index.html"

docs:
	$(UV) run --extra docs sphinx-build -W --keep-going \
		-b html docs docs/_build/html

build:
	$(UV) build
