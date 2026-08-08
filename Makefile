.PHONY: check lint format format-check type-check test test-cov test-cov-html test-50-times docs build

UV = uv
PYTEST = uv run pytest
RUFF_PATHS = .

check: lint format-check type-check test-cov docs build

lint:
	$(UV) run ruff check $(RUFF_PATHS)

format:
	$(UV) run ruff format $(RUFF_PATHS)

format-check:
	$(UV) run ruff format --check $(RUFF_PATHS)

type-check:
	$(UV) run ty check src/ tests/

test:
	$(PYTEST) -n auto --random-order --durations=10

test-cov:
	$(PYTEST) -n auto --random-order --durations=10 --cov=chuk_vfs_postgres --cov=chuk_fsspec \
		--cov-report=term-missing --cov-fail-under=99

test-cov-html: test-cov
	$(UV) run coverage html
	@echo "→ htmlcov/index.html"

# Flakiness stress run: every test 50x in parallel.
# pytest-repeat duplicates each test at collection, so xdist spreads the
# 50 iterations across all workers — exactly 50 runs per test total.
test-50-times:
	$(PYTEST) -n auto --count=50 --random-order --durations=10

docs:
	$(UV) run --extra docs sphinx-build -W --keep-going \
		-b html docs docs/_build/html

build:
	$(UV) build
