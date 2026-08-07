.PHONY: test test-cov test-cov-html

PYTEST = uv run pytest

test:
	$(PYTEST)

test-cov:
	$(PYTEST) --cov=chuk_vfs_postgres --cov=chuk_fsspec --cov-report=term-missing

test-cov-html: test-cov
	coverage html
	@echo "→ htmlcov/index.html"
