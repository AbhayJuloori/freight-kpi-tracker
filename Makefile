VENV = .venv-v2
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
FREIGHT = $(VENV)/bin/freight-v2
BOOTSTRAP_PYTHON ?= python3.11
ARTIFACT_ROOT ?= artifacts/runs
PORTFOLIO_DATA ?=

.PHONY: install test lint format-check fixture run export-portfolio clean

install:
	$(BOOTSTRAP_PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e '.[dev]'

test:
	$(PYTHON) -m pytest tests/ -q

lint:
	$(VENV)/bin/ruff check src tests scripts/load_snowflake.py scripts/validate_load.py

format-check:
	$(VENV)/bin/ruff format --check src tests scripts/load_snowflake.py scripts/validate_load.py

fixture:
	$(FREIGHT) build --seed-source TEST --rows 5000 --output /tmp/freight-v2-fixture

run:
	$(FREIGHT) build --seed-source TEST --rows 5000 --output $(ARTIFACT_ROOT)

export-portfolio:
	@test -n "$(PORTFOLIO_DATA)" || (echo "Set PORTFOLIO_DATA to the portfolio's public/data/freight/v2 directory" >&2; exit 2)
	$(FREIGHT) export-portfolio --artifact-root $(ARTIFACT_ROOT) --run accepted --output "$(PORTFOLIO_DATA)"

clean:
	rm -rf -- .venv-v2 .pytest_cache .ruff_cache
