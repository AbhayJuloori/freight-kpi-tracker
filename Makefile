VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
GENERATE_ARGS ?=

.PHONY: install download generate generate-priors generate-fixture load validate test lint evaluate dashboard clean

install:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

download:
	$(PYTHON) scripts/download_data.py

generate:
	$(PYTHON) scripts/generate_synthetic.py $(GENERATE_ARGS)

generate-priors:
	$(PYTHON) scripts/generate_synthetic.py --use-priors $(GENERATE_ARGS)

generate-fixture:
	$(PYTHON) scripts/generate_synthetic.py --use-priors --n 500

load:
	$(PYTHON) scripts/load_snowflake.py

validate:
	$(PYTHON) scripts/validate_load.py

test:
	$(VENV)/bin/pytest tests/ -v

lint:
	$(VENV)/bin/ruff check scripts/ sql/

evaluate:
	$(PYTHON) scripts/evaluate_anomaly.py --local

dashboard:
	$(PYTHON) scripts/dashboard.py

clean:
	rm -rf data/raw/ data/processed/ $(VENV)
