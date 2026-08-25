.PHONY: check lint fmt test train replay-check demo smoke docker-build

VENV := .venv/bin
DATA ?= data/raw/creditcard.csv
ARTIFACTS ?= artifacts

check: lint test replay-check
	@echo "All local checks passed"

lint:
	$(VENV)/ruff check src/fraud_workbench src/dashboard src/pipeline/train.py tests

fmt:
	$(VENV)/ruff format src/fraud_workbench src/dashboard src/pipeline/train.py tests

test:
	$(VENV)/pytest tests -q

train:
	$(VENV)/python -m src.pipeline.train --data $(DATA) --artifacts $(ARTIFACTS)

replay-check:
	@if [ -f "$(DATA)" ]; then $(VENV)/python -m src.pipeline.train --data $(DATA) --artifacts $(ARTIFACTS); else echo "Private full dataset absent; replay check skipped"; fi

demo:
	FRAUD_ARTIFACT_ROOT=$(ARTIFACTS) $(VENV)/python -m src.dashboard.app

smoke:
	$(VENV)/python -m scripts.smoke --artifacts $(ARTIFACTS)

docker-build:
	docker build -f docker/Dockerfile -t fraud-decision-workbench:local .
