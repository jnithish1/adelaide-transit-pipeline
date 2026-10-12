# Shortcuts. Run `make help`.
.PHONY: help install test lint extract-local load dbt-dev dbt-docs metrics airflow

DBT := cd dbt && dbt --no-use-colors
PROFILES := --profiles-dir .

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

install: ## Create .venv with everything
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt -r requirements-dbt.txt

test: ## Unit tests (offline)
	pytest -q

lint: ## Ruff
	ruff check .

extract-local: ## Run all extractors writing to ./data_lake (no AWS)
	S3_BUCKET= python -m ingestion.extract_gtfs_static
	S3_BUCKET= python -m ingestion.extract_gtfs_realtime
	S3_BUCKET= python -m ingestion.extract_weather

extract: ## Run all extractors writing to S3
	python -m ingestion.extract_gtfs_static
	python -m ingestion.extract_gtfs_realtime
	python -m ingestion.extract_weather

load: ## COPY new S3 files into Snowflake RAW
	python -m ingestion.load_snowflake

dbt-dev: ## Full dbt build into your dev schema
	$(DBT) deps $(PROFILES) && $(DBT) seed $(PROFILES) && $(DBT) snapshot $(PROFILES) && $(DBT) build $(PROFILES) --exclude resource_type:snapshot resource_type:seed

dbt-docs: ## Generate and serve dbt docs (lineage graph at localhost:8081)
	$(DBT) docs generate $(PROFILES) && $(DBT) docs serve $(PROFILES) --port 8081

airflow: ## Start local Airflow at localhost:8080
	cd airflow && docker compose up --build
