# ============================================================
# Spotify Data Pipeline — Developer Commands
# ============================================================
# Usage: make <target>
# ============================================================

.PHONY: help install lint format test up down producer consumer clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Setup ────────────────────────────────────────────────────

install: ## Install dependencies
	pip install -e ".[dev]"

# ── Code Quality ─────────────────────────────────────────────

lint: ## Run linter (ruff)
	ruff check src/ tests/ dags/

format: ## Auto-format code (ruff)
	ruff format src/ tests/ dags/

typecheck: ## Run type checker (mypy)
	mypy src/

# ── Testing ──────────────────────────────────────────────────

test: ## Run test suite
	pytest tests/ -v --tb=short

test-cov: ## Run tests with coverage report, measure how much of the code is executed during the tests
	pytest tests/ -v --cov=src --cov-report=term-missing

# ── Infrastructure ───────────────────────────────────────────

up: ## Start all Docker services
	docker compose up -d

down: ## Stop all Docker services
	docker compose down

logs: ## Tail Docker service logs
	docker compose logs -f

# ── Pipeline Components ──────────────────────────────────────

producer: ## Run the Kafka event producer
	python -m src.producer.main

consumer: ## Run the S3 consumer
	python -m src.consumer.main

# ── dbt ──────────────────────────────────────────────────────

dbt-run: ## Run dbt models
	cd spotify_dbt && dbt run

dbt-test: ## Run dbt schema tests
	cd spotify_dbt && dbt test

dbt-docs: ## Generate dbt documentation
	cd spotify_dbt && dbt docs generate && dbt docs serve

# ── Cleanup ──────────────────────────────────────────────────

clean: ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/ *.egg-info/
