.PHONY: up down build test lint clean logs

# ─── Docker ──────────────────────────────────────────────────────────────────

up: ## Start all services
	MY_UID=$$(id -u) docker compose up -d --build

down: ## Stop all services
	docker compose down

down-v: ## Stop all services and remove volumes
	docker compose down -v

build: ## Rebuild Docker images
	docker compose build

logs: ## Follow logs from all services
	docker compose logs -f

# ─── Development ─────────────────────────────────────────────────────────────

test: ## Run tests locally (Spark tests skipped without Java)
	python -m pytest --tb=short -q

test-docker: ## Run tests inside Docker (includes Spark/Java)
	docker compose run --rm airflow-webserver pytest --tb=short -q

lint: ## Run flake8 linter
	flake8 src/ tests/ dags/ --max-line-length=120 --ignore=E501,W503

clean: ## Remove Python cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

# ─── Help ────────────────────────────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
