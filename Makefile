.DEFAULT_GOAL := dev
dev:
	@if [ ! -f .env ]; then \
		echo "Copying .env.example to .env to bootstrap dev"; \
		cp .env.example .env; \
	fi && \
	npx supabase start && \
	make migrate && \
	uv venv && \
	source .venv/bin/activate && \
	uv pip install -e ".[dev]" && \
	playwright install --with-deps --no-shell && \
	uv run dagster dev -p 3001

dbt:
	@if [ ! -f .env ]; then \
		echo "Copying .env.example to .env to bootstrap dev"; \
		cp .env.example .env; \
	fi && \
	npx supabase start && \
	make migrate && \
	uv venv && \
	source .venv/bin/activate && \
	uv pip install -e ".[dev]" && \
	cd contributions_pipeline/transformation/dbt_transformations && \
	uv run dbt deps && \
	uv run dbt run --target dev --select $(select)

migrate:
	npx prisma migrate deploy

studio:
	npx prisma studio

# Usage: make migration (and then follow prompt)
migration:
	@read -p "Enter migration name: " migration_name; \
	timestamp=$$(date +'%Y%m%d%H%M%S'); \
	sanitized_name=$$(echo $$migration_name | sed 's/[^a-zA-Z0-9]/_/g'); \
	dir_name=prisma/migrations/$${timestamp}_$${sanitized_name}; \
	mkdir -p $${dir_name}; \
	echo '' >> $${dir_name}/migration.sql; \
	echo "Migration file created at $${dir_name}/migration.sql"

# Reset the local database to empty and rerun migrations (caution)
reset-db:
	npx prisma migrate reset --force --skip-generate

test:
	uv venv && \
  source .venv/bin/activate && \
  uv pip install -e ".[dev]" && \
  uv run ruff check --fix . && \
  uv run ruff format . && \
  uv run pyright && \
  uv run pytest -vvv

manual-dl:
	uv venv && \
	source .venv/bin/activate && \
	uv pip install -e ".[dev]" && \
	python manual_download.py ${state}
