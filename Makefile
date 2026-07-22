.PHONY: install build test run

install:
	cd backend && uv sync --extra test

build:
	@echo "Native frontend: no build step required"

test:
	cd backend && uv run pytest -q

run:
	cd backend && uv run alembic upgrade head
	cd backend && uv run python -m app
