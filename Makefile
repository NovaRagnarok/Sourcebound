.PHONY: bootstrap bootstrap-graphrag install lint typecheck test check format dev seed status newcomer-smoke default-stack-integration

bootstrap:
	python3 -m venv .venv
	.venv/bin/python -m pip install -U pip
	.venv/bin/python -m pip install -e .[dev]

bootstrap-graphrag: bootstrap
	.venv/bin/python -m pip install -e .[graphrag]

install: bootstrap

lint:
	.venv/bin/ruff check src tests

typecheck:
	.venv/bin/python -m mypy src

format:
	.venv/bin/ruff format src tests

test:
	.venv/bin/pytest

check: lint typecheck test

seed:
	.venv/bin/saw seed-dev-data

status:
	.venv/bin/saw status

dev:
	.venv/bin/saw serve --reload

newcomer-smoke: bootstrap
	./scripts/newcomer_smoke.sh

default-stack-integration: bootstrap
	.venv/bin/pytest -m live_default_stack tests/test_postgres_integration.py
