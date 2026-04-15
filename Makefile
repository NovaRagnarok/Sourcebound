.PHONY: bootstrap bootstrap-graphrag install lint test dev seed status newcomer-smoke

bootstrap:
	python3 -m venv .venv
	.venv/bin/python -m pip install -U pip
	.venv/bin/python -m pip install -e .[dev]

bootstrap-graphrag: bootstrap
	.venv/bin/python -m pip install -e .[graphrag]

install: bootstrap

lint:
	.venv/bin/ruff check src tests

format:
	.venv/bin/ruff format src tests

test:
	.venv/bin/pytest

seed:
	.venv/bin/saw seed-dev-data

status:
	.venv/bin/saw status

dev:
	.venv/bin/saw serve --reload

newcomer-smoke: bootstrap
	./scripts/newcomer_smoke.sh
