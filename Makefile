.PHONY: bootstrap install lint test dev seed status

bootstrap:
	python3 -m venv .venv
	.venv/bin/python -m pip install -U pip
	.venv/bin/python -m pip install -e .[dev]

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
