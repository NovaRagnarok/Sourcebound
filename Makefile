.PHONY: bootstrap install lint test dev seed

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

dev:
	.venv/bin/saw serve --reload
