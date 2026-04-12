.PHONY: install lint test dev seed

install:
	python -m pip install -U pip
	python -m pip install -e .[dev]

lint:
	ruff check src tests

format:
	ruff format src tests

test:
	pytest

seed:
	saw seed-dev-data

dev:
	saw serve --reload
