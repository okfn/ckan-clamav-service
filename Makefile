.PHONY: help devserver format install lint test

help:
	@grep '^\.PHONY' Makefile | cut -d' ' -f2- | tr ' ' '\n'

devserver:
	python clamav_service/main.py settings_local.py

format:
	isort --profile black .
	black .

install:
	pip install -r requirements.txt
	pip install -r dev-requirements.txt
	pip install -e .

lint:
	isort --profile black -c --diff .
	black --check .
	flake8 .

test:
	pytest --cov clamav_service --cov-report=xml
