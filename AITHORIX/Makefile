.PHONY: help install dev test deploy clean

help:
	@echo "AITHORIX Trading System Commands"
	@echo "make install  - Install all dependencies"
	@echo "make dev      - Run development server"
	@echo "make test     - Run all tests"
	@echo "make deploy   - Deploy to production"
	@echo "make clean    - Clean cache files"

install:
	pip install --upgrade pip
	pip install -r requirements.txt
	pre-commit install

dev:
	python -m uvicorn core.main:app --reload --host 0.0.0.0 --port 8000

test:
	pytest tests/ -v --cov=./ --cov-report=html

deploy:
	./scripts/deploy.sh

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov
