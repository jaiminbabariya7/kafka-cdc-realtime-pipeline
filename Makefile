.PHONY: install install-dev lint format test clean
install:
	pip install -r requirements.txt
install-dev: install
	pip install pytest pytest-cov black flake8
lint:
	flake8 consumers/ tests/ --max-line-length=100 --ignore=E501,W503
	black --check --line-length 100 consumers/ tests/
format:
	black --line-length 100 consumers/ tests/
test:
	pytest tests/ -v --cov=consumers --cov-report=term-missing
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/ .pytest_cache/
