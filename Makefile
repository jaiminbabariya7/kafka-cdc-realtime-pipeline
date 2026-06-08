.PHONY: install install-dev lint format test clean
install:
	pip install -r requirements.txt
install-dev: install
	pip install pytest pytest-cov black flake8
lint:
	flake8 code/ --max-line-length=100 --ignore=E501,W503
	black --check --line-length 100 code/
format:
	black --line-length 100 code/
test:
	pytest tests/ -v --cov=code/ --cov-report=term-missing
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/ .pytest_cache/
