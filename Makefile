.PHONY: help clean clean-pyc clean-build list test coverage release

help:
	@echo "  clean-build - remove build artifacts"
	@echo "  clean-pyc - remove Python file artifacts"
	@echo "  clean-data - remove generated data files"
	@echo "  clean - clean everything"
	@echo "  lint - check code style"
	@echo "  test - run tests quickly with the default Python"
	@echo "  test-mark - run tests with specific marks, e.g. MARK=\"envtest and intgtest\""
	@echo "  test-mock - run tests on mock objects"
	@echo "  test-unit - run unit tests"
	@echo "  test-env - run environment tests"
	@echo "  test-intg - run integration tests"
	@echo "  run - run ETL tasks"
	@echo "  coverage - check code coverage quickly"
	@echo "  coverage-report - open the coverage report in your browser"
	@echo "  install-requirements - install the requirements for development"
	@echo "  anonymize - anonymize data"
	@echo "  docker-build - build docker image taipei-bi-etl-img"
	@echo "  docker-run - run docker image taipei-bi-etl-img"

clean: clean-build clean-pyc clean-data

clean-data:
	rm -fr data/
	rm -fr debug-data/
	mkdir data
	mkdir debug-data

clean-build:
	rm -fr .mypy_cache/
	rm -fr .pytest_cache/
	rm -fr docs/build/
	rm -fr htmlcov/
	rm -fr *.egg-info
	rm .coverage

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

lint:
	pytest -v --black --docstyle --flake8 --mypy-ignore-missing-imports -n 4 -m "not envtest and not unittest and not intgtest"

test:
	py.test

test-mark:
	pytest -m $(MARK)

test-mock:
	pytest -m "mocktest"

test-unit:
	pytest -m "unittest"

test-env:
	pytest -m "envtest"

test-intg:
	pytest -m "intgtest"

coverage:
	pytest tests/ --cov=.
	coverage report -m

coverage-report: coverage
	coverage html
	open htmlcov/index.html

install-requirements:
	pip install -r requirements/requirements.txt
	pip install -r requirements/test_requirements.txt

run:
	./etl.py $(COMMAND)

anonymize:
	python utils/anonymizer.py

docker-build:
	docker build -t taipei-bi-etl-img .

docker-run:
	docker run $(COMMAND) taipei-bi-etl-img


