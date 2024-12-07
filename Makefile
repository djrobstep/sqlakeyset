# test commands and arguments
tcommand = PYTHONPATH=. uv run ${UV_RUN_ARGS} -m pytest -x
tmessy = -svv
targs = --cov-report term-missing --cov sqlakeyset --junitxml=test-reports/pytest-`date +%s`.xml ${PYTEST_ARGS}

pip:
	pip install --upgrade pip
	pip install --upgrade -r requirements.txt

pipeditable:
	pip install -e .

tox:
	tox tests

test:
	$(tcommand) $(targs) tests

stest:
	$(tcommand) $(tmessy) $(targs) tests

clean:
	find . -name \*.pyc -delete
	rm -rf build
	rm -rf dist

lint:
	uv run flake8 sqlakeyset
	uv run flake8 tests

tidy: clean fmt lint


all: clean lint tox

publish:
	python setup.py sdist bdist_wheel --universal
	twine upload dist/*
