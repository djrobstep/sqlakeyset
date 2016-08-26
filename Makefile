
# test commands and arguments
tcommand = PYTHONPATH=. py.test -x
tmessy = -svv
targs = --cov-report term-missing --cov sqlakeyset

pip:
	pip install -r requirements-dev.txt

pipupgrade:
	pip install --upgrade pip
	pip install --upgrade -r requirements-dev.txt

pipreqs:
	pip install -r requirements.txt

pipeditable:
	pip install -e .

tox:
	tox tests

test:
	$(tcommand) $(targs) tests

stest:
	$(tcommand) $(tmessy) $(targs) tests

clean:
	git clean -fXd
	find . -name \*.pyc -delete


lint:
	flake8 sqlakeyset
	flake8 tests

tidy: clean fmt lint


all: pipupgrade clean lint tox

publish:
	python setup.py register
	python setup.py sdist bdist_wheel --universal upload
