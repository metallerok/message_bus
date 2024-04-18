PYTHON_VENV=.venv/bin/python
PYTHON=$(shell if test -f ${PYTHON_VENV}; then echo ${PYTHON_VENV}; else echo ${SYSTEM_PYTHON}; fi)
CONTAINER_NAME = car-plate-recognizer

install:
	$(PYTHON) -m pip install -e .
