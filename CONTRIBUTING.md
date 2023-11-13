# Contributing to spgci-python

Thanks for contributing! Read below for instructions on getting started

## Setup

1. Make sure you have Poetry installed ([see instructions here](https://python-poetry.org))
2. Clone the spgci-python respository
3. Run `poetry shell` to create a virtual environment
4. Run `poetry install` to install all dependencies
5. Run `python` and then `import spgci as ci` and `ci.version` to ensure it's installed correctly.

## Executing Tests

1. Set env variables `SPGCI_USERNAME`, `SPGCI_PASSWORD`, `SPGCI_APPKEY`
1. Run `poetry run pytest .\tests\{test file}`
