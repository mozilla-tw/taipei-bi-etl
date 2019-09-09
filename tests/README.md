How to Run Tests
===

This repository uses `pytest`:

```
# install requirements
pip install -r test_requirements.txt

# run pytest with all linters and 4 workers in parallel
pytest --black --docstyle --flake8 --mypy-ignore-missing-imports -n 4
```

When developing tests, you may want to run them separately with -v (verbose) option

```
# run code format tests only
pytest -v --black --docstyle --flake8 --mypy-ignore-missing-imports -n 4 -m "not envtest and not unittest and not intgtest"

# run environment tests only
pytest -v -m "envtest"

# run unit tests only
pytest -v -m "unittest"

# run integrated tests only
pytest -v -m "intgtest"

# run mock tests only (won't run by default)
pytest tests/conftest.py -v -m "mocktest"
```

## Technical References
- [pytest](https://docs.pytest.org/en/latest/contents.html)
    - [assertion](https://docs.pytest.org/en/latest/assert.html)
    - [fixture](https://docs.pytest.org/en/latest/fixture.html)
    - [parameterize](https://docs.pytest.org/en/latest/parametrize.html#parametrize)
    - [Monkeypatching/mocking](https://docs.pytest.org/en/latest/monkeypatch.html)
