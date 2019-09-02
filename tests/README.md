How to Run Tests
===

This repository uses `pytest`:

```
# install requirements
pip install -r test_requirements.txt

# run pytest with all linters and 4 workers in parallel
pytest --black --docstyle --flake8 --mypy-ignore-missing-imports -n 4
```